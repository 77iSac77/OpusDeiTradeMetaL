"""
OpusDeiTradeMetaL - Coletor de Preços
======================================
Coleta preços de metais de múltiplas fontes.
"""

import aiohttp
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass, field
from bs4 import BeautifulSoup
import re

from config.settings import METAIS, DATA_SOURCES, ALERT_THRESHOLDS, AlertLevel
from storage.database import get_database

logger = logging.getLogger(__name__)


@dataclass
class PriceData:
    """Dados de preço de um metal."""
    metal: str
    price: float
    change_percent: float = 0.0
    change_value: float = 0.0
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None
    volume: Optional[float] = None
    source: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict:
        return {
            "metal": self.metal,
            "price": self.price,
            "change_percent": self.change_percent,
            "change_value": self.change_value,
            "high_24h": self.high_24h,
            "low_24h": self.low_24h,
            "volume": self.volume,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
        }


class PriceCollector:
    """Coletor de preços de múltiplas fontes."""
    
    def __init__(self):
        self.db = get_database()
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_prices: Dict[str, PriceData] = {}
        self.price_history: Dict[str, List[Tuple[datetime, float]]] = {}
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Obtém ou cria sessão HTTP."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session
    
    async def close(self):
        """Fecha sessão HTTP."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def _fetch_metals_live(self) -> Dict[str, PriceData]:
        """
        Coleta preços do Metals.live.
        
        Returns:
            Dict com preços por metal
        """
        session = await self._get_session()
        prices = {}
        
        try:
            # Metals.live tem API simples
            async with session.get("https://api.metals.live/v1/spot") as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Mapear para nossos códigos
                    mapping = {
                        "gold": "XAU",
                        "silver": "XAG",
                        "platinum": "XPT",
                        "palladium": "XPD",
                        "copper": "XCU",
                        "aluminum": "XAL",
                        "nickel": "XNI",
                        "lead": "XPB",
                        "zinc": "XZN",
                        "tin": "XSN",
                    }
                    
                    for item in data:
                        metal_name = item.get("metal", "").lower()
                        if metal_name in mapping:
                            code = mapping[metal_name]
                            price = float(item.get("price", 0))
                            change = float(item.get("change", 0))
                            
                            prices[code] = PriceData(
                                metal=code,
                                price=price,
                                change_value=change,
                                change_percent=(change / price * 100) if price else 0,
                                source="metals.live",
                            )
                    
                    logger.debug(f"Metals.live: {len(prices)} preços obtidos")
                else:
                    logger.warning(f"Metals.live retornou status {response.status}")
                    
        except Exception as e:
            logger.error(f"Erro ao coletar metals.live: {e}")
            self.db.log_error("collector", "metals.live", str(e), "tentando fonte alternativa")
        
        return prices
    
    async def _fetch_kitco(self) -> Dict[str, PriceData]:
        """
        Coleta preços do Kitco (scraping).
        
        Returns:
            Dict com preços por metal
        """
        session = await self._get_session()
        prices = {}
        
        try:
            async with session.get("https://www.kitco.com/market/") as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Procurar tabelas de preço
                    # Kitco tem estrutura variável, esse é um exemplo simplificado
                    price_elements = soup.select('[data-symbol]')
                    
                    mapping = {
                        "XAUUSD": "XAU",
                        "XAGUSD": "XAG",
                        "XPTUSD": "XPT",
                        "XPDUSD": "XPD",
                    }
                    
                    for elem in price_elements:
                        symbol = elem.get('data-symbol', '')
                        if symbol in mapping:
                            price_text = elem.get_text(strip=True)
                            try:
                                price = float(re.sub(r'[^\d.]', '', price_text))
                                code = mapping[symbol]
                                prices[code] = PriceData(
                                    metal=code,
                                    price=price,
                                    source="kitco",
                                )
                            except ValueError:
                                pass
                    
                    logger.debug(f"Kitco: {len(prices)} preços obtidos")
                    
        except Exception as e:
            logger.error(f"Erro ao coletar kitco: {e}")
            self.db.log_error("collector", "kitco", str(e))
        
        return prices
    
    async def _fetch_yahoo_finance(self, symbol: str) -> Optional[PriceData]:
        """
        Coleta preço do Yahoo Finance.
        
        Args:
            symbol: Símbolo Yahoo (ex: GC=F para ouro)
        
        Returns:
            PriceData ou None
        """
        session = await self._get_session()
        
        mapping = {
            "GC=F": "XAU",   # Gold
            "SI=F": "XAG",   # Silver
            "PL=F": "XPT",   # Platinum
            "PA=F": "XPD",   # Palladium
            "HG=F": "XCU",   # Copper
        }
        
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    result = data.get("chart", {}).get("result", [{}])[0]
                    meta = result.get("meta", {})
                    
                    price = meta.get("regularMarketPrice", 0)
                    prev_close = meta.get("previousClose", price)
                    
                    code = mapping.get(symbol)
                    if code and price:
                        change = price - prev_close
                        return PriceData(
                            metal=code,
                            price=price,
                            change_value=change,
                            change_percent=(change / prev_close * 100) if prev_close else 0,
                            high_24h=meta.get("regularMarketDayHigh"),
                            low_24h=meta.get("regularMarketDayLow"),
                            volume=meta.get("regularMarketVolume"),
                            source="yahoo",
                        )
                        
        except Exception as e:
            logger.error(f"Erro ao coletar Yahoo {symbol}: {e}")
        
        return None
    
    async def _fetch_uranium_price(self) -> Optional[PriceData]:
        """
        Coleta preço de urânio (UxC spot).
        
        Returns:
            PriceData ou None
        """
        session = await self._get_session()
        
        try:
            # Camteco ou UxC publicam preços de urânio
            # Usando scraping como exemplo
            async with session.get("https://www.cameco.com/invest/markets/uranium-price") as response:
                if response.status == 200:
                    html = await response.text()
                    # Parsear preço do HTML
                    match = re.search(r'\$[\d,.]+/lb', html)
                    if match:
                        price_str = match.group().replace('$', '').replace('/lb', '').replace(',', '')
                        price = float(price_str)
                        return PriceData(
                            metal="UX",
                            price=price,
                            source="cameco",
                        )
        except Exception as e:
            logger.error(f"Erro ao coletar preço urânio: {e}")
        
        return None
    
    async def _fetch_iron_ore_price(self) -> Optional[PriceData]:
        """
        Coleta preço de minério de ferro.
        
        Returns:
            PriceData ou None
        """
        session = await self._get_session()
        
        try:
            # Trading Economics tem dados de minério de ferro
            # Usando exemplo simplificado
            async with session.get("https://tradingeconomics.com/commodity/iron-ore") as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    price_elem = soup.select_one('[id*="iron"]')
                    if price_elem:
                        price_text = price_elem.get_text(strip=True)
                        price = float(re.sub(r'[^\d.]', '', price_text))
                        return PriceData(
                            metal="FE",
                            price=price,
                            source="tradingeconomics",
                        )
        except Exception as e:
            logger.error(f"Erro ao coletar preço minério de ferro: {e}")
        
        return None
    
    async def collect_all_prices(self) -> Dict[str, PriceData]:
        """
        Coleta preços de todas as fontes.
        
        Returns:
            Dict com todos os preços
        """
        all_prices = {}
        
        # Coletar de múltiplas fontes em paralelo
        tasks = [
            self._fetch_metals_live(),
            self._fetch_kitco(),
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Mesclar resultados (metals.live tem prioridade)
        for result in results:
            if isinstance(result, dict):
                for code, price_data in result.items():
                    if code not in all_prices:
                        all_prices[code] = price_data
        
        # Coletar Yahoo para metais que faltam
        yahoo_symbols = ["GC=F", "SI=F", "PL=F", "PA=F", "HG=F"]
        for symbol in yahoo_symbols:
            mapping = {"GC=F": "XAU", "SI=F": "XAG", "PL=F": "XPT", "PA=F": "XPD", "HG=F": "XCU"}
            code = mapping.get(symbol)
            if code and code not in all_prices:
                price_data = await self._fetch_yahoo_finance(symbol)
                if price_data:
                    all_prices[code] = price_data
        
        # Coletar urânio e minério de ferro
        ux_price = await self._fetch_uranium_price()
        if ux_price:
            all_prices["UX"] = ux_price
        
        fe_price = await self._fetch_iron_ore_price()
        if fe_price:
            all_prices["FE"] = fe_price
        
        # Atualizar histórico interno
        for code, price_data in all_prices.items():
            self.last_prices[code] = price_data
            
            # Salvar no banco
            self.db.add_price(code, price_data.price, price_data.volume)
            
            # Manter histórico em memória (últimas 24h)
            if code not in self.price_history:
                self.price_history[code] = []
            self.price_history[code].append((price_data.timestamp, price_data.price))
            
            # Limpar entradas antigas (>24h)
            cutoff = datetime.utcnow() - timedelta(hours=24)
            self.price_history[code] = [
                (ts, p) for ts, p in self.price_history[code] if ts > cutoff
            ]
        
        logger.info(f"Coletados {len(all_prices)} preços")
        return all_prices
    
    def get_last_price(self, metal: str) -> Optional[PriceData]:
        """
        Obtém último preço de um metal.
        
        Args:
            metal: Código do metal
        
        Returns:
            PriceData ou None
        """
        return self.last_prices.get(metal.upper())
    
    def get_all_last_prices(self) -> Dict[str, PriceData]:
        """Retorna todos os últimos preços."""
        return self.last_prices.copy()
    
    def calculate_change(self, metal: str, minutes: int) -> Optional[Tuple[float, float]]:
        """
        Calcula mudança de preço em um período.
        
        Args:
            metal: Código do metal
            minutes: Período em minutos
        
        Returns:
            Tuple (change_percent, change_value) ou None
        """
        history = self.price_history.get(metal.upper(), [])
        if not history:
            return None
        
        current_price = history[-1][1]
        cutoff = datetime.utcnow() - timedelta(minutes=minutes)
        
        # Encontrar preço mais próximo do período
        old_prices = [(ts, p) for ts, p in history if ts <= cutoff]
        if not old_prices:
            return None
        
        old_price = old_prices[-1][1]
        change_value = current_price - old_price
        change_percent = (change_value / old_price) * 100 if old_price else 0
        
        return (change_percent, change_value)
    
    def check_price_alerts(self) -> List[Dict]:
        """
        Verifica se algum preço disparou alerta.
        
        Returns:
            Lista de alertas a serem enviados
        """
        alerts = []
        
        for metal in METAIS.keys():
            for level in AlertLevel:
                threshold = ALERT_THRESHOLDS[level]
                minutes = threshold["timeframe_minutes"]
                percent = threshold["percent_change"]
                
                change = self.calculate_change(metal, minutes)
                if change:
                    change_percent, change_value = change
                    if abs(change_percent) >= percent:
                        current = self.get_last_price(metal)
                        if current:
                            alerts.append({
                                "level": level,
                                "metal": metal,
                                "change_percent": change_percent,
                                "change_value": change_value,
                                "current_price": current.price,
                                "timeframe_minutes": minutes,
                            })
        
        return alerts
    
    def get_price_summary(self) -> Dict[str, Dict]:
        """
        Retorna resumo de preços para todos os metais.
        
        Returns:
            Dict organizado por tipo de metal
        """
        summary = {
            "preciosos": {},
            "industriais": {},
            "estrategicos": {},
        }
        
        for code, metal in METAIS.items():
            price_data = self.last_prices.get(code)
            if price_data:
                tipo = metal.tipo + "s"
                if tipo == "precioso":
                    tipo = "preciosos"
                elif tipo == "industrial":
                    tipo = "industriais"
                else:
                    tipo = "estrategicos"
                
                summary[tipo][code] = {
                    "nome": metal.nome,
                    "emoji": metal.emoji,
                    "price": price_data.price,
                    "change_percent": price_data.change_percent,
                }
        
        return summary


# Singleton
_collector: Optional[PriceCollector] = None


def get_price_collector() -> PriceCollector:
    """Retorna instância singleton do coletor de preços."""
    global _collector
    if _collector is None:
        _collector = PriceCollector()
    return _collector
