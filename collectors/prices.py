"""
OpusDeiTradeMetaL - Coletor de Preços COMPLETO
===============================================
Coleta preços de metais de múltiplas fontes com fallback robusto.

Fontes:
- Metals.live API (principal - real-time)
- Kitco (backup - scraping)
- Yahoo Finance (backup - API)
- Investing.com (backup - scraping)
- MetalPriceAPI (backup)

Metais cobertos:
- Preciosos: XAU, XAG, XPT, XPD
- Industriais: XCU, XAL, XNI, XPB, XZN, XSN
- Estratégicos: UX (Urânio), FE (Minério de Ferro)
"""

import aiohttp
import asyncio
import logging
import re
import json
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple, Any
from dataclasses import dataclass, field
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod

from config.settings import METAIS, ALERT_THRESHOLDS, AlertLevel
from storage.database import get_database

logger = logging.getLogger(__name__)


@dataclass
class PriceData:
    """Dados de preço de um metal."""
    metal: str
    price: float
    currency: str = "USD"
    unit: str = "oz"  # oz, ton, lb, kg
    change_percent: float = 0.0
    change_value: float = 0.0
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None
    open_24h: Optional[float] = None
    volume: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    spread: Optional[float] = None
    source: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    reliability: int = 100  # 0-100, quanto maior mais confiável
    
    def __post_init__(self):
        if self.bid and self.ask:
            self.spread = self.ask - self.bid
    
    def to_dict(self) -> Dict:
        return {
            "metal": self.metal,
            "price": self.price,
            "currency": self.currency,
            "unit": self.unit,
            "change_percent": self.change_percent,
            "change_value": self.change_value,
            "high_24h": self.high_24h,
            "low_24h": self.low_24h,
            "open_24h": self.open_24h,
            "volume": self.volume,
            "bid": self.bid,
            "ask": self.ask,
            "spread": self.spread,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "reliability": self.reliability,
        }
    
    def convert_to_oz(self) -> 'PriceData':
        """Converte preço para onças troy se necessário."""
        conversions = {
            "kg": 32.1507,    # 1 kg = 32.1507 oz troy
            "g": 0.0321507,   # 1 g = 0.0321507 oz troy
            "lb": 14.5833,    # 1 lb = 14.5833 oz troy
            "ton": 32150.7,   # 1 ton métrica = 32150.7 oz troy
        }
        
        if self.unit == "oz":
            return self
        
        factor = conversions.get(self.unit, 1)
        return PriceData(
            metal=self.metal,
            price=self.price / factor,
            currency=self.currency,
            unit="oz",
            change_percent=self.change_percent,
            change_value=self.change_value / factor if self.change_value else 0,
            high_24h=self.high_24h / factor if self.high_24h else None,
            low_24h=self.low_24h / factor if self.low_24h else None,
            volume=self.volume,
            bid=self.bid / factor if self.bid else None,
            ask=self.ask / factor if self.ask else None,
            source=self.source,
            timestamp=self.timestamp,
            reliability=self.reliability,
        )


class PriceSource(ABC):
    """Classe base para fontes de preço."""
    
    name: str = "base"
    priority: int = 0  # Menor = maior prioridade
    reliability: int = 100
    
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.db = get_database()
        self.last_success: Optional[datetime] = None
        self.consecutive_failures: int = 0
    
    @abstractmethod
    async def fetch_prices(self) -> Dict[str, PriceData]:
        """Coleta preços da fonte."""
        pass
    
    async def fetch_with_retry(self, retries: int = 3) -> Dict[str, PriceData]:
        """Coleta com retry automático."""
        for attempt in range(retries):
            try:
                prices = await self.fetch_prices()
                if prices:
                    self.last_success = datetime.utcnow()
                    self.consecutive_failures = 0
                    logger.info(f"{self.name}: {len(prices)} preços coletados")
                    return prices
            except Exception as e:
                self.consecutive_failures += 1
                logger.warning(f"{self.name} tentativa {attempt + 1}/{retries} falhou: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
        
        self.db.log_error("collector", self.name, f"Falhou após {retries} tentativas")
        return {}


class MetalsLiveSource(PriceSource):
    """
    Metals.live - Fonte principal de preços spot.
    
    API gratuita com preços em tempo real.
    Atualiza a cada poucos segundos.
    """
    
    name = "metals.live"
    priority = 1
    reliability = 95
    
    # Endpoints
    SPOT_URL = "https://api.metals.live/v1/spot"
    SPOT_ALL_URL = "https://api.metals.live/v1/spot/all"
    
    # Mapeamento de nomes para nossos códigos
    METAL_MAP = {
        "gold": "XAU",
        "silver": "XAG",
        "platinum": "XPT",
        "palladium": "XPD",
        "copper": "XCU",
        "aluminum": "XAL",
        "aluminium": "XAL",
        "nickel": "XNI",
        "lead": "XPB",
        "zinc": "XZN",
        "tin": "XSN",
    }
    
    async def fetch_prices(self) -> Dict[str, PriceData]:
        prices = {}
        
        try:
            # Tentar endpoint principal
            async with self.session.get(self.SPOT_URL, timeout=15) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Metals.live retorna lista de objetos
                    if isinstance(data, list):
                        for item in data:
                            metal_name = item.get("metal", "").lower()
                            code = self.METAL_MAP.get(metal_name)
                            
                            if code:
                                price = float(item.get("price", 0))
                                if price > 0:
                                    # Calcular variação se tiver dados anteriores
                                    change = float(item.get("change", 0))
                                    change_pct = float(item.get("change_percent", 0))
                                    
                                    # Se não tiver change_percent, calcular
                                    if change and not change_pct and price:
                                        prev_price = price - change
                                        if prev_price > 0:
                                            change_pct = (change / prev_price) * 100
                                    
                                    prices[code] = PriceData(
                                        metal=code,
                                        price=price,
                                        change_value=change,
                                        change_percent=change_pct,
                                        high_24h=float(item.get("high", 0)) or None,
                                        low_24h=float(item.get("low", 0)) or None,
                                        open_24h=float(item.get("open", 0)) or None,
                                        bid=float(item.get("bid", 0)) or None,
                                        ask=float(item.get("ask", 0)) or None,
                                        source=self.name,
                                        reliability=self.reliability,
                                    )
                    
                    # Se retornar objeto único
                    elif isinstance(data, dict):
                        for metal_name, value in data.items():
                            code = self.METAL_MAP.get(metal_name.lower())
                            if code and isinstance(value, (int, float)):
                                prices[code] = PriceData(
                                    metal=code,
                                    price=float(value),
                                    source=self.name,
                                    reliability=self.reliability,
                                )
                                
        except asyncio.TimeoutError:
            logger.warning(f"{self.name}: Timeout")
        except Exception as e:
            logger.error(f"{self.name}: Erro - {e}")
        
        return prices


class KitcoSource(PriceSource):
    """
    Kitco - Fonte tradicional de preços de metais preciosos.
    
    Scraping da página principal.
    Atualização frequente, muito confiável para preciosos.
    """
    
    name = "kitco"
    priority = 2
    reliability = 90
    
    BASE_URL = "https://www.kitco.com"
    PRICES_URL = "https://www.kitco.com/market/"
    
    # User agent para evitar bloqueio
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    
    # Mapeamento de símbolos Kitco
    SYMBOL_MAP = {
        "AU": "XAU",
        "AG": "XAG", 
        "PT": "XPT",
        "PD": "XPD",
        "GOLD": "XAU",
        "SILVER": "XAG",
        "PLATINUM": "XPT",
        "PALLADIUM": "XPD",
    }
    
    async def fetch_prices(self) -> Dict[str, PriceData]:
        prices = {}
        
        try:
            async with self.session.get(
                self.PRICES_URL, 
                headers=self.HEADERS,
                timeout=20
            ) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Método 1: Procurar tabela de preços
                    price_tables = soup.select('table.price-table, .metal-prices-table, #price-table')
                    
                    for table in price_tables:
                        rows = table.select('tr')
                        for row in rows:
                            cells = row.select('td')
                            if len(cells) >= 2:
                                metal_cell = cells[0].get_text(strip=True).upper()
                                price_cell = cells[1].get_text(strip=True)
                                
                                # Identificar metal
                                code = None
                                for symbol, mapped in self.SYMBOL_MAP.items():
                                    if symbol in metal_cell:
                                        code = mapped
                                        break
                                
                                if code:
                                    # Extrair preço
                                    price_match = re.search(r'[\d,]+\.?\d*', price_cell.replace(',', ''))
                                    if price_match:
                                        price = float(price_match.group())
                                        
                                        # Tentar extrair variação
                                        change_pct = 0
                                        if len(cells) >= 4:
                                            change_cell = cells[3].get_text(strip=True)
                                            change_match = re.search(r'[+-]?\d+\.?\d*', change_cell)
                                            if change_match:
                                                change_pct = float(change_match.group())
                                        
                                        prices[code] = PriceData(
                                            metal=code,
                                            price=price,
                                            change_percent=change_pct,
                                            source=self.name,
                                            reliability=self.reliability,
                                        )
                    
                    # Método 2: Procurar elementos com data attributes
                    if not prices:
                        price_elements = soup.select('[data-symbol], [data-metal], .metal-price')
                        
                        for elem in price_elements:
                            symbol = (elem.get('data-symbol') or elem.get('data-metal') or '').upper()
                            code = self.SYMBOL_MAP.get(symbol)
                            
                            if code:
                                price_text = elem.get_text(strip=True)
                                price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                                if price_match:
                                    prices[code] = PriceData(
                                        metal=code,
                                        price=float(price_match.group()),
                                        source=self.name,
                                        reliability=self.reliability - 5,  # Menos confiável via scraping genérico
                                    )
                    
                    # Método 3: Procurar JSON embutido
                    if not prices:
                        scripts = soup.select('script')
                        for script in scripts:
                            script_text = script.string or ''
                            if 'gold' in script_text.lower() and 'price' in script_text.lower():
                                # Tentar extrair JSON
                                json_match = re.search(r'\{[^{}]*"gold"[^{}]*\}', script_text, re.I)
                                if json_match:
                                    try:
                                        data = json.loads(json_match.group())
                                        for key, value in data.items():
                                            code = self.SYMBOL_MAP.get(key.upper())
                                            if code and isinstance(value, (int, float)):
                                                prices[code] = PriceData(
                                                    metal=code,
                                                    price=float(value),
                                                    source=self.name,
                                                    reliability=self.reliability - 10,
                                                )
                                    except json.JSONDecodeError:
                                        pass
                                        
        except Exception as e:
            logger.error(f"{self.name}: Erro - {e}")
        
        return prices


class YahooFinanceSource(PriceSource):
    """
    Yahoo Finance - Backup confiável com API.
    
    Usa futuros como proxy para spot.
    Bom para dados históricos e volume.
    """
    
    name = "yahoo"
    priority = 3
    reliability = 85
    
    BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/"
    
    # Símbolos de futuros Yahoo
    SYMBOLS = {
        "GC=F": "XAU",      # Gold Futures
        "SI=F": "XAG",      # Silver Futures
        "PL=F": "XPT",      # Platinum Futures
        "PA=F": "XPD",      # Palladium Futures
        "HG=F": "XCU",      # Copper Futures
        "ALI=F": "XAL",     # Aluminum Futures (pode não existir)
    }
    
    async def _fetch_symbol(self, symbol: str) -> Optional[PriceData]:
        """Coleta dados de um símbolo específico."""
        try:
            url = f"{self.BASE_URL}{symbol}"
            params = {
                "interval": "1d",
                "range": "2d",
            }
            
            async with self.session.get(url, params=params, timeout=15) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    result = data.get("chart", {}).get("result", [])
                    if not result:
                        return None
                    
                    meta = result[0].get("meta", {})
                    indicators = result[0].get("indicators", {})
                    quote = indicators.get("quote", [{}])[0]
                    
                    price = meta.get("regularMarketPrice", 0)
                    prev_close = meta.get("previousClose", 0) or meta.get("chartPreviousClose", 0)
                    
                    if not price:
                        return None
                    
                    # Calcular variação
                    change = price - prev_close if prev_close else 0
                    change_pct = (change / prev_close * 100) if prev_close else 0
                    
                    code = self.SYMBOLS.get(symbol)
                    
                    return PriceData(
                        metal=code,
                        price=price,
                        change_value=change,
                        change_percent=change_pct,
                        high_24h=meta.get("regularMarketDayHigh"),
                        low_24h=meta.get("regularMarketDayLow"),
                        open_24h=meta.get("regularMarketOpen"),
                        volume=meta.get("regularMarketVolume"),
                        source=self.name,
                        reliability=self.reliability,
                    )
                    
        except Exception as e:
            logger.debug(f"{self.name} {symbol}: {e}")
        
        return None
    
    async def fetch_prices(self) -> Dict[str, PriceData]:
        prices = {}
        
        # Coletar todos em paralelo
        tasks = [self._fetch_symbol(symbol) for symbol in self.SYMBOLS.keys()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, PriceData) and result.price > 0:
                prices[result.metal] = result
        
        return prices


class InvestingComSource(PriceSource):
    """
    Investing.com - Fonte alternativa com muitos metais.
    
    Scraping, mas tem dados de industriais que outras não têm.
    """
    
    name = "investing"
    priority = 4
    reliability = 80
    
    BASE_URL = "https://www.investing.com/commodities/"
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }
    
    # URLs específicas por metal
    METAL_URLS = {
        "XAU": "gold",
        "XAG": "silver",
        "XPT": "platinum",
        "XPD": "palladium",
        "XCU": "copper",
        "XAL": "aluminum",
        "XNI": "nickel",
        "XPB": "lead",
        "XZN": "zinc",
        "XSN": "tin",
        "UX": "uranium",
    }
    
    async def _fetch_metal(self, code: str, url_part: str) -> Optional[PriceData]:
        """Coleta preço de um metal específico."""
        try:
            url = f"{self.BASE_URL}{url_part}"
            
            async with self.session.get(url, headers=self.HEADERS, timeout=20) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Procurar preço principal
                    price_elem = soup.select_one('[data-test="instrument-price-last"], .instrument-price_last__KQzyA, .last-price')
                    
                    if price_elem:
                        price_text = price_elem.get_text(strip=True)
                        price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                        
                        if price_match:
                            price = float(price_match.group())
                            
                            # Procurar variação
                            change_pct = 0
                            change_elem = soup.select_one('[data-test="instrument-price-change-percent"], .instrument-price_change-percent')
                            if change_elem:
                                change_text = change_elem.get_text(strip=True)
                                change_match = re.search(r'[+-]?\d+\.?\d*', change_text)
                                if change_match:
                                    change_pct = float(change_match.group())
                            
                            return PriceData(
                                metal=code,
                                price=price,
                                change_percent=change_pct,
                                source=self.name,
                                reliability=self.reliability,
                            )
                            
        except Exception as e:
            logger.debug(f"{self.name} {code}: {e}")
        
        return None
    
    async def fetch_prices(self) -> Dict[str, PriceData]:
        prices = {}
        
        # Limitar concorrência para não sobrecarregar
        semaphore = asyncio.Semaphore(3)
        
        async def fetch_with_semaphore(code: str, url: str):
            async with semaphore:
                return await self._fetch_metal(code, url)
        
        tasks = [fetch_with_semaphore(code, url) for code, url in self.METAL_URLS.items()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, PriceData) and result.price > 0:
                prices[result.metal] = result
        
        return prices


class ShanghaiGoldExchangeSource(PriceSource):
    """
    Shanghai Gold Exchange (SGE) - Preços físicos da China.
    
    Importante para detectar premium/desconto vs Londres.
    """
    
    name = "sge"
    priority = 5
    reliability = 85
    
    # SGE tem API limitada, principalmente scraping
    BASE_URL = "https://www.sge.com.cn/sjzx/mrhqsj"
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    
    async def fetch_prices(self) -> Dict[str, PriceData]:
        prices = {}
        
        try:
            async with self.session.get(self.BASE_URL, headers=self.HEADERS, timeout=20) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # SGE lista Au99.99 como benchmark
                    # Preço em CNY/grama
                    
                    tables = soup.select('table')
                    for table in tables:
                        rows = table.select('tr')
                        for row in rows:
                            cells = row.select('td')
                            if len(cells) >= 3:
                                product = cells[0].get_text(strip=True)
                                
                                # Au99.99 é ouro puro
                                if 'Au99.99' in product or 'Au9999' in product:
                                    price_text = cells[1].get_text(strip=True)
                                    price_match = re.search(r'[\d.]+', price_text)
                                    
                                    if price_match:
                                        price_cny_gram = float(price_match.group())
                                        
                                        # Converter CNY/g para USD/oz
                                        # Taxa aproximada - em produção usar API de câmbio
                                        usdcny = 7.25
                                        price_usd_oz = (price_cny_gram / usdcny) * 31.1035
                                        
                                        prices["XAU_SGE"] = PriceData(
                                            metal="XAU",
                                            price=price_usd_oz,
                                            currency="USD",
                                            unit="oz",
                                            source=f"{self.name} (convertido)",
                                            reliability=self.reliability - 5,  # Menos confiável por conversão
                                        )
                                        
                                        # Também salvar em CNY para cálculo de premium
                                        prices["XAU_SGE_CNY"] = PriceData(
                                            metal="XAU",
                                            price=price_cny_gram,
                                            currency="CNY",
                                            unit="g",
                                            source=self.name,
                                            reliability=self.reliability,
                                        )
                                
                                # Ag(T+D) para prata
                                elif 'Ag' in product and 'T+D' in product:
                                    price_text = cells[1].get_text(strip=True)
                                    price_match = re.search(r'[\d.]+', price_text)
                                    
                                    if price_match:
                                        price_cny_kg = float(price_match.group())
                                        usdcny = 7.25
                                        # Prata SGE é em CNY/kg
                                        price_usd_oz = (price_cny_kg / usdcny / 1000) * 31.1035
                                        
                                        prices["XAG_SGE"] = PriceData(
                                            metal="XAG",
                                            price=price_usd_oz,
                                            currency="USD",
                                            unit="oz",
                                            source=f"{self.name} (convertido)",
                                            reliability=self.reliability - 5,
                                        )
                                        
        except Exception as e:
            logger.error(f"{self.name}: Erro - {e}")
        
        return prices


class UraniumSource(PriceSource):
    """
    Fonte especializada para preço de urânio.
    
    Urânio não é negociado em bolsa tradicional.
    Usa Cameco e UxC como referência.
    """
    
    name = "uranium"
    priority = 6
    reliability = 75
    
    CAMECO_URL = "https://www.cameco.com/invest/markets/uranium-price"
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    
    async def fetch_prices(self) -> Dict[str, PriceData]:
        prices = {}
        
        try:
            async with self.session.get(self.CAMECO_URL, headers=self.HEADERS, timeout=20) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Procurar preço spot de urânio
                    # Formato típico: "$XX.XX /lb U₃O₈"
                    price_patterns = [
                        r'\$\s*([\d.]+)\s*/\s*lb',
                        r'([\d.]+)\s*USD\s*/\s*lb',
                        r'Spot.*?\$\s*([\d.]+)',
                    ]
                    
                    text = soup.get_text()
                    
                    for pattern in price_patterns:
                        match = re.search(pattern, text, re.I)
                        if match:
                            price = float(match.group(1))
                            
                            prices["UX"] = PriceData(
                                metal="UX",
                                price=price,
                                currency="USD",
                                unit="lb",  # Urânio é cotado em libras
                                source=self.name,
                                reliability=self.reliability,
                            )
                            break
                    
        except Exception as e:
            logger.error(f"{self.name}: Erro - {e}")
        
        return prices


class IronOreSource(PriceSource):
    """
    Fonte para preço de minério de ferro.
    
    Referência: SGX (Singapore Exchange) e DCE (Dalian).
    """
    
    name = "ironore"
    priority = 6
    reliability = 75
    
    # Trading Economics como backup
    URL = "https://tradingeconomics.com/commodity/iron-ore"
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    
    async def fetch_prices(self) -> Dict[str, PriceData]:
        prices = {}
        
        try:
            async with self.session.get(self.URL, headers=self.HEADERS, timeout=20) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Procurar preço principal
                    price_elem = soup.select_one('#p, .last-price, [id*="iron"]')
                    
                    if price_elem:
                        price_text = price_elem.get_text(strip=True)
                        price_match = re.search(r'[\d.]+', price_text)
                        
                        if price_match:
                            price = float(price_match.group())
                            
                            prices["FE"] = PriceData(
                                metal="FE",
                                price=price,
                                currency="USD",
                                unit="ton",  # Minério de ferro é cotado em toneladas
                                source=self.name,
                                reliability=self.reliability,
                            )
                    
                    # Procurar em scripts JSON
                    if "FE" not in prices:
                        scripts = soup.select('script')
                        for script in scripts:
                            if script.string and 'iron' in script.string.lower():
                                price_match = re.search(r'"last":\s*([\d.]+)', script.string)
                                if price_match:
                                    prices["FE"] = PriceData(
                                        metal="FE",
                                        price=float(price_match.group(1)),
                                        currency="USD",
                                        unit="ton",
                                        source=self.name,
                                        reliability=self.reliability - 5,
                                    )
                                    break
                                    
        except Exception as e:
            logger.error(f"{self.name}: Erro - {e}")
        
        return prices


class PriceCollector:
    """
    Coletor principal de preços com múltiplas fontes e fallback.
    
    Estratégia:
    1. Tenta fonte principal (Metals.live)
    2. Se falhar, tenta fontes em ordem de prioridade
    3. Combina dados de múltiplas fontes para completude
    4. Valida dados (preços absurdos, timestamps antigos)
    5. Calcula variações em múltiplos timeframes
    """
    
    def __init__(self):
        self.db = get_database()
        self.session: Optional[aiohttp.ClientSession] = None
        self.sources: List[PriceSource] = []
        
        # Cache de preços
        self.last_prices: Dict[str, PriceData] = {}
        self.price_history: Dict[str, List[Tuple[datetime, float]]] = {}
        
        # Limites de validação (preços absurdos)
        self.price_limits = {
            "XAU": (1000, 5000),    # Ouro: $1000-$5000/oz
            "XAG": (10, 100),       # Prata: $10-$100/oz
            "XPT": (500, 2000),     # Platina: $500-$2000/oz
            "XPD": (500, 3000),     # Paládio: $500-$3000/oz
            "XCU": (2, 10),         # Cobre: $2-$10/lb
            "XAL": (1000, 5000),    # Alumínio: $1000-$5000/ton
            "XNI": (10000, 50000),  # Níquel: $10k-$50k/ton
            "XPB": (1000, 5000),    # Chumbo: $1k-$5k/ton
            "XZN": (1500, 6000),    # Zinco: $1.5k-$6k/ton
            "XSN": (15000, 50000),  # Estanho: $15k-$50k/ton
            "UX": (20, 200),        # Urânio: $20-$200/lb
            "FE": (50, 300),        # Minério ferro: $50-$300/ton
        }
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Obtém ou cria sessão HTTP."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            connector = aiohttp.TCPConnector(limit=20, limit_per_host=5)
            self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
            
            # Inicializar fontes
            self.sources = [
                MetalsLiveSource(self.session),
                KitcoSource(self.session),
                YahooFinanceSource(self.session),
                InvestingComSource(self.session),
                ShanghaiGoldExchangeSource(self.session),
                UraniumSource(self.session),
                IronOreSource(self.session),
            ]
            self.sources.sort(key=lambda x: x.priority)
            
        return self.session
    
    async def close(self):
        """Fecha sessão HTTP."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    def _validate_price(self, price_data: PriceData) -> bool:
        """
        Valida se preço está dentro de limites razoáveis.
        
        Evita dados corrompidos ou erros de parsing.
        """
        limits = self.price_limits.get(price_data.metal)
        if not limits:
            return True  # Sem limite definido, aceita
        
        min_price, max_price = limits
        
        # Verificar se está dentro dos limites
        if not (min_price <= price_data.price <= max_price):
            logger.warning(
                f"Preço fora dos limites: {price_data.metal} = {price_data.price} "
                f"(esperado: {min_price}-{max_price})"
            )
            return False
        
        # Verificar variação absurda (>20% em um dia é suspeito)
        if abs(price_data.change_percent) > 20:
            logger.warning(
                f"Variação suspeita: {price_data.metal} = {price_data.change_percent}%"
            )
            # Não rejeita, só avisa
        
        return True
    
    def _merge_prices(self, all_results: List[Dict[str, PriceData]]) -> Dict[str, PriceData]:
        """
        Combina preços de múltiplas fontes.
        
        Prioriza por reliability e recência.
        """
        merged = {}
        
        for result in all_results:
            for code, price_data in result.items():
                # Pular dados inválidos
                if not self._validate_price(price_data):
                    continue
                
                # Se não tem ainda, adiciona
                if code not in merged:
                    merged[code] = price_data
                else:
                    # Se já tem, comparar reliability
                    existing = merged[code]
                    if price_data.reliability > existing.reliability:
                        merged[code] = price_data
                    elif price_data.reliability == existing.reliability:
                        # Mesmo reliability, pegar mais recente
                        if price_data.timestamp > existing.timestamp:
                            merged[code] = price_data
        
        return merged
    
    async def collect_all_prices(self) -> Dict[str, PriceData]:
        """
        Coleta preços de todas as fontes disponíveis.
        
        Returns:
            Dict com preços por metal
        """
        await self._get_session()
        
        all_results = []
        
        # Coletar de todas as fontes em paralelo
        tasks = [source.fetch_with_retry() for source in self.sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, dict) and result:
                all_results.append(result)
                logger.debug(f"{self.sources[i].name}: {len(result)} preços")
            elif isinstance(result, Exception):
                logger.error(f"Fonte {self.sources[i].name} falhou: {result}")
        
        # Combinar resultados
        merged = self._merge_prices(all_results)
        
        # Atualizar cache e histórico
        now = datetime.utcnow()
        for code, price_data in merged.items():
            self.last_prices[code] = price_data
            
            # Salvar no banco
            self.db.add_price(code, price_data.price, price_data.volume)
            
            # Manter histórico em memória
            if code not in self.price_history:
                self.price_history[code] = []
            self.price_history[code].append((now, price_data.price))
            
            # Limpar entradas antigas (>48h)
            cutoff = now - timedelta(hours=48)
            self.price_history[code] = [
                (ts, p) for ts, p in self.price_history[code] if ts > cutoff
            ]
        
        logger.info(f"Total coletado: {len(merged)} preços de {len(all_results)} fontes")
        return merged
    
    def get_last_price(self, metal: str) -> Optional[PriceData]:
        """Obtém último preço de um metal."""
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
    
    def get_shanghai_premium(self) -> Optional[Dict]:
        """
        Calcula Shanghai Gold Premium.
        
        Premium positivo = demanda forte na China.
        Premium negativo = excesso de oferta.
        """
        sge_price = self.last_prices.get("XAU_SGE")
        lbma_price = self.last_prices.get("XAU")
        
        if not sge_price or not lbma_price:
            return None
        
        premium_usd = sge_price.price - lbma_price.price
        premium_pct = (premium_usd / lbma_price.price) * 100
        
        # Interpretar sinal
        if premium_pct > 1.0:
            signal = "bullish_strong"
            interpretation = "Demanda física muito forte na China"
        elif premium_pct > 0.3:
            signal = "bullish"
            interpretation = "Demanda física acima do normal"
        elif premium_pct < -0.3:
            signal = "bearish"
            interpretation = "Demanda fraca, possível excesso de oferta"
        else:
            signal = "neutral"
            interpretation = "Premium em níveis normais"
        
        return {
            "sge_price": sge_price.price,
            "lbma_price": lbma_price.price,
            "premium_usd": premium_usd,
            "premium_pct": premium_pct,
            "signal": signal,
            "interpretation": interpretation,
        }
    
    def get_price_summary(self) -> Dict[str, Dict]:
        """
        Retorna resumo de preços para todos os metais.
        
        Organizado por tipo de metal.
        """
        summary = {
            "preciosos": {},
            "industriais": {},
            "estrategicos": {},
        }
        
        for code, metal in METAIS.items():
            price_data = self.last_prices.get(code)
            if price_data:
                tipo_map = {
                    "precioso": "preciosos",
                    "industrial": "industriais",
                    "estrategico": "estrategicos",
                }
                tipo = tipo_map.get(metal.tipo, "outros")
                
                summary[tipo][code] = {
                    "nome": metal.nome,
                    "emoji": metal.emoji,
                    "price": price_data.price,
                    "change_percent": price_data.change_percent,
                    "source": price_data.source,
                    "reliability": price_data.reliability,
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