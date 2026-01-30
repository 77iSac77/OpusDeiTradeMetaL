"""
OpusDeiTradeMetaL - Coletor de Dados Institucionais
====================================================
Coleta COT, ETF flows, dados on-chain e posicionamento institucional.
"""

import aiohttp
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from bs4 import BeautifulSoup
import re

from config.settings import (
    ETHERSCAN_API_KEY, DATA_SOURCES, METAIS,
    WHALE_ALERT_THRESHOLD_USD, ETF_FLOW_THRESHOLD_USD
)
from storage.database import get_database

logger = logging.getLogger(__name__)


@dataclass
class COTData:
    """Dados do Commitment of Traders Report."""
    metal: str
    report_date: datetime
    # Managed Money (especuladores)
    mm_long: int = 0
    mm_short: int = 0
    mm_net: int = 0
    mm_change: int = 0
    # Comerciais (hedgers)
    comm_long: int = 0
    comm_short: int = 0
    comm_net: int = 0
    comm_change: int = 0
    # Swap Dealers
    swap_long: int = 0
    swap_short: int = 0
    swap_net: int = 0
    # Open Interest
    open_interest: int = 0
    oi_change: int = 0
    
    def to_dict(self) -> Dict:
        return {
            "metal": self.metal,
            "report_date": self.report_date.isoformat(),
            "managed_money": {
                "long": self.mm_long,
                "short": self.mm_short,
                "net": self.mm_net,
                "change": self.mm_change,
            },
            "commercials": {
                "long": self.comm_long,
                "short": self.comm_short,
                "net": self.comm_net,
                "change": self.comm_change,
            },
            "open_interest": self.open_interest,
        }


@dataclass
class ETFFlow:
    """Dados de fluxo de ETF."""
    etf_symbol: str
    metal: str
    holdings_tons: float = 0
    holdings_value_usd: float = 0
    flow_tons: float = 0
    flow_value_usd: float = 0
    date: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict:
        return {
            "etf": self.etf_symbol,
            "metal": self.metal,
            "holdings_tons": self.holdings_tons,
            "holdings_value_usd": self.holdings_value_usd,
            "flow_tons": self.flow_tons,
            "flow_value_usd": self.flow_value_usd,
            "date": self.date.isoformat(),
        }


@dataclass
class OnChainMovement:
    """Movimento on-chain de metal tokenizado."""
    token: str
    metal: str
    amount: float
    value_usd: float
    from_address: str
    to_address: str
    tx_hash: str
    timestamp: datetime
    movement_type: str = ""  # mint, burn, transfer, exchange_deposit, exchange_withdrawal
    
    def to_dict(self) -> Dict:
        return {
            "token": self.token,
            "metal": self.metal,
            "amount": self.amount,
            "value_usd": self.value_usd,
            "from": self.from_address,
            "to": self.to_address,
            "tx_hash": self.tx_hash,
            "timestamp": self.timestamp.isoformat(),
            "type": self.movement_type,
        }


class InstitutionalCollector:
    """Coletor de dados institucionais."""
    
    def __init__(self):
        self.db = get_database()
        self.session: Optional[aiohttp.ClientSession] = None
        self.cot_data: Dict[str, COTData] = {}
        self.etf_data: Dict[str, ETFFlow] = {}
        
        # Contratos conhecidos
        self.token_contracts = {
            "PAXG": "0x45804880De22913dAFE09f4980848ECE6EcbAf78",
            "XAUT": "0x68749665FF8D2d112Fa859AA293F07A622782F38",
        }
        
        # Endereços de exchanges conhecidos
        self.exchange_addresses = {
            "binance": ["0x28c6c06298d514db089934071355e5743bf21d60"],
            "kraken": ["0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0"],
            "coinbase": ["0x71660c4005ba85c37ccec55d0c4493e66fe775d3"],
        }
    
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
    
    # =========================================================================
    # CFTC COT REPORT
    # =========================================================================
    
    async def fetch_cot_report(self) -> Dict[str, COTData]:
        """
        Coleta dados do COT Report da CFTC.
        
        Returns:
            Dict com dados por metal
        """
        session = await self._get_session()
        cot_data = {}
        
        try:
            # CFTC publica em formato texto/CSV
            # URL para futuros de commodities
            url = "https://www.cftc.gov/dea/newcot/deafut.txt"
            
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.text()
                    
                    # Mapear códigos CFTC para nossos tickers
                    cftc_map = {
                        "GOLD": "XAU",
                        "SILVER": "XAG",
                        "PLATINUM": "XPT",
                        "PALLADIUM": "XPD",
                        "COPPER": "XCU",
                    }
                    
                    lines = content.strip().split('\n')
                    
                    for line in lines[1:]:  # Pular header
                        try:
                            fields = line.split(',')
                            if len(fields) < 20:
                                continue
                            
                            market_name = fields[0].strip().upper()
                            
                            # Verificar se é um metal que monitoramos
                            metal_code = None
                            for cftc_name, code in cftc_map.items():
                                if cftc_name in market_name:
                                    metal_code = code
                                    break
                            
                            if not metal_code:
                                continue
                            
                            # Parsear campos do COT
                            # Estrutura varia, isso é simplificado
                            report_date = datetime.strptime(fields[2].strip(), "%Y-%m-%d")
                            
                            cot = COTData(
                                metal=metal_code,
                                report_date=report_date,
                                mm_long=int(fields[7]) if fields[7].strip() else 0,
                                mm_short=int(fields[8]) if fields[8].strip() else 0,
                                comm_long=int(fields[11]) if fields[11].strip() else 0,
                                comm_short=int(fields[12]) if fields[12].strip() else 0,
                                open_interest=int(fields[15]) if fields[15].strip() else 0,
                            )
                            
                            cot.mm_net = cot.mm_long - cot.mm_short
                            cot.comm_net = cot.comm_long - cot.comm_short
                            
                            cot_data[metal_code] = cot
                            self.cot_data[metal_code] = cot
                            
                        except Exception as e:
                            logger.debug(f"Erro ao parsear linha COT: {e}")
                            continue
                else:
                    logger.warning(f"CFTC retornou status {response.status}")
                    
        except Exception as e:
            logger.error(f"Erro ao coletar COT: {e}")
            self.db.log_error("collector", "CFTC", str(e))
        
        logger.info(f"Coletados COT para {len(cot_data)} metais")
        return cot_data
    
    def get_cot_for_metal(self, metal: str) -> Optional[COTData]:
        """Retorna dados COT de um metal."""
        return self.cot_data.get(metal.upper())
    
    # =========================================================================
    # ETF FLOWS
    # =========================================================================
    
    async def fetch_etf_holdings(self, etf_symbol: str) -> Optional[ETFFlow]:
        """
        Coleta holdings de um ETF.
        
        Args:
            etf_symbol: Símbolo do ETF (GLD, SLV, etc)
        
        Returns:
            ETFFlow ou None
        """
        session = await self._get_session()
        
        # Mapear ETF para metal
        etf_metal_map = {
            "GLD": "XAU",
            "IAU": "XAU",
            "SLV": "XAG",
            "PPLT": "XPT",
            "PALL": "XPD",
        }
        
        metal = etf_metal_map.get(etf_symbol)
        if not metal:
            return None
        
        try:
            # Yahoo Finance para dados de ETF
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{etf_symbol}"
            
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    result = data.get("chart", {}).get("result", [{}])[0]
                    meta = result.get("meta", {})
                    
                    # Calcular holdings aproximados
                    # GLD: cada share = ~0.093 oz de ouro
                    shares_multiplier = {
                        "GLD": 0.093,
                        "IAU": 0.01,
                        "SLV": 0.93,
                    }
                    
                    price = meta.get("regularMarketPrice", 0)
                    volume = meta.get("regularMarketVolume", 0)
                    
                    # Isso é uma aproximação
                    # Dados reais viriam do site do ETF
                    etf_flow = ETFFlow(
                        etf_symbol=etf_symbol,
                        metal=metal,
                        holdings_tons=0,  # Precisaria de dados específicos
                        holdings_value_usd=0,
                        flow_tons=0,
                        flow_value_usd=volume * price if volume and price else 0,
                    )
                    
                    self.etf_data[etf_symbol] = etf_flow
                    return etf_flow
                    
        except Exception as e:
            logger.error(f"Erro ao coletar ETF {etf_symbol}: {e}")
        
        return None
    
    async def fetch_gld_holdings(self) -> Optional[ETFFlow]:
        """
        Coleta holdings específicos do GLD (SPDR Gold).
        
        Returns:
            ETFFlow com dados detalhados
        """
        session = await self._get_session()
        
        try:
            # SPDR Gold Trust publica holdings diariamente
            url = "https://www.spdrgoldshares.com/usa/historical-data/"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Procurar tabela de holdings
                    # Estrutura específica do site
                    holdings_elem = soup.select_one('.holdings-value')
                    
                    if holdings_elem:
                        holdings_text = holdings_elem.get_text(strip=True)
                        # Parsear valor (ex: "842.50 tonnes")
                        match = re.search(r'([\d,]+\.?\d*)\s*tonnes?', holdings_text, re.I)
                        if match:
                            holdings_tons = float(match.group(1).replace(',', ''))
                            
                            return ETFFlow(
                                etf_symbol="GLD",
                                metal="XAU",
                                holdings_tons=holdings_tons,
                            )
                            
        except Exception as e:
            logger.error(f"Erro ao coletar GLD holdings: {e}")
        
        return None
    
    async def fetch_all_etf_data(self) -> Dict[str, ETFFlow]:
        """Coleta dados de todos os ETFs monitorados."""
        etfs = ["GLD", "IAU", "SLV", "PPLT", "PALL"]
        
        tasks = [self.fetch_etf_holdings(etf) for etf in etfs]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        data = {}
        for etf, result in zip(etfs, results):
            if isinstance(result, ETFFlow):
                data[etf] = result
        
        # Tentar obter dados detalhados do GLD
        gld_detailed = await self.fetch_gld_holdings()
        if gld_detailed:
            data["GLD"] = gld_detailed
        
        return data
    
    # =========================================================================
    # ON-CHAIN (PAXG, XAUT)
    # =========================================================================
    
    async def fetch_token_transfers(self, token: str, 
                                     blocks_back: int = 1000) -> List[OnChainMovement]:
        """
        Coleta transferências de token via Etherscan.
        
        Args:
            token: Nome do token (PAXG, XAUT)
            blocks_back: Quantos blocos para trás buscar
        
        Returns:
            Lista de movimentos
        """
        if not ETHERSCAN_API_KEY:
            logger.warning("Etherscan API key não configurada")
            return []
        
        contract = self.token_contracts.get(token)
        if not contract:
            return []
        
        session = await self._get_session()
        movements = []
        
        try:
            url = "https://api.etherscan.io/api"
            params = {
                "module": "account",
                "action": "tokentx",
                "contractaddress": contract,
                "page": 1,
                "offset": 100,
                "sort": "desc",
                "apikey": ETHERSCAN_API_KEY,
            }
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get("status") == "1":
                        for tx in data.get("result", []):
                            try:
                                amount = float(tx.get("value", 0)) / (10 ** int(tx.get("tokenDecimal", 18)))
                                
                                # Estimar valor em USD (1 PAXG/XAUT ≈ 1 oz gold)
                                gold_price = 2350  # Aproximado, seria obtido do coletor de preços
                                value_usd = amount * gold_price
                                
                                # Determinar tipo de movimento
                                from_addr = tx.get("from", "").lower()
                                to_addr = tx.get("to", "").lower()
                                
                                movement_type = "transfer"
                                if from_addr == "0x0000000000000000000000000000000000000000":
                                    movement_type = "mint"
                                elif to_addr == "0x0000000000000000000000000000000000000000":
                                    movement_type = "burn"
                                else:
                                    # Verificar se é depósito/saque de exchange
                                    for exchange, addrs in self.exchange_addresses.items():
                                        if to_addr in addrs:
                                            movement_type = "exchange_deposit"
                                            break
                                        elif from_addr in addrs:
                                            movement_type = "exchange_withdrawal"
                                            break
                                
                                movement = OnChainMovement(
                                    token=token,
                                    metal="XAU",
                                    amount=amount,
                                    value_usd=value_usd,
                                    from_address=from_addr,
                                    to_address=to_addr,
                                    tx_hash=tx.get("hash", ""),
                                    timestamp=datetime.fromtimestamp(int(tx.get("timeStamp", 0))),
                                    movement_type=movement_type,
                                )
                                
                                movements.append(movement)
                                
                            except Exception as e:
                                logger.debug(f"Erro ao parsear tx: {e}")
                                continue
                else:
                    logger.warning(f"Etherscan retornou status {response.status}")
                    
        except Exception as e:
            logger.error(f"Erro ao coletar transfers {token}: {e}")
            self.db.log_error("collector", "etherscan", str(e))
        
        logger.info(f"Coletados {len(movements)} movimentos de {token}")
        return movements
    
    async def fetch_all_onchain_movements(self) -> List[OnChainMovement]:
        """Coleta movimentos de todos os tokens monitorados."""
        all_movements = []
        
        for token in self.token_contracts.keys():
            movements = await self.fetch_token_transfers(token)
            all_movements.extend(movements)
        
        return all_movements
    
    def check_whale_alerts(self, movements: List[OnChainMovement]) -> List[OnChainMovement]:
        """
        Filtra movimentos que são whale alerts.
        
        Args:
            movements: Lista de movimentos
        
        Returns:
            Lista de whale alerts
        """
        return [
            m for m in movements
            if m.value_usd >= WHALE_ALERT_THRESHOLD_USD
        ]
    
    # =========================================================================
    # COMEX WAREHOUSE
    # =========================================================================
    
    async def fetch_comex_warehouse(self) -> Dict[str, Dict]:
        """
        Coleta dados de estoque do COMEX.
        
        Returns:
            Dict com dados de warehouse
        """
        session = await self._get_session()
        warehouse_data = {}
        
        try:
            # CME Group publica dados de warehouse
            url = "https://www.cmegroup.com/delivery_reports/MetalsIssuesAndStopsYTDReport.pdf"
            # Isso seria um PDF, precisaria de parsing específico
            
            # Alternativa: scraping da página de estatísticas
            url = "https://www.cmegroup.com/clearing/operations-and-deliveries/nymex-delivery-notices.html"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            # Dados exemplo (em produção viria do scraping)
            warehouse_data = {
                "XAU": {
                    "registered": 15_000_000,  # oz
                    "eligible": 10_000_000,
                    "total": 25_000_000,
                    "registered_change": -500_000,
                },
                "XAG": {
                    "registered": 280_000_000,
                    "eligible": 50_000_000,
                    "total": 330_000_000,
                    "registered_change": -2_000_000,
                },
            }
            
        except Exception as e:
            logger.error(f"Erro ao coletar COMEX warehouse: {e}")
        
        return warehouse_data
    
    # =========================================================================
    # SHANGHAI PREMIUM
    # =========================================================================
    
    async def fetch_shanghai_premium(self) -> Optional[Dict]:
        """
        Calcula Shanghai Gold Premium (SGE vs LBMA).
        
        Returns:
            Dict com dados do premium
        """
        session = await self._get_session()
        
        try:
            # SGE price
            sge_url = "https://www.sge.com.cn/"
            # Isso seria scraping do site chinês
            
            # Dados exemplo
            sge_price_cny = 545.20  # por grama
            lbma_price_usd = 2347.00  # por oz
            
            # Converter
            usdcny = 7.25  # Taxa de câmbio
            sge_price_usd_oz = (sge_price_cny / usdcny) * 31.1035  # g to oz
            
            premium_usd = sge_price_usd_oz - lbma_price_usd
            premium_percent = (premium_usd / lbma_price_usd) * 100
            
            return {
                "sge_price_cny": sge_price_cny,
                "sge_price_usd_oz": sge_price_usd_oz,
                "lbma_price_usd": lbma_price_usd,
                "premium_usd": premium_usd,
                "premium_percent": premium_percent,
                "signal": "bullish" if premium_percent > 0.5 else ("bearish" if premium_percent < -0.5 else "neutral"),
            }
            
        except Exception as e:
            logger.error(f"Erro ao calcular Shanghai premium: {e}")
        
        return None
    
    # =========================================================================
    # VERIFICAÇÃO DE ALERTAS
    # =========================================================================
    
    def check_cot_alerts(self) -> List[Dict]:
        """
        Verifica se há alertas baseados no COT.
        
        Returns:
            Lista de alertas
        """
        alerts = []
        
        for metal, cot in self.cot_data.items():
            # Alerta se Managed Money muito long ou short
            if cot.mm_net > 0:
                # Calcular percentual do OI
                if cot.open_interest > 0:
                    mm_pct = (cot.mm_net / cot.open_interest) * 100
                    if mm_pct > 30:  # Muito crowded
                        alerts.append({
                            "type": "cot_crowded_long",
                            "metal": metal,
                            "mm_net": cot.mm_net,
                            "mm_percent_oi": mm_pct,
                        })
            elif cot.mm_net < 0:
                if cot.open_interest > 0:
                    mm_pct = (abs(cot.mm_net) / cot.open_interest) * 100
                    if mm_pct > 20:
                        alerts.append({
                            "type": "cot_crowded_short",
                            "metal": metal,
                            "mm_net": cot.mm_net,
                            "mm_percent_oi": mm_pct,
                        })
            
            # Alerta se mudança semanal grande
            if abs(cot.mm_change) > 20000:
                alerts.append({
                    "type": "cot_large_change",
                    "metal": metal,
                    "change": cot.mm_change,
                    "direction": "long" if cot.mm_change > 0 else "short",
                })
        
        return alerts
    
    def check_etf_flow_alerts(self) -> List[Dict]:
        """
        Verifica alertas de fluxo de ETF.
        
        Returns:
            Lista de alertas
        """
        alerts = []
        
        for etf, data in self.etf_data.items():
            if abs(data.flow_value_usd) >= ETF_FLOW_THRESHOLD_USD:
                alerts.append({
                    "type": "etf_large_flow",
                    "etf": etf,
                    "metal": data.metal,
                    "flow_usd": data.flow_value_usd,
                    "direction": "inflow" if data.flow_value_usd > 0 else "outflow",
                })
        
        return alerts


# Singleton
_collector: Optional[InstitutionalCollector] = None


def get_institutional_collector() -> InstitutionalCollector:
    """Retorna instância singleton do coletor institucional."""
    global _collector
    if _collector is None:
        _collector = InstitutionalCollector()
    return _collector
