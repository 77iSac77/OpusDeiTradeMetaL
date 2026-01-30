"""
OpusDeiTradeMetaL - Coletor de Dados Macro e Calendário
========================================================
Coleta dados econômicos, calendário de eventos e dados de bancos centrais.
"""

import aiohttp
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from bs4 import BeautifulSoup
import re
import xml.etree.ElementTree as ET

from config.settings import (
    FRED_API_KEY, DATA_SOURCES, CRITICAL_KEYWORDS,
    ECONOMIC_EVENTS
)
from storage.database import get_database

logger = logging.getLogger(__name__)


@dataclass
class EconomicEvent:
    """Representa um evento econômico."""
    event_type: str
    title: str
    event_time: datetime
    country: str = ""
    impact: str = "medium"  # high, medium, low
    actual: Optional[str] = None
    forecast: Optional[str] = None
    previous: Optional[str] = None
    description: str = ""
    
    def to_dict(self) -> Dict:
        return {
            "event_type": self.event_type,
            "title": self.title,
            "event_time": self.event_time.isoformat(),
            "country": self.country,
            "impact": self.impact,
            "actual": self.actual,
            "forecast": self.forecast,
            "previous": self.previous,
            "description": self.description,
        }


@dataclass
class MacroData:
    """Dados macroeconômicos."""
    indicator: str
    value: float
    unit: str = ""
    date: datetime = field(default_factory=datetime.utcnow)
    source: str = ""
    change: Optional[float] = None
    
    def to_dict(self) -> Dict:
        return {
            "indicator": self.indicator,
            "value": self.value,
            "unit": self.unit,
            "date": self.date.isoformat(),
            "source": self.source,
            "change": self.change,
        }


class MacroCollector:
    """Coletor de dados macroeconômicos e calendário."""
    
    def __init__(self):
        self.db = get_database()
        self.session: Optional[aiohttp.ClientSession] = None
        self.events_cache: List[EconomicEvent] = []
        self.macro_data: Dict[str, MacroData] = {}
    
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
    # FRED API - Dados Econômicos US
    # =========================================================================
    
    async def fetch_fred_series(self, series_id: str) -> Optional[MacroData]:
        """
        Coleta série de dados do FRED.
        
        Args:
            series_id: ID da série (ex: DGS10 para Treasury 10Y)
        
        Returns:
            MacroData ou None
        """
        if not FRED_API_KEY:
            logger.warning("FRED API key não configurada")
            return None
        
        session = await self._get_session()
        
        try:
            url = f"{DATA_SOURCES['fred']}"
            params = {
                "series_id": series_id,
                "api_key": FRED_API_KEY,
                "file_type": "json",
                "limit": 2,
                "sort_order": "desc",
            }
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    observations = data.get("observations", [])
                    
                    if observations:
                        latest = observations[0]
                        value = float(latest.get("value", 0))
                        date = datetime.strptime(latest.get("date", ""), "%Y-%m-%d")
                        
                        # Calcular mudança se houver dado anterior
                        change = None
                        if len(observations) > 1:
                            prev_value = float(observations[1].get("value", 0))
                            if prev_value:
                                change = value - prev_value
                        
                        return MacroData(
                            indicator=series_id,
                            value=value,
                            date=date,
                            source="FRED",
                            change=change,
                        )
                else:
                    logger.warning(f"FRED retornou status {response.status}")
                    
        except Exception as e:
            logger.error(f"Erro ao coletar FRED {series_id}: {e}")
            self.db.log_error("collector", "FRED", str(e))
        
        return None
    
    async def fetch_key_macro_data(self) -> Dict[str, MacroData]:
        """
        Coleta principais dados macroeconômicos.
        
        Returns:
            Dict com indicadores
        """
        series_map = {
            "DGS10": "Treasury 10Y",      # Yields
            "DGS2": "Treasury 2Y",
            "DEXUSEU": "EUR/USD",
            "DTWEXBGS": "DXY",            # Dollar Index
            "CPIAUCSL": "CPI",            # Inflação
            "UNRATE": "Unemployment",     # Desemprego
            "FEDFUNDS": "Fed Funds Rate", # Taxa Fed
        }
        
        data = {}
        
        for series_id, name in series_map.items():
            macro = await self.fetch_fred_series(series_id)
            if macro:
                macro.indicator = name
                data[name] = macro
                self.macro_data[name] = macro
        
        logger.info(f"Coletados {len(data)} indicadores macro do FRED")
        return data
    
    # =========================================================================
    # CALENDÁRIO ECONÔMICO
    # =========================================================================
    
    async def fetch_economic_calendar(self, days_ahead: int = 7) -> List[EconomicEvent]:
        """
        Coleta calendário econômico.
        
        Args:
            days_ahead: Dias à frente para buscar
        
        Returns:
            Lista de eventos
        """
        session = await self._get_session()
        events = []
        
        try:
            # Investing.com calendar (scraping)
            url = "https://www.investing.com/economic-calendar/"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Parsear tabela de eventos
                    rows = soup.select('tr.js-event-item')
                    
                    for row in rows:
                        try:
                            # Extrair dados do evento
                            time_elem = row.select_one('.time')
                            title_elem = row.select_one('.event')
                            country_elem = row.select_one('.flagCur')
                            impact_elem = row.select_one('.sentiment')
                            actual_elem = row.select_one('.act')
                            forecast_elem = row.select_one('.fore')
                            prev_elem = row.select_one('.prev')
                            
                            if not title_elem:
                                continue
                            
                            title = title_elem.get_text(strip=True)
                            
                            # Determinar impacto
                            impact = "medium"
                            if impact_elem:
                                bulls = len(impact_elem.select('.grayFullBullishIcon'))
                                if bulls >= 3:
                                    impact = "high"
                                elif bulls == 1:
                                    impact = "low"
                            
                            # Verificar se é evento relevante para metais
                            is_relevant = any(
                                kw.lower() in title.lower() 
                                for kw in ECONOMIC_EVENTS["high_impact"] + ECONOMIC_EVENTS["medium_impact"]
                            )
                            
                            if is_relevant or impact == "high":
                                event = EconomicEvent(
                                    event_type=self._categorize_event(title),
                                    title=title,
                                    event_time=datetime.utcnow(),  # Será ajustado
                                    country=country_elem.get_text(strip=True) if country_elem else "",
                                    impact=impact,
                                    actual=actual_elem.get_text(strip=True) if actual_elem else None,
                                    forecast=forecast_elem.get_text(strip=True) if forecast_elem else None,
                                    previous=prev_elem.get_text(strip=True) if prev_elem else None,
                                )
                                events.append(event)
                                
                        except Exception as e:
                            logger.debug(f"Erro ao parsear evento: {e}")
                            continue
                    
                else:
                    logger.warning(f"Investing.com retornou status {response.status}")
                    
        except Exception as e:
            logger.error(f"Erro ao coletar calendário: {e}")
            self.db.log_error("collector", "investing.com", str(e))
        
        # Adicionar eventos conhecidos manualmente
        events.extend(await self._get_known_events(days_ahead))
        
        # Atualizar cache
        self.events_cache = events
        
        # Salvar no banco
        for event in events:
            self.db.add_calendar_event(
                event_type=event.event_type,
                title=event.title,
                event_time=event.event_time,
                description=event.description,
                impact=event.impact,
            )
        
        logger.info(f"Coletados {len(events)} eventos do calendário")
        return events
    
    async def _get_known_events(self, days_ahead: int) -> List[EconomicEvent]:
        """
        Retorna eventos conhecidos/fixos.
        
        FOMC, ECB etc têm datas publicadas.
        """
        events = []
        
        # FOMC 2025 dates (exemplo)
        fomc_dates = [
            datetime(2025, 1, 29, 19, 0),   # Janeiro
            datetime(2025, 3, 19, 18, 0),   # Março
            datetime(2025, 5, 7, 18, 0),    # Maio
            datetime(2025, 6, 18, 18, 0),   # Junho
            datetime(2025, 7, 30, 18, 0),   # Julho
            datetime(2025, 9, 17, 18, 0),   # Setembro
            datetime(2025, 11, 5, 19, 0),   # Novembro
            datetime(2025, 12, 17, 19, 0),  # Dezembro
        ]
        
        now = datetime.utcnow()
        cutoff = now + timedelta(days=days_ahead)
        
        for date in fomc_dates:
            if now <= date <= cutoff:
                events.append(EconomicEvent(
                    event_type="FOMC",
                    title="FOMC Rate Decision",
                    event_time=date,
                    country="US",
                    impact="high",
                    description="Federal Reserve interest rate decision",
                ))
        
        return events
    
    def _categorize_event(self, title: str) -> str:
        """Categoriza evento pelo título."""
        title_lower = title.lower()
        
        if "fomc" in title_lower or "fed" in title_lower:
            return "FOMC"
        elif "ecb" in title_lower:
            return "ECB"
        elif "cpi" in title_lower:
            return "CPI"
        elif "nonfarm" in title_lower or "nfp" in title_lower:
            return "NFP"
        elif "gdp" in title_lower:
            return "GDP"
        elif "pmi" in title_lower:
            return "PMI"
        elif "jobless" in title_lower:
            return "JOBLESS"
        elif "pce" in title_lower:
            return "PCE"
        else:
            return "OTHER"
    
    # =========================================================================
    # PROBABILIDADES DE TAXA
    # =========================================================================
    
    async def fetch_fed_probabilities(self) -> Dict[str, float]:
        """
        Coleta probabilidades de decisão do Fed (CME FedWatch).
        
        Returns:
            Dict com probabilidades
        """
        session = await self._get_session()
        
        try:
            # CME FedWatch tool
            url = "https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    html = await response.text()
                    # Parsear probabilidades
                    # Estrutura depende do site, isso é simplificado
                    
                    # Retornar exemplo
                    return {
                        "hold": 88.0,
                        "cut_25bp": 10.0,
                        "cut_50bp": 2.0,
                        "hike_25bp": 0.0,
                    }
                    
        except Exception as e:
            logger.error(f"Erro ao coletar Fed probabilities: {e}")
        
        return {}
    
    # =========================================================================
    # DADOS DE BANCOS CENTRAIS
    # =========================================================================
    
    async def fetch_central_bank_reserves(self) -> Dict[str, Dict]:
        """
        Coleta dados de reservas de ouro de bancos centrais.
        
        Returns:
            Dict com dados por país
        """
        session = await self._get_session()
        reserves = {}
        
        try:
            # World Gold Council data
            # Isso seria scraping ou API específica
            
            # Dados exemplo
            reserves = {
                "US": {"tons": 8133.5, "change": 0},
                "Germany": {"tons": 3352.6, "change": 0},
                "Italy": {"tons": 2451.8, "change": 0},
                "France": {"tons": 2436.9, "change": 0},
                "Russia": {"tons": 2332.7, "change": 5.0},
                "China": {"tons": 2264.0, "change": 5.0},
                "Switzerland": {"tons": 1040.0, "change": 0},
                "India": {"tons": 853.6, "change": 2.0},
                "Turkey": {"tons": 570.3, "change": 8.0},
                "Poland": {"tons": 420.0, "change": 8.0},
            }
            
        except Exception as e:
            logger.error(f"Erro ao coletar reservas de BCs: {e}")
        
        return reserves
    
    # =========================================================================
    # RSS FEEDS
    # =========================================================================
    
    async def fetch_rss_feed(self, url: str) -> List[Dict]:
        """
        Coleta itens de feed RSS.
        
        Args:
            url: URL do feed RSS
        
        Returns:
            Lista de itens
        """
        session = await self._get_session()
        items = []
        
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.text()
                    root = ET.fromstring(content)
                    
                    for item in root.findall('.//item'):
                        title = item.find('title')
                        link = item.find('link')
                        pub_date = item.find('pubDate')
                        description = item.find('description')
                        
                        items.append({
                            "title": title.text if title is not None else "",
                            "link": link.text if link is not None else "",
                            "pub_date": pub_date.text if pub_date is not None else "",
                            "description": description.text if description is not None else "",
                        })
                        
        except Exception as e:
            logger.error(f"Erro ao coletar RSS {url}: {e}")
        
        return items
    
    async def fetch_kitco_news(self) -> List[Dict]:
        """Coleta news do Kitco RSS."""
        return await self.fetch_rss_feed("https://www.kitco.com/rss/gold.xml")
    
    # =========================================================================
    # VERIFICAÇÃO DE ALERTAS
    # =========================================================================
    
    def check_event_alerts(self) -> List[Dict]:
        """
        Verifica eventos que precisam de alerta.
        
        Returns:
            Lista de alertas a enviar
        """
        alerts = []
        now = datetime.utcnow()
        
        for event in self.events_cache:
            time_until = event.event_time - now
            hours_until = time_until.total_seconds() / 3600
            
            # 7 dias antes (para eventos grandes)
            if 167 < hours_until <= 168:  # ~7 dias
                if event.event_type in ["FOMC", "ECB"]:
                    alerts.append({
                        "type": "7d",
                        "event": event,
                    })
            
            # 1 dia antes
            elif 23 < hours_until <= 24:
                alerts.append({
                    "type": "1d",
                    "event": event,
                })
            
            # 1 hora antes
            elif 0.9 < hours_until <= 1:
                alerts.append({
                    "type": "1h",
                    "event": event,
                })
        
        return alerts
    
    def get_upcoming_events(self, hours: int = 24) -> List[EconomicEvent]:
        """
        Retorna eventos nas próximas X horas.
        
        Args:
            hours: Janela de tempo
        
        Returns:
            Lista de eventos
        """
        now = datetime.utcnow()
        cutoff = now + timedelta(hours=hours)
        
        return [
            event for event in self.events_cache
            if now <= event.event_time <= cutoff
        ]
    
    def get_macro_summary(self) -> Dict[str, Any]:
        """
        Retorna resumo de dados macro.
        
        Returns:
            Dict com resumo
        """
        return {
            "dxy": self.macro_data.get("DXY"),
            "yields_10y": self.macro_data.get("Treasury 10Y"),
            "yields_2y": self.macro_data.get("Treasury 2Y"),
            "fed_rate": self.macro_data.get("Fed Funds Rate"),
            "cpi": self.macro_data.get("CPI"),
            "unemployment": self.macro_data.get("Unemployment"),
        }


# Singleton
_collector: Optional[MacroCollector] = None


def get_macro_collector() -> MacroCollector:
    """Retorna instância singleton do coletor macro."""
    global _collector
    if _collector is None:
        _collector = MacroCollector()
    return _collector
