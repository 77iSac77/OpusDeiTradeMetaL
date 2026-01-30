"""
OpusDeiTradeMetaL - Configurações Globais
==========================================
Sistema de alertas em tempo real para metais preciosos e industriais.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from enum import Enum

# =============================================================================
# AMBIENTE
# =============================================================================

# Keys (via variáveis de ambiente - NUNCA hardcode)
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
FRED_API_KEY = os.getenv("FRED_API_KEY", "")

# Telegram
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# =============================================================================
# METAIS MONITORADOS
# =============================================================================

@dataclass
class Metal:
    """Configuração de um metal."""
    ticker: str
    nome: str
    tipo: str  # precioso, industrial, estrategico
    emoji: str
    tokens_onchain: List[str] = field(default_factory=list)
    etfs: List[str] = field(default_factory=list)

METAIS: Dict[str, Metal] = {
    # Preciosos
    "XAU": Metal("XAU", "Ouro", "precioso", "🥇", ["PAXG", "XAUT"], ["GLD", "IAU"]),
    "XAG": Metal("XAG", "Prata", "precioso", "🥈", ["SLVR"], ["SLV"]),
    "XPT": Metal("XPT", "Platina", "precioso", "🔘", [], ["PPLT"]),
    "XPD": Metal("XPD", "Paládio", "precioso", "🔷", [], ["PALL"]),
    
    # Industriais
    "XCU": Metal("XCU", "Cobre", "industrial", "🔶", [], ["CPER"]),
    "XAL": Metal("XAL", "Alumínio", "industrial", "⬜", [], []),
    "XNI": Metal("XNI", "Níquel", "industrial", "🔵", [], []),
    "XPB": Metal("XPB", "Chumbo", "industrial", "⚫", [], []),
    "XZN": Metal("XZN", "Zinco", "industrial", "🔹", [], []),
    "XSN": Metal("XSN", "Estanho", "industrial", "🟤", [], []),
    
    # Estratégicos
    "UX": Metal("UX", "Urânio", "estrategico", "☢️", [], ["URA"]),
    "FE": Metal("FE", "Minério de Ferro", "estrategico", "�ite", [], []),
}

def formato_metal(ticker: str) -> str:
    """Retorna formato padrão: 'XAU Ouro'"""
    metal = METAIS.get(ticker.upper())
    if metal:
        return f"{metal.ticker} {metal.nome}"
    return ticker

# =============================================================================
# NÍVEIS DE ALERTA
# =============================================================================

class AlertLevel(Enum):
    CRITICO = "🔴"
    IMPORTANTE = "🟡"
    INFO = "🟢"

# Thresholds de movimento de preço
ALERT_THRESHOLDS = {
    AlertLevel.CRITICO: {
        "timeframe_minutes": 15,
        "percent_change": 2.0,
    },
    AlertLevel.IMPORTANTE: {
        "timeframe_minutes": 60,
        "percent_change": 1.0,
    },
    AlertLevel.INFO: {
        "timeframe_minutes": 1440,  # 24h
        "percent_change": 0.5,
    },
}

# Outros thresholds
LIQUIDATION_THRESHOLD_USD = 10_000_000  # $10M
WHALE_ALERT_THRESHOLD_USD = 1_000_000   # $1M
ETF_FLOW_THRESHOLD_USD = 50_000_000     # $50M

# Aproximação de níveis técnicos
TECHNICAL_PROXIMITY_PERCENT = 0.3  # Alertar quando 0.3% de distância

# =============================================================================
# LLMs - POOL OPENROUTER (GRÁTIS)
# =============================================================================

@dataclass
class LLMModel:
    """Configuração de modelo LLM."""
    name: str
    model_id: str
    context_length: int
    best_for: str
    priority: int  # 1 = principal, maior = backup

LLM_POOL: List[LLMModel] = [
    LLMModel(
        name="Gemini 2.0 Flash",
        model_id="google/gemini-2.0-flash-exp:free",
        context_length=128_000,
        best_for="geral",
        priority=1
    ),
    LLMModel(
        name="Nemotron 3 Nano",
        model_id="nvidia/nemotron-3-nano-30b-a3b:free",
        context_length=256_000,
        best_for="analise",
        priority=2
    ),
    LLMModel(
        name="DeepSeek R1 Distill",
        model_id="deepseek/deepseek-r1-distill-qwen-14b:free",
        context_length=64_000,
        best_for="raciocinio",
        priority=3
    ),
    LLMModel(
        name="Gemini 2.5 Flash",
        model_id="google/gemini-2.5-flash-preview:free",
        context_length=128_000,
        best_for="backup",
        priority=4
    ),
    LLMModel(
        name="LFM2.5 Thinking",
        model_id="liquid/lfm-2.5-1.2b-thinking:free",
        context_length=32_000,
        best_for="ultimo_recurso",
        priority=5
    ),
]

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1/chat/completions"

# =============================================================================
# TIMEZONES
# =============================================================================

TIMEZONES = {
    "UTC": 0,
    "BR": -3,      # Brasil (BRT)
    "ES": 1,       # Espanha (CET) - verão: 2
    "US": -5,      # Nova York (EST) - verão: -4
    "CN": 8,       # Shanghai (CST)
}

# Horários de verão (mês início, mês fim)
DAYLIGHT_SAVING = {
    "ES": (3, 10),   # Março a Outubro
    "US": (3, 11),   # Março a Novembro
}

# =============================================================================
# HORÁRIOS DE MERCADO (UTC)
# =============================================================================

MARKET_HOURS = {
    "SGE": {"open": "01:30", "close": "07:00"},      # Shanghai Gold Exchange
    "SHFE": {"open": "01:00", "close": "07:00"},     # Shanghai Futures
    "MCX": {"open": "03:45", "close": "11:30"},      # India
    "LBMA": {"open": "10:30", "close": "16:30"},     # London
    "COMEX": {"open": "13:00", "close": "21:00"},    # New York
}

# Horários dos digests (UTC)
DIGEST_TIMES = {
    "asia": "07:30",    # Após fechamento Shanghai
    "eu_us": "21:30",   # Após fechamento COMEX
    # semanal: sábado noite horário do usuário
}

# =============================================================================
# FONTES DE DADOS
# =============================================================================

DATA_SOURCES = {
    # Preços
    "metals_live": "https://metals.live/api",
    "kitco": "https://www.kitco.com/",
    
    # Macro US
    "fred": "https://api.stlouisfed.org/fred/series/observations",
    "investing": "https://www.investing.com/economic-calendar/",
    
    # Institucional
    "cftc_cot": "https://www.cftc.gov/dea/newcot/",
    "sec_edgar": "https://www.sec.gov/cgi-bin/browse-edgar",
    
    # ETFs
    "yahoo_finance": "https://query1.finance.yahoo.com/v8/finance/chart/",
    
    # On-chain
    "etherscan": "https://api.etherscan.io/api",
    
    # China
    "sge": "https://www.sge.com.cn/",
    "shfe": "https://www.shfe.com.cn/",
    
    # India
    "mcx": "https://www.mcxindia.com/",
    
    # Suíça
    "swiss_customs": "https://www.ezv.admin.ch/",
    
    # Austrália
    "perth_mint": "https://www.perthmint.com/",
    
    # COMEX
    "cme": "https://www.cmegroup.com/",
}

# =============================================================================
# KEYWORDS CRÍTICAS PARA NEWS
# =============================================================================

CRITICAL_KEYWORDS = [
    # Bancos Centrais
    "fed", "fomc", "ecb", "boj", "pboc", "central bank",
    "rate cut", "rate hike", "interest rate", "monetary policy",
    
    # Geopolítica
    "sanctions", "tariffs", "trade war", "brics", "de-dollarization",
    "default", "crisis", "recession", "war", "conflict",
    
    # Supply
    "mine shutdown", "mine closure", "strike", "supply disruption",
    "production cut", "output", "shortage",
    
    # Demanda
    "reserve", "gold reserve", "buying", "accumulating",
    "etf inflow", "etf outflow",
    
    # Específicos
    "comex", "lbma", "shanghai premium", "backwardation", "contango",
    "nornickel", "eskom", "load shedding",
]

# =============================================================================
# NÍVEIS TÉCNICOS
# =============================================================================

TECHNICAL_LEVELS = {
    # Longo prazo
    "long_term": [
        "max_52w",      # Máxima 52 semanas
        "min_52w",      # Mínima 52 semanas
        "sma_50",       # Média móvel 50 dias
        "sma_200",      # Média móvel 200 dias
    ],
    # Curto prazo / Daytrade
    "short_term": [
        "pivot_pp",     # Pivot Point
        "pivot_r1", "pivot_r2", "pivot_r3",  # Resistências
        "pivot_s1", "pivot_s2", "pivot_s3",  # Suportes
        "vwap",         # Volume Weighted Average Price
        "high_volume_zones",  # Zonas de alto volume 48h
        "multiple_touches",   # 2+ toques em 5 dias
    ],
}

# =============================================================================
# CALENDÁRIO ECONÔMICO
# =============================================================================

ECONOMIC_EVENTS = {
    "high_impact": [
        "FOMC", "ECB Rate Decision", "BoJ Rate Decision",
        "CPI", "NFP", "PCE", "GDP",
    ],
    "medium_impact": [
        "Fed Minutes", "PMI", "Jobless Claims",
        "COT Report", "LBMA Gold Price",
    ],
    "alerts_before": [
        {"days": 7, "events": ["FOMC", "ECB Rate Decision"]},
        {"days": 1, "events": "all"},
        {"hours": 1, "events": "all"},
    ],
}

# =============================================================================
# CONFIGURAÇÕES DO BOT
# =============================================================================

BOT_CONFIG = {
    "name": "OpusDeiTradeMetaL",
    "username": "OpusDeiTradeMetaL_bot",
    "language": "pt",
    "default_timezone_offset": -3,  # Brasil
    
    # Keep-alive
    "ping_interval_seconds": 240,  # 4 minutos
    
    # Cache LLM
    "cache_ttl_seconds": 3600,  # 1 hora
    
    # Rate limits
    "max_alerts_per_hour": 50,
    "max_llm_calls_per_day": 1000,
    
    # Retry
    "retry_attempts": 3,
    "retry_delay_seconds": [30, 60, 120],
    
    # Database
    "db_path": "data/opusdei.db",
}

# =============================================================================
# MENSAGENS PADRÃO
# =============================================================================

MESSAGES = {
    "start": """
🤖 **OpusDeiTradeMetaL** ativo!

Monitorando 12 metais em 9 mercados globais.

Use /comandos para ver opções disponíveis.
""",
    
    "system_offline": """
⚠️ SISTEMA | Fonte offline

{fonte} não responde há {tempo}.
{acao}

Última cotação válida:
{ultima_cotacao}
""",
    
    "llm_limit": """
⚠️ SISTEMA | Limite LLM

Requests LLM esgotados por hoje.

Alertas continuam funcionando ✓
Análises enriquecidas pausadas ✗

Volta em: ~{horas} horas
""",
}
