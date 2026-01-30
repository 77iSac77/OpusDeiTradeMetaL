"""
OpusDeiTradeMetaL - Utilidades de Tempo e Formatação
=====================================================
Gerencia fusos horários, horário de verão e formatação de timestamps.
"""

from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
import pytz

# =============================================================================
# FUSOS HORÁRIOS
# =============================================================================

TIMEZONE_INFO = {
    "UTC": {"offset": 0, "emoji": "🕐", "name": "UTC"},
    "BR": {"offset": -3, "emoji": "🇧🇷", "name": "Brasil", "tz": "America/Sao_Paulo"},
    "ES": {"offset": 1, "emoji": "🇪🇸", "name": "Espanha", "tz": "Europe/Madrid"},
    "US": {"offset": -5, "emoji": "🇺🇸", "name": "Nova York", "tz": "America/New_York"},
    "CN": {"offset": 8, "emoji": "🇨🇳", "name": "Shanghai", "tz": "Asia/Shanghai"},
}


def get_timezone_offset(tz_code: str, dt: Optional[datetime] = None) -> int:
    """
    Retorna o offset UTC atual para um fuso horário, considerando horário de verão.
    
    Args:
        tz_code: Código do fuso (BR, ES, US, CN, UTC)
        dt: Data para verificar (default: agora)
    
    Returns:
        Offset em horas
    """
    if dt is None:
        dt = datetime.utcnow()
    
    tz_info = TIMEZONE_INFO.get(tz_code.upper())
    if not tz_info:
        return 0
    
    # Se tem timezone pytz, usa para calcular automaticamente
    if "tz" in tz_info:
        try:
            tz = pytz.timezone(tz_info["tz"])
            localized = tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)
            return int(localized.utcoffset().total_seconds() / 3600)
        except Exception:
            pass
    
    return tz_info["offset"]


def format_time_for_timezone(utc_dt: datetime, tz_code: str) -> str:
    """
    Formata datetime UTC para um fuso horário específico.
    
    Args:
        utc_dt: Datetime em UTC
        tz_code: Código do fuso
    
    Returns:
        String formatada HH:MM
    """
    offset = get_timezone_offset(tz_code, utc_dt)
    local_dt = utc_dt + timedelta(hours=offset)
    return local_dt.strftime("%H:%M")


def format_timestamp_all_zones(utc_dt: Optional[datetime] = None) -> str:
    """
    Formata timestamp para todos os fusos configurados.
    
    Formato:
    ⏱ 14:32 UTC | 🇧🇷 11:32 | 🇪🇸 15:32
                 | 🇺🇸 09:32 | 🇨🇳 22:32
    
    Args:
        utc_dt: Datetime em UTC (default: agora)
    
    Returns:
        String formatada com duas linhas
    """
    if utc_dt is None:
        utc_dt = datetime.utcnow()
    
    utc_str = utc_dt.strftime("%H:%M")
    br_str = format_time_for_timezone(utc_dt, "BR")
    es_str = format_time_for_timezone(utc_dt, "ES")
    us_str = format_time_for_timezone(utc_dt, "US")
    cn_str = format_time_for_timezone(utc_dt, "CN")
    
    line1 = f"⏱ {utc_str} UTC | 🇧🇷 {br_str} | 🇪🇸 {es_str}"
    line2 = f"             | 🇺🇸 {us_str} | 🇨🇳 {cn_str}"
    
    return f"{line1}\n{line2}"


def format_date_br(dt: datetime) -> str:
    """Formata data no padrão brasileiro: DD/MM/YYYY"""
    return dt.strftime("%d/%m/%Y")


def format_datetime_br(dt: datetime) -> str:
    """Formata data e hora no padrão brasileiro: DD/MM/YYYY HH:MM"""
    return dt.strftime("%d/%m/%Y %H:%M")


def get_market_status(market: str) -> Dict[str, any]:
    """
    Verifica se um mercado está aberto.
    
    Args:
        market: Nome do mercado (SGE, SHFE, MCX, LBMA, COMEX)
    
    Returns:
        Dict com status, próxima abertura/fechamento
    """
    from config.settings import MARKET_HOURS
    
    now_utc = datetime.utcnow()
    hours = MARKET_HOURS.get(market.upper())
    
    if not hours:
        return {"open": None, "status": "unknown"}
    
    open_time = datetime.strptime(hours["open"], "%H:%M").time()
    close_time = datetime.strptime(hours["close"], "%H:%M").time()
    current_time = now_utc.time()
    
    # Verificar se está entre abertura e fechamento
    if open_time <= current_time <= close_time:
        return {
            "open": True,
            "status": "aberto",
            "closes_at": hours["close"],
        }
    else:
        return {
            "open": False,
            "status": "fechado",
            "opens_at": hours["open"],
        }


def time_until_event(event_dt: datetime) -> str:
    """
    Calcula tempo até um evento e formata em linguagem natural.
    
    Args:
        event_dt: Datetime do evento (UTC)
    
    Returns:
        String como "em 2 horas", "amanhã", "em 3 dias"
    """
    now = datetime.utcnow()
    delta = event_dt - now
    
    if delta.total_seconds() < 0:
        return "já passou"
    
    days = delta.days
    hours = delta.seconds // 3600
    minutes = (delta.seconds % 3600) // 60
    
    if days > 7:
        return f"em {days} dias"
    elif days > 1:
        return f"em {days} dias"
    elif days == 1:
        return "amanhã"
    elif hours > 1:
        return f"em {hours} horas"
    elif hours == 1:
        return f"em 1 hora"
    elif minutes > 1:
        return f"em {minutes} minutos"
    else:
        return "agora"


def is_dst_active(tz_code: str, dt: Optional[datetime] = None) -> bool:
    """
    Verifica se horário de verão está ativo para um fuso.
    
    Args:
        tz_code: Código do fuso
        dt: Data para verificar (default: agora)
    
    Returns:
        True se DST ativo
    """
    if dt is None:
        dt = datetime.utcnow()
    
    tz_info = TIMEZONE_INFO.get(tz_code.upper())
    if not tz_info or "tz" not in tz_info:
        return False
    
    try:
        tz = pytz.timezone(tz_info["tz"])
        localized = tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)
        return bool(localized.dst())
    except Exception:
        return False


def get_next_digest_time(digest_type: str, user_tz_offset: int = -3) -> datetime:
    """
    Calcula próximo horário de digest.
    
    Args:
        digest_type: "asia", "eu_us", ou "weekly"
        user_tz_offset: Offset do usuário em horas
    
    Returns:
        Datetime UTC do próximo digest
    """
    now = datetime.utcnow()
    
    if digest_type == "asia":
        # 07:30 UTC (após fechamento Shanghai)
        target = now.replace(hour=7, minute=30, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        return target
    
    elif digest_type == "eu_us":
        # 21:30 UTC (após fechamento COMEX)
        target = now.replace(hour=21, minute=30, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        return target
    
    elif digest_type == "weekly":
        # Sábado à noite no horário do usuário
        # Calcular próximo sábado
        days_until_saturday = (5 - now.weekday()) % 7
        if days_until_saturday == 0 and now.hour >= 20:
            days_until_saturday = 7
        
        saturday = now + timedelta(days=days_until_saturday)
        # 20:00 no horário do usuário = 20:00 - offset em UTC
        target_hour_utc = 20 - user_tz_offset
        target = saturday.replace(hour=target_hour_utc % 24, minute=0, second=0, microsecond=0)
        return target
    
    return now


# =============================================================================
# FORMATAÇÃO DE NÚMEROS
# =============================================================================

def format_price(value: float, decimals: int = 2) -> str:
    """
    Formata preço com separador de milhares e decimais.
    
    Args:
        value: Valor numérico
        decimals: Casas decimais
    
    Returns:
        String formatada: "$2.347,50"
    """
    formatted = f"{value:,.{decimals}f}"
    # Trocar para padrão brasileiro (. milhares, , decimais)
    formatted = formatted.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"${formatted}"


def format_percent(value: float, include_sign: bool = True) -> str:
    """
    Formata percentual.
    
    Args:
        value: Valor em percentual (1.5 = 1.5%)
        include_sign: Incluir + para positivos
    
    Returns:
        String formatada: "+1,5%" ou "-1,5%"
    """
    sign = "+" if value > 0 and include_sign else ""
    formatted = f"{value:.1f}".replace(".", ",")
    return f"{sign}{formatted}%"


def format_large_number(value: float) -> str:
    """
    Formata números grandes com sufixos.
    
    Args:
        value: Valor numérico
    
    Returns:
        String formatada: "$45M", "$1.2B", "$500K"
    """
    abs_value = abs(value)
    sign = "-" if value < 0 else ""
    
    if abs_value >= 1_000_000_000:
        return f"{sign}${abs_value / 1_000_000_000:.1f}B"
    elif abs_value >= 1_000_000:
        return f"{sign}${abs_value / 1_000_000:.1f}M"
    elif abs_value >= 1_000:
        return f"{sign}${abs_value / 1_000:.1f}K"
    else:
        return f"{sign}${abs_value:.0f}"


def format_volume(value: float, unit: str = "ton") -> str:
    """
    Formata volume com unidade.
    
    Args:
        value: Valor
        unit: Unidade (ton, oz, contratos)
    
    Returns:
        String formatada
    """
    if value >= 1000:
        return f"{value / 1000:.1f}k {unit}"
    return f"{value:.1f} {unit}"


def format_change_emoji(value: float) -> str:
    """
    Retorna emoji baseado na direção da mudança.
    
    Args:
        value: Valor da mudança
    
    Returns:
        Emoji: ⬆️, ⬇️, ou ➡️
    """
    if value > 0:
        return "⬆️"
    elif value < 0:
        return "⬇️"
    else:
        return "➡️"
