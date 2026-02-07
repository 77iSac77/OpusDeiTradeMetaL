"""
OpusDeiTradeMetaL - Processador de Alertas
===========================================
Processa e decide quais alertas enviar.
"""

import asyncio
import hashlib
import logging
from datetime import datetime, timedelta
from utils.time_utils import utcnow
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from enum import Enum

from config.settings import (
    METAIS, AlertLevel, ALERT_THRESHOLDS,
    LIQUIDATION_THRESHOLD_USD, WHALE_ALERT_THRESHOLD_USD,
    TECHNICAL_PROXIMITY_PERCENT
)
from storage.database import get_database
from utils.llm_client import get_llm_client
from bot.formatter import MessageFormatter

logger = logging.getLogger(__name__)


@dataclass
class Alert:
    """Representa um alerta a ser enviado."""
    level: AlertLevel
    alert_type: str
    metal: Optional[str]
    message: str
    content_hash: str
    priority: int = 0  # Maior = mais prioritário
    requires_llm: bool = False
    context: Dict = None
    
    def __post_init__(self):
        if self.context is None:
            self.context = {}


class AlertProcessor:
    """Processador central de alertas."""
    
    def __init__(self, send_callback: Callable):
        """
        Inicializa processador.
        
        Args:
            send_callback: Função async para enviar mensagem ao Telegram
        """
        self.db = get_database()
        self.llm = get_llm_client()
        self.formatter = MessageFormatter()
        self.send_callback = send_callback
        
        # Fila de alertas
        self.alert_queue: List[Alert] = []
        
        # Cache de preços anteriores para comparação
        self.previous_prices: Dict[str, float] = {}
        
        # Controle de rate limit
        self.alerts_sent_this_hour = 0
        self.hour_start = utcnow()
        
        # Configurações do usuário
        self.user_config = self._load_user_config()
    
    def _load_user_config(self) -> Dict:
        """Carrega configurações do usuário."""
        return {
            "alertas_ativos": self.db.get_config("alertas_ativos", True),
            "filtros": self.db.get_config("filtros", []),  # Lista vazia = todos
            "silenciado_ate": self.db.get_config("silenciado_ate", None),
            "timezone": self.db.get_config("timezone", -3),
        }
    
    def _generate_hash(self, content: str) -> str:
        """Gera hash único para conteúdo de alerta."""
        # Incluir hora para permitir alertas similares em horários diferentes
        hour_key = utcnow().strftime("%Y%m%d%H")
        return hashlib.md5(f"{content}:{hour_key}".encode()).hexdigest()
    
    def _is_silenced(self) -> bool:
        """Verifica se alertas estão silenciados."""
        if not self.user_config.get("alertas_ativos", True):
            return True
        
        silenciado_ate = self.user_config.get("silenciado_ate")
        if silenciado_ate:
            try:
                until = datetime.fromisoformat(silenciado_ate)
                if utcnow() < until:
                    return True
            except (ValueError, TypeError) as e:
                logger.warning(f"Erro ao parsear silenciado_ate: {e}")
        
        return False
    
    def _should_filter_metal(self, metal: str) -> bool:
        """Verifica se metal deve ser filtrado."""
        filtros = self.user_config.get("filtros", [])
        if not filtros:  # Lista vazia = todos os metais
            return False
        return metal.upper() not in [f.upper() for f in filtros]
    
    def _check_rate_limit(self) -> bool:
        """Verifica rate limit de alertas."""
        now = utcnow()
        
        # Resetar contador a cada hora
        if (now - self.hour_start).total_seconds() >= 3600:
            self.alerts_sent_this_hour = 0
            self.hour_start = now
        
        # Limite de 50 alertas/hora
        return self.alerts_sent_this_hour < 50
    
    async def _enrich_with_llm(self, alert: Alert) -> Alert:
        """Enriquece alerta com análise do LLM."""
        if not alert.requires_llm:
            return alert
        
        # Verificar se tem quota de LLM
        llm_stats = self.llm.get_stats()
        if llm_stats.get("remaining", 0) <= 0:
            logger.warning("Sem quota LLM para enriquecer alerta")
            return alert
        
        # Só alertas críticos usam LLM garantido
        if alert.level != AlertLevel.CRITICO and llm_stats.get("remaining", 0) < 100:
            logger.debug("Preservando quota LLM para alertas críticos")
            return alert
        
        try:
            # Gerar análise baseada no tipo de alerta
            if alert.alert_type == "price":
                analysis = await self.llm.analyze_correlation(alert.context)
            elif alert.alert_type == "technical":
                analysis = await self.llm.analyze_technical_level(
                    alert.metal,
                    alert.context.get("current_price", 0),
                    alert.context.get("level_name", ""),
                    alert.context.get("level_value", 0),
                    alert.context.get("level_type", "suporte")
                )
            elif alert.alert_type == "event":
                analysis = await self.llm.analyze_impact(
                    alert.context.get("event_type", ""),
                    alert.context
                )
            else:
                analysis = None
            
            if analysis:
                alert.context["llm_analysis"] = analysis
                
        except Exception as e:
            logger.error(f"Erro ao enriquecer alerta com LLM: {e}")
        
        return alert
    
    async def process_price_change(self, metal: str, current_price: float,
                                    change_percent: float, change_value: float,
                                    timeframe_minutes: int, context: Dict = None) -> Optional[Alert]:
        """
        Processa mudança de preço e cria alerta se necessário.
        """
        # Determinar nível do alerta
        level = None
        for alert_level in AlertLevel:
            threshold = ALERT_THRESHOLDS[alert_level]
            if (timeframe_minutes <= threshold["timeframe_minutes"] and
                abs(change_percent) >= threshold["percent_change"]):
                level = alert_level
                break
        
        if not level:
            return None
        
        # Criar mensagem
        message = self.formatter.format_price_alert(
            level, metal, current_price, change_percent,
            change_value, timeframe_minutes, context
        )
        
        # Criar alerta
        alert = Alert(
            level=level,
            alert_type="price",
            metal=metal,
            message=message,
            content_hash=self._generate_hash(f"price:{metal}:{level.value}"),
            priority=3 if level == AlertLevel.CRITICO else (2 if level == AlertLevel.IMPORTANTE else 1),
            requires_llm=level == AlertLevel.CRITICO,
            context={
                "current_price": current_price,
                "change_percent": change_percent,
                **(context or {})
            }
        )
        
        return alert
    
    async def process_technical_proximity(self, metal: str, current_price: float,
                                           level_name: str, level_value: float,
                                           level_type: str, distance_percent: float,
                                           context: Dict = None) -> Optional[Alert]:
        """
        Processa aproximação de nível técnico.
        """
        if distance_percent > TECHNICAL_PROXIMITY_PERCENT:
            return None
        
        message = self.formatter.format_technical_proximity_alert(
            metal, current_price, level_name, level_value,
            level_type, distance_percent, context
        )
        
        alert = Alert(
            level=AlertLevel.IMPORTANTE,
            alert_type="technical",
            metal=metal,
            message=message,
            content_hash=self._generate_hash(f"tech_prox:{metal}:{level_name}"),
            priority=2,
            requires_llm=True,
            context={
                "current_price": current_price,
                "level_name": level_name,
                "level_value": level_value,
                "level_type": level_type,
                **(context or {})
            }
        )
        
        return alert
    
    async def process_technical_break(self, metal: str, current_price: float,
                                       previous_price: float, level_name: str,
                                       level_value: float, direction: str) -> Optional[Alert]:
        """
        Processa rompimento de nível técnico.
        """
        message = self.formatter.format_technical_break_alert(
            metal, current_price, level_name, level_value, direction
        )
        
        alert = Alert(
            level=AlertLevel.CRITICO,
            alert_type="technical_break",
            metal=metal,
            message=message,
            content_hash=self._generate_hash(f"tech_break:{metal}:{level_name}:{direction}"),
            priority=3,
            requires_llm=True,
            context={
                "current_price": current_price,
                "previous_price": previous_price,
                "level_name": level_name,
                "level_value": level_value,
                "direction": direction,
            }
        )
        
        return alert
    
    async def process_whale_movement(self, movement: Dict) -> Optional[Alert]:
        """
        Processa movimento whale on-chain.
        """
        if movement.get("value_usd", 0) < WHALE_ALERT_THRESHOLD_USD:
            return None
        
        message = self.formatter.format_whale_alert(movement)
        
        alert = Alert(
            level=AlertLevel.IMPORTANTE,
            alert_type="whale",
            metal="XAU",  # PAXG/XAUT são ouro
            message=message,
            content_hash=self._generate_hash(f"whale:{movement.get('tx_hash', '')}"),
            priority=2,
            requires_llm=False,
            context=movement
        )
        
        return alert
    
    async def process_cot_update(self, metal: str, cot_data: Dict) -> Optional[Alert]:
        """
        Processa atualização do COT Report.
        """
        # Verificar se há sinal relevante
        mm_net = cot_data.get("mm_net", 0)
        mm_change = cot_data.get("mm_change", 0)
        open_interest = cot_data.get("open_interest", 1)
        
        mm_pct = (mm_net / open_interest * 100) if open_interest else 0
        
        # Alertar se muito crowded ou mudança grande
        should_alert = (
            abs(mm_pct) > 30 or  # Muito crowded
            abs(mm_change) > 20000  # Mudança grande
        )
        
        if not should_alert:
            return None
        
        # Adicionar sinal
        if mm_pct > 30:
            cot_data["signal"] = "Managed Money muito long - possível crowded trade"
        elif mm_pct < -20:
            cot_data["signal"] = "Managed Money muito short - possível squeeze"
        elif mm_change > 20000:
            cot_data["signal"] = f"Grande aumento de posições long (+{mm_change:,})"
        elif mm_change < -20000:
            cot_data["signal"] = f"Grande redução de posições long ({mm_change:,})"
        
        message = self.formatter.format_cot_alert(metal, cot_data)
        
        alert = Alert(
            level=AlertLevel.INFO,
            alert_type="cot",
            metal=metal,
            message=message,
            content_hash=self._generate_hash(f"cot:{metal}:{cot_data.get('report_date', '')}"),
            priority=1,
            requires_llm=False,
            context=cot_data
        )
        
        return alert
    
    async def process_calendar_event(self, event: Dict, alert_type: str) -> Optional[Alert]:
        """
        Processa evento do calendário.
        """
        if alert_type == "7d":
            message = self.formatter.format_calendar_7d(event)
            level = AlertLevel.INFO
        elif alert_type == "1d":
            # Para 1 dia antes, enriquecer com análise de impacto
            impact_analysis = await self.llm.analyze_impact(
                event.get("event_type", ""),
                event
            )
            message = self.formatter.format_calendar_1d(event, impact_analysis)
            level = AlertLevel.IMPORTANTE
        elif alert_type == "1h":
            message = self.formatter.format_calendar_1h(event)
            level = AlertLevel.IMPORTANTE
        else:
            message = self.formatter.format_calendar_result(event)
            level = AlertLevel.CRITICO
        
        alert = Alert(
            level=level,
            alert_type="calendar",
            metal=None,
            message=message,
            content_hash=self._generate_hash(f"cal:{event.get('title', '')}:{alert_type}"),
            priority=2 if alert_type in ["1h", "result"] else 1,
            requires_llm=False,
            context=event
        )
        
        return alert
    
    async def queue_alert(self, alert: Alert):
        """
        Adiciona alerta à fila para processamento.
        """
        if alert is None:
            return
        
        # Verificar filtros
        if alert.metal and self._should_filter_metal(alert.metal):
            logger.debug(f"Alerta filtrado para {alert.metal}")
            return
        
        # Verificar duplicata
        if self.db.is_alert_sent(alert.content_hash):
            logger.debug(f"Alerta já enviado: {alert.content_hash[:8]}")
            return
        
        self.alert_queue.append(alert)
        logger.info(f"Alerta adicionado à fila: {alert.alert_type} - {alert.metal}")
    
    async def process_queue(self):
        """
        Processa fila de alertas.
        """
        if not self.alert_queue:
            return
        
        if self._is_silenced():
            logger.info("Alertas silenciados, limpando fila")
            self.alert_queue.clear()
            return
        
        # Ordenar por prioridade
        self.alert_queue.sort(key=lambda x: x.priority, reverse=True)
        
        while self.alert_queue:
            if not self._check_rate_limit():
                logger.warning("Rate limit atingido, aguardando")
                break
            
            alert = self.alert_queue.pop(0)
            
            try:
                # Enriquecer com LLM se necessário
                alert = await self._enrich_with_llm(alert)
                
                # Enviar
                await self.send_callback(alert.message)
                
                # Marcar como enviado
                self.db.mark_alert_sent(alert.alert_type, alert.content_hash, alert.metal)
                self.alerts_sent_this_hour += 1
                
                logger.info(f"Alerta enviado: {alert.alert_type}")
                
                # Pequeno delay entre alertas
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Erro ao enviar alerta: {e}")
                self.db.log_error("alert", "send", str(e))
    
    def update_config(self, key: str, value: Any):
        """Atualiza configuração do usuário."""
        self.user_config[key] = value
        self.db.set_config(key, value)
    
    def silence(self, minutes: int):
        """Silencia alertas por X minutos."""
        until = utcnow() + timedelta(minutes=minutes)
        self.update_config("silenciado_ate", until.isoformat())
    
    def unsilence(self):
        """Reativa alertas."""
        self.update_config("silenciado_ate", None)
        self.update_config("alertas_ativos", True)
    
    def set_filter(self, metals: List[str]):
        """Define filtro de metais."""
        self.update_config("filtros", [m.upper() for m in metals])


# Singleton
_processor: Optional[AlertProcessor] = None


def get_alert_processor(send_callback: Callable = None) -> AlertProcessor:
    """Retorna instância singleton do processador de alertas."""
    global _processor
    if _processor is None:
        if send_callback is None:
            raise ValueError("send_callback é obrigatório na primeira inicialização")
        _processor = AlertProcessor(send_callback)
    return _processor
