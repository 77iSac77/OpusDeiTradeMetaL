# “””
OpusDeiTradeMetaL - Main Entry Point

Sistema de alertas em tempo real para metais preciosos e industriais.
“””

import asyncio
import logging
import signal
import sys
from datetime import datetime, timedelta
from typing import Optional

# Configurar logging

logging.basicConfig(
level=logging.INFO,
format=’%(asctime)s - %(name)s - %(levelname)s - %(message)s’,
handlers=[
logging.StreamHandler(sys.stdout),
]
)
logger = logging.getLogger(**name**)

from config.settings import BOT_CONFIG, METAIS
from storage.database import get_database
from bot.handler import get_telegram_bot
from collectors.prices import get_price_collector
from collectors.technical import get_technical_analyzer
from collectors.macro import get_macro_collector
from collectors.institutional import get_institutional_collector
from processors.alerts import get_alert_processor
from utils.time_utils import get_next_digest_time

class OpusDeiTradeMetaL:
“”“Classe principal do sistema.”””

```
def __init__(self):
    self.db = get_database()
    self.bot = get_telegram_bot()
    self.price_collector = get_price_collector()
    self.technical = get_technical_analyzer()
    self.macro = get_macro_collector()
    self.institutional = get_institutional_collector()
    
    self.running = False
    self.tasks = []

async def collect_prices_loop(self):
    """Loop de coleta de preços."""
    logger.info("Iniciando loop de coleta de preços")
    
    while self.running:
        try:
            # Coletar preços
            prices = await self.price_collector.collect_all_prices()
            logger.debug(f"Coletados {len(prices)} preços")
            
            # Verificar alertas de preço
            processor = get_alert_processor(self.bot.send_message)
            
            for metal, price_data in prices.items():
                # Verificar mudanças em diferentes timeframes
                for minutes in [15, 60, 1440]:
                    change = self.price_collector.calculate_change(metal, minutes)
                    if change:
                        change_percent, change_value = change
                        
                        # Criar alerta se necessário
                        alert = await processor.process_price_change(
                            metal=metal,
                            current_price=price_data.price,
                            change_percent=change_percent,
                            change_value=change_value,
                            timeframe_minutes=minutes,
                        )
                        
                        if alert:
                            await processor.queue_alert(alert)
            
            # Processar fila de alertas
            await processor.process_queue()
            
        except Exception as e:
            logger.error(f"Erro no loop de preços: {e}")
            self.db.log_error("main", "price_loop", str(e))
        
        # Intervalo de 30 segundos
        await asyncio.sleep(30)

async def collect_technical_loop(self):
    """Loop de cálculo de níveis técnicos."""
    logger.info("Iniciando loop de análise técnica")
    
    while self.running:
        try:
            # Atualizar níveis técnicos para metais principais
            for metal in ["XAU", "XAG", "XPT", "XCU"]:
                await self.technical.update_levels_for_metal(metal)
            
            # Verificar proximidade de níveis
            processor = get_alert_processor(self.bot.send_message)
            
            for metal in METAIS.keys():
                price_data = self.price_collector.get_last_price(metal)
                if not price_data:
                    continue
                
                # Verificar proximidade
                proximity_alerts = self.technical.check_proximity_alerts(
                    metal, price_data.price
                )
                
                for prox in proximity_alerts:
                    alert = await processor.process_technical_proximity(
                        metal=metal,
                        current_price=price_data.price,
                        level_name=prox["level"].name,
                        level_value=prox["level"].value,
                        level_type=prox["level"].level_type.value,
                        distance_percent=prox["distance_percent"],
                    )
                    if alert:
                        await processor.queue_alert(alert)
            
            await processor.process_queue()
            
        except Exception as e:
            logger.error(f"Erro no loop técnico: {e}")
            self.db.log_error("main", "technical_loop", str(e))
        
        # Intervalo de 5 minutos
        await asyncio.sleep(300)

async def collect_macro_loop(self):
    """Loop de coleta de dados macro."""
    logger.info("Iniciando loop macro")
    
    while self.running:
        try:
            # Coletar dados macro (menos frequente)
            await self.macro.fetch_key_macro_data()
            
            # Verificar alertas de calendário
            event_alerts = self.macro.check_event_alerts()
            
            processor = get_alert_processor(self.bot.send_message)
            
            for event_alert in event_alerts:
                alert = await processor.process_calendar_event(
                    event_alert["event"].to_dict(),
                    event_alert["type"]
                )
                if alert:
                    await processor.queue_alert(alert)
            
            await processor.process_queue()
            
        except Exception as e:
            logger.error(f"Erro no loop macro: {e}")
            self.db.log_error("main", "macro_loop", str(e))
        
        # Intervalo de 30 minutos
        await asyncio.sleep(1800)

async def collect_institutional_loop(self):
    """Loop de coleta de dados institucionais."""
    logger.info("Iniciando loop institucional")
    
    while self.running:
        try:
            # COT (atualiza sexta-feira)
            if datetime.utcnow().weekday() == 4:  # Sexta
                await self.institutional.fetch_cot_report()
            
            # ETF flows diariamente
            await self.institutional.fetch_all_etf_data()
            
            # On-chain
            movements = await self.institutional.fetch_all_onchain_movements()
            whale_alerts = self.institutional.check_whale_alerts(movements)
            
            processor = get_alert_processor(self.bot.send_message)
            
            for movement in whale_alerts:
                alert = await processor.process_whale_movement(movement.to_dict())
                if alert:
                    await processor.queue_alert(alert)
            
            # COT alerts
            cot_alerts = self.institutional.check_cot_alerts()
            for cot_alert in cot_alerts:
                cot = self.institutional.get_cot_for_metal(cot_alert["metal"])
                if cot:
                    alert = await processor.process_cot_update(
                        cot_alert["metal"],
                        cot.to_dict()
                    )
                    if alert:
                        await processor.queue_alert(alert)
            
            await processor.process_queue()
            
        except Exception as e:
            logger.error(f"Erro no loop institucional: {e}")
            self.db.log_error("main", "institutional_loop", str(e))
        
        # Intervalo de 1 hora
        await asyncio.sleep(3600)

async def digest_loop(self):
    """Loop de geração de digests."""
    logger.info("Iniciando loop de digests")
    
    while self.running:
        try:
            now = datetime.utcnow()
            
            # Digest Ásia (07:30 UTC)
            if now.hour == 7 and 30 <= now.minute < 35:
                if not self.db.get_config(f"digest_asia_{now.date()}", False):
                    await self._send_digest("asia")
                    self.db.set_config(f"digest_asia_{now.date()}", True)
            
            # Digest EU/US (21:30 UTC)
            if now.hour == 21 and 30 <= now.minute < 35:
                if not self.db.get_config(f"digest_euus_{now.date()}", False):
                    await self._send_digest("eu_us")
                    self.db.set_config(f"digest_euus_{now.date()}", True)
            
            # Digest Semanal (Sábado 20:00 UTC)
            if now.weekday() == 5 and now.hour == 20 and now.minute < 5:
                week = now.isocalendar()[1]
                if not self.db.get_config(f"digest_weekly_{now.year}_{week}", False):
                    await self._send_digest("weekly")
                    self.db.set_config(f"digest_weekly_{now.year}_{week}", True)
            
        except Exception as e:
            logger.error(f"Erro no loop de digest: {e}")
            self.db.log_error("main", "digest_loop", str(e))
        
        # Verificar a cada 5 minutos
        await asyncio.sleep(300)

async def _send_digest(self, period: str):
    """Envia digest."""
    from bot.formatter import MessageFormatter
    formatter = MessageFormatter()
    
    prices = await self.price_collector.collect_all_prices()
    prices_dict = {}
    for code, price_data in prices.items():
        prices_dict[code] = {
            "price": price_data.price,
            "change": price_data.change_percent,
        }
    
    highlights = []
    sorted_by_change = sorted(
        prices.items(),
        key=lambda x: abs(x[1].change_percent),
        reverse=True
    )
    for code, data in sorted_by_change[:3]:
        direction = "📈" if data.change_percent > 0 else "📉"
        from config.settings import formato_metal
        highlights.append(f"{direction} {formato_metal(code)}: {data.change_percent:+.2f}%")
    
    if period == "asia":
        msg = formatter.format_digest_asia(prices_dict, highlights)
    elif period == "eu_us":
        msg = formatter.format_digest_eu_us(prices_dict, highlights)
    else:
        msg = formatter.format_digest_weekly({"performance": prices_dict})
    
    await self.bot.send_message(msg)

async def keepalive_loop(self):
    """Loop de keep-alive para evitar sleep."""
    logger.info("Iniciando loop de keep-alive")
    
    while self.running:
        try:
            # Self-ping interno
            self.db.increment_counter("keepalive")
            logger.debug("Keep-alive ping")
            
        except Exception as e:
            logger.error(f"Erro no keep-alive: {e}")
        
        # Ping a cada 4 minutos
        await asyncio.sleep(240)

async def cleanup_loop(self):
    """Loop de limpeza de dados antigos."""
    logger.info("Iniciando loop de cleanup")
    
    while self.running:
        try:
            # Limpar alertas antigos (7 dias)
            self.db.cleanup_old_alerts(7)
            
            # Limpar cache LLM expirado
            self.db.clear_expired_cache()
            
            # Compactar banco
            self.db.vacuum()
            
            logger.info("Cleanup concluído")
            
        except Exception as e:
            logger.error(f"Erro no cleanup: {e}")
        
        # Cleanup diário
        await asyncio.sleep(86400)

async def start(self):
    """Inicia o sistema."""
    logger.info("=" * 50)
    logger.info("OpusDeiTradeMetaL iniciando...")
    logger.info("=" * 50)
    
    self.running = True
    
    try:
        # Iniciar bot Telegram
        await self.bot.start()
        
        # Enviar mensagem de início
        await self.bot.send_message("🤖 OpusDeiTradeMetaL iniciado e monitorando!")
        
        # Coleta inicial
        logger.info("Realizando coleta inicial de dados...")
        await self.price_collector.collect_all_prices()
        await self.technical.update_all_levels()
        
        # Iniciar loops de monitoramento
        self.tasks = [
            asyncio.create_task(self.collect_prices_loop()),
            asyncio.create_task(self.collect_technical_loop()),
            asyncio.create_task(self.collect_macro_loop()),
            asyncio.create_task(self.collect_institutional_loop()),
            asyncio.create_task(self.digest_loop()),
            asyncio.create_task(self.keepalive_loop()),
            asyncio.create_task(self.cleanup_loop()),
        ]
        
        logger.info("Todos os loops iniciados")
        
        # Aguardar indefinidamente
        await asyncio.gather(*self.tasks)
        
    except asyncio.CancelledError:
        logger.info("Recebido sinal de cancelamento")
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        raise
    finally:
        await self.stop()

async def stop(self):
    """Para o sistema."""
    logger.info("Parando OpusDeiTradeMetaL...")
    
    self.running = False
    
    # Cancelar tasks
    for task in self.tasks:
        task.cancel()
    
    # Aguardar cancelamento
    await asyncio.gather(*self.tasks, return_exceptions=True)
    
    # Parar bot
    await self.bot.stop()
    
    logger.info("OpusDeiTradeMetaL parado")
```

def handle_signal(signum, frame):
“”“Handler de sinais do sistema.”””
logger.info(f”Recebido sinal {signum}”)
raise KeyboardInterrupt

async def main():
“”“Função principal.”””
# Configurar handlers de sinal
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

```
app = OpusDeiTradeMetaL()

try:
    await app.start()
except KeyboardInterrupt:
    logger.info("Interrupção pelo usuário")
finally:
    await app.stop()
```

if **name** == “**main**”:
asyncio.run(main())