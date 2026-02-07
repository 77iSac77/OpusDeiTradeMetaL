"""
OpusDeiTradeMetaL - Main entrypoint

Runs the Telegram bot plus background collectors using APScheduler.
All code identifiers are in English for a professional codebase.
User-facing Telegram messages remain in Portuguese by design.
"""

import asyncio
import logging
import signal
import sys
import os
from datetime import datetime
from typing import Dict
from utils.time_utils import utcnow

from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config.settings import BOT_CONFIG, METAIS, formato_metal
from storage.database import get_database
from bot.handler import get_telegram_bot
from collectors.prices import get_price_collector
from collectors.technical import get_technical_analyzer
from collectors.macro import get_macro_collector
from collectors.institutional import get_institutional_collector
from processors.alerts import get_alert_processor


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# HEALTH SERVER (CRÍTICO PARA KOYEB FREE)
# -----------------------------------------------------------------------------

async def start_health_server() -> None:
    """
    Minimal HTTP server to satisfy Koyeb free-tier Web Service requirements.
    Keeps port/health checks passing while the Telegram bot runs via polling.
    """

    port = int(os.getenv("PORT", "8000"))

    async def health(_request):
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.router.add_get("/healthz", health)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    logger.info(f"Health server running on port {port}")


# -----------------------------------------------------------------------------
# APP
# -----------------------------------------------------------------------------

class OpusDeiTradeMetaLApp:
    """Application orchestrator (bot + scheduler jobs)."""

    def __init__(self) -> None:
        self.db = get_database()
        self.bot = get_telegram_bot()

        self.price_collector = get_price_collector()
        self.technical = get_technical_analyzer()
        self.macro = get_macro_collector()
        self.institutional = get_institutional_collector()

        self.alert_processor = get_alert_processor(self.bot.send_message)

        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self._stopped = asyncio.Event()

    # -------------------------------------------------------------------------

    async def job_collect_prices(self) -> None:
        try:
            prices = await self.price_collector.collect_all_prices()

            for metal, price_data in prices.items():

                for minutes in (15, 60, 1440):
                    change = self.price_collector.calculate_change(metal, minutes)
                    if not change:
                        continue

                    change_percent, change_value = change

                    alert = await self.alert_processor.process_price_change(
                        metal=metal,
                        current_price=price_data.price,
                        change_percent=change_percent,
                        change_value=change_value,
                        timeframe_minutes=minutes,
                    )

                    if alert:
                        await self.alert_processor.queue_alert(alert)

            await self.alert_processor.process_queue()

        except Exception as exc:
            logger.exception("price job failed: %s", exc)
            self.db.log_error("main", "job_collect_prices", str(exc))

    # -------------------------------------------------------------------------

    async def job_collect_technical(self) -> None:
        try:
            for metal in ("XAU", "XAG", "XPT", "XCU"):
                await self.technical.update_levels_for_metal(metal)

            for metal in METAIS.keys():

                price_data = self.price_collector.get_last_price(metal)
                if not price_data:
                    continue

                proximity_alerts = self.technical.check_proximity_alerts(
                    metal,
                    price_data.price,
                )

                for prox in proximity_alerts:

                    alert = await self.alert_processor.process_technical_proximity(
                        metal=metal,
                        current_price=price_data.price,
                        level_name=prox["level"].name,
                        level_value=prox["level"].value,
                        level_type=prox["level"].level_type.value,
                        distance_percent=prox["distance_percent"],
                    )

                    if alert:
                        await self.alert_processor.queue_alert(alert)

            await self.alert_processor.process_queue()

        except Exception as exc:
            logger.exception("technical job failed: %s", exc)
            self.db.log_error("main", "job_collect_technical", str(exc))

    # -------------------------------------------------------------------------

    async def job_collect_macro(self) -> None:
        try:
            await self.macro.fetch_key_macro_data()

            event_alerts = self.macro.check_event_alerts()

            for event_alert in event_alerts:

                alert = await self.alert_processor.process_calendar_event(
                    event_alert["event"].to_dict(),
                    event_alert["type"],
                )

                if alert:
                    await self.alert_processor.queue_alert(alert)

            await self.alert_processor.process_queue()

        except Exception as exc:
            logger.exception("macro job failed: %s", exc)
            self.db.log_error("main", "job_collect_macro", str(exc))

    # -------------------------------------------------------------------------

    async def job_collect_institutional(self) -> None:
        try:

            if utcnow().weekday() == 4:
                await self.institutional.fetch_cot_report()

            await self.institutional.fetch_all_etf_data()

            movements = await self.institutional.fetch_all_onchain_movements()
            whale_alerts = self.institutional.check_whale_alerts(movements)

            for movement in whale_alerts:

                alert = await self.alert_processor.process_whale_movement(
                    movement.to_dict()
                )

                if alert:
                    await self.alert_processor.queue_alert(alert)

            cot_alerts = self.institutional.check_cot_alerts()

            for cot_alert in cot_alerts:

                cot = self.institutional.get_cot_for_metal(cot_alert["metal"])
                if not cot:
                    continue

                alert = await self.alert_processor.process_cot_update(
                    cot_alert["metal"],
                    cot.to_dict(),
                )

                if alert:
                    await self.alert_processor.queue_alert(alert)

            await self.alert_processor.process_queue()

        except Exception as exc:
            logger.exception("institutional job failed: %s", exc)
            self.db.log_error("main", "job_collect_institutional", str(exc))

    # -------------------------------------------------------------------------
    # DIGESTS
    # -------------------------------------------------------------------------

    def _build_prices_dict(self) -> Dict:
        """Monta dict de preços para formatação dos digests."""
        prices_dict = {}
        all_prices = self.price_collector.get_all_last_prices()
        for code, price_data in all_prices.items():
            prices_dict[code] = {
                "price": price_data.price,
                "change": price_data.change_percent,
            }
        return prices_dict

    def _build_highlights(self) -> list:
        """Identifica os maiores movimentos para destaque."""
        all_prices = self.price_collector.get_all_last_prices()
        sorted_by_change = sorted(
            all_prices.items(),
            key=lambda x: abs(x[1].change_percent),
            reverse=True,
        )
        highlights = []
        for code, data in sorted_by_change[:4]:
            direction = "📈" if data.change_percent > 0 else "📉"
            highlights.append(
                f"{direction} {formato_metal(code)}: {data.change_percent:+.2f}%"
            )
        return highlights

    async def job_digest_asia(self) -> None:
        """Digest do fechamento da sessão asiática (07:30 UTC)."""
        try:
            if not self.db.get_config("digest_asia", True):
                logger.info("digest_asia desativado, pulando")
                return

            prices_dict = self._build_prices_dict()
            if not prices_dict:
                logger.warning("Sem preços para digest_asia")
                return

            highlights = self._build_highlights()

            from bot.formatter import MessageFormatter
            formatter = MessageFormatter()
            msg = formatter.format_digest_asia(prices_dict, highlights)

            await self.bot.send_message(msg)
            logger.info("digest_asia enviado")

        except Exception as exc:
            logger.exception("digest_asia failed: %s", exc)
            self.db.log_error("main", "job_digest_asia", str(exc))

    async def job_digest_eu_us(self) -> None:
        """Digest do fechamento EU/US (21:30 UTC)."""
        try:
            if not self.db.get_config("digest_eu_us", True):
                logger.info("digest_eu_us desativado, pulando")
                return

            prices_dict = self._build_prices_dict()
            if not prices_dict:
                logger.warning("Sem preços para digest_eu_us")
                return

            highlights = self._build_highlights()

            # Eventos das próximas 16h (até abertura Ásia)
            upcoming_events = self.macro.get_upcoming_events(hours=16)
            upcoming = [
                f"{e.title} ({e.country})" for e in upcoming_events[:3]
            ]

            from bot.formatter import MessageFormatter
            formatter = MessageFormatter()
            msg = formatter.format_digest_eu_us(prices_dict, highlights, upcoming)

            await self.bot.send_message(msg)
            logger.info("digest_eu_us enviado")

        except Exception as exc:
            logger.exception("digest_eu_us failed: %s", exc)
            self.db.log_error("main", "job_digest_eu_us", str(exc))

    async def job_digest_weekly(self) -> None:
        """Digest semanal (sábado 20:00 horário do usuário)."""
        try:
            if not self.db.get_config("digest_weekly", True):
                logger.info("digest_weekly desativado, pulando")
                return

            # Montar performance semanal
            performance = {}
            all_prices = self.price_collector.get_all_last_prices()
            for code, price_data in all_prices.items():
                # Usar variação do price_data como proxy da semana
                weekly_change = self.price_collector.calculate_change(code, 1440 * 7)
                if weekly_change:
                    performance[code] = weekly_change[0]  # percent
                elif price_data.change_percent:
                    performance[code] = price_data.change_percent

            # COT highlights
            cot_highlights = []
            for metal in ("XAU", "XAG", "XCU"):
                cot = self.institutional.get_cot_for_metal(metal)
                if cot:
                    cot_dict = cot.to_dict()
                    signal = cot_dict.get("signal", "")
                    if signal:
                        cot_highlights.append(f"{formato_metal(metal)}: {signal}")

            # Próxima semana
            next_week_events = self.macro.get_upcoming_events(hours=168)
            next_week = [
                f"{e.title} ({e.country})" for e in next_week_events[:5]
            ]

            data = {
                "performance": performance,
                "cot_highlights": cot_highlights,
                "next_week": next_week,
            }

            from bot.formatter import MessageFormatter
            formatter = MessageFormatter()
            msg = formatter.format_digest_weekly(data)

            await self.bot.send_message(msg)
            logger.info("digest_weekly enviado")

        except Exception as exc:
            logger.exception("digest_weekly failed: %s", exc)
            self.db.log_error("main", "job_digest_weekly", str(exc))

    # -------------------------------------------------------------------------
    # KEEP-ALIVE (KOYEB FREE TIER)
    # -------------------------------------------------------------------------

    async def job_self_ping(self) -> None:
        """Pinga o próprio health endpoint para evitar sleep do Koyeb free tier."""
        port = int(os.getenv("PORT", "8000"))
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://localhost:{port}/healthz", timeout=10
                ) as resp:
                    if resp.status == 200:
                        logger.debug("self-ping OK")
                    else:
                        logger.warning(f"self-ping retornou {resp.status}")
        except Exception as exc:
            logger.warning(f"self-ping falhou: {exc}")

    # -------------------------------------------------------------------------

    def _configure_scheduler(self) -> None:

        job_defaults = {
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 30,
        }

        self.scheduler.configure(job_defaults=job_defaults)

        self.scheduler.add_job(self.job_collect_prices, "interval", seconds=90)
        self.scheduler.add_job(self.job_collect_technical, "interval", minutes=5)
        self.scheduler.add_job(self.job_collect_macro, "interval", minutes=30)
        self.scheduler.add_job(self.job_collect_institutional, "interval", hours=1)

        # Auto-ping para manter Koyeb free tier acordado
        self.scheduler.add_job(
            self.job_self_ping, "interval",
            seconds=BOT_CONFIG.get("ping_interval_seconds", 240),
            id="self_ping",
        )

        # Digests automáticos (cron UTC)
        self.scheduler.add_job(
            self.job_digest_asia, "cron",
            hour=7, minute=30, id="digest_asia",
        )
        self.scheduler.add_job(
            self.job_digest_eu_us, "cron",
            hour=21, minute=30, id="digest_eu_us",
        )
        self.scheduler.add_job(
            self.job_digest_weekly, "cron",
            day_of_week="sat", hour=23, minute=0, id="digest_weekly",
        )

    # -------------------------------------------------------------------------

    async def start(self) -> None:

        logger.info("starting OpusDeiTradeMetaL...")

        await self.bot.start()

        await self.bot.send_message("🤖 OpusDeiTradeMetaL iniciado e monitorando!")

        self._configure_scheduler()
        self.scheduler.start()

        await self._stopped.wait()

    # -------------------------------------------------------------------------

    async def stop(self) -> None:

        if self._stopped.is_set():
            return

        logger.info("stopping OpusDeiTradeMetaL...")
        self._stopped.set()

        try:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
        except Exception:
            pass

        try:
            await self.bot.stop()
        except Exception:
            pass


# -----------------------------------------------------------------------------

def _install_signal_handlers(app: OpusDeiTradeMetaLApp) -> None:

    def _handler(signum, _frame):
        logger.info("signal received: %s", signum)

        try:
            loop = asyncio.get_event_loop()
            loop.create_task(app.stop())
        except RuntimeError:
            pass

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


# -----------------------------------------------------------------------------

async def main() -> None:

    await start_health_server()  # ⭐ CRÍTICO PARA KOYEB

    app = OpusDeiTradeMetaLApp()
    _install_signal_handlers(app)

    try:
        await app.start()
    finally:
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())