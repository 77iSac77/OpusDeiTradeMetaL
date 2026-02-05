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

from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config.settings import BOT_CONFIG, METAIS
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

            if datetime.utcnow().weekday() == 4:
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

    def _configure_scheduler(self) -> None:

        job_defaults = {
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 30,
        }

        self.scheduler.configure(job_defaults=job_defaults)

        self.scheduler.add_job(self.job_collect_prices, "interval", seconds=30)
        self.scheduler.add_job(self.job_collect_technical, "interval", minutes=5)
        self.scheduler.add_job(self.job_collect_macro, "interval", minutes=30)
        self.scheduler.add_job(self.job_collect_institutional, "interval", hours=1)

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
    
    import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")


def run_health_server():
    server = HTTPServer(("0.0.0.0", 8000), HealthHandler)
    server.serve_forever()


threading.Thread(target=run_health_server, daemon=True).start()