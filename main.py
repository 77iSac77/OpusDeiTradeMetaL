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
from datetime import datetime

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


class OpusDeiTradeMetaLApp:
    """Application orchestrator (bot + scheduler jobs)."""

    def __init__(self) -> None:
        self.db = get_database()
        self.bot = get_telegram_bot()

        self.price_collector = get_price_collector()
        self.technical = get_technical_analyzer()
        self.macro = get_macro_collector()
        self.institutional = get_institutional_collector()

        # Reuse the same alert processor instance (no re-creation per job).
        self.alert_processor = get_alert_processor(self.bot.send_message)

        # Scheduler runs in UTC by default (we format multi-timezones inside messages).
        self.scheduler = AsyncIOScheduler(timezone="UTC")

        self._stopped = asyncio.Event()

    # -------------------------------------------------------------------------
    # One-shot jobs (used by APScheduler)
    # -------------------------------------------------------------------------

    async def job_collect_prices(self) -> None:
        try:
            prices = await self.price_collector.collect_all_prices()

            for metal, price_data in prices.items():
                # 15m / 1h / 1d timeframes (same as your spec)
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

    async def job_collect_technical(self) -> None:
        try:
            # Keep it focused on primary metals for frequent updates
            for metal in ("XAU", "XAG", "XPT", "XCU"):
                await self.technical.update_levels_for_metal(metal)

            # Proximity checks for all metals (spec)
            for metal in METAIS.keys():
                price_data = self.price_collector.get_last_price(metal)
                if not price_data:
                    continue

                proximity_alerts = self.technical.check_proximity_alerts(metal, price_data.price)
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

    async def job_collect_institutional(self) -> None:
        try:
            # COT is published weekly; this keeps a best-effort update on Fridays (UTC)
            if datetime.utcnow().weekday() == 4:
                await self.institutional.fetch_cot_report()

            await self.institutional.fetch_all_etf_data()

            movements = await self.institutional.fetch_all_onchain_movements()
            whale_alerts = self.institutional.check_whale_alerts(movements)
            for movement in whale_alerts:
                alert = await self.alert_processor.process_whale_movement(movement.to_dict())
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

    async def job_digest_asia(self) -> None:
        try:
            await self._send_digest("asia")
        except Exception as exc:
            logger.exception("digest asia failed: %s", exc)
            self.db.log_error("main", "job_digest_asia", str(exc))

    async def job_digest_eu_us(self) -> None:
        try:
            await self._send_digest("eu_us")
        except Exception as exc:
            logger.exception("digest eu/us failed: %s", exc)
            self.db.log_error("main", "job_digest_eu_us", str(exc))

    async def job_digest_weekly(self) -> None:
        try:
            await self._send_digest("weekly")
        except Exception as exc:
            logger.exception("digest weekly failed: %s", exc)
            self.db.log_error("main", "job_digest_weekly", str(exc))

    async def job_keepalive(self) -> None:
        try:
            self.db.increment_counter("keepalive")
        except Exception as exc:
            logger.exception("keepalive job failed: %s", exc)
            self.db.log_error("main", "job_keepalive", str(exc))

    async def job_cleanup(self) -> None:
        try:
            self.db.cleanup_old_alerts(7)
            self.db.clear_expired_cache()
            self.db.vacuum()
            logger.info("cleanup completed")
        except Exception as exc:
            logger.exception("cleanup job failed: %s", exc)
            self.db.log_error("main", "job_cleanup", str(exc))

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    async def _send_digest(self, period: str) -> None:
        from bot.formatter import MessageFormatter
        from config.settings import formato_metal

        formatter = MessageFormatter()

        prices = await self.price_collector.collect_all_prices()
        prices_dict = {
            code: {"price": data.price, "change": data.change_percent}
            for code, data in prices.items()
        }

        highlights = []
        sorted_by_change = sorted(
            prices.items(),
            key=lambda x: abs(x[1].change_percent),
            reverse=True,
        )
        for code, data in sorted_by_change[:3]:
            direction = "📈" if data.change_percent > 0 else "📉"
            highlights.append(f"{direction} {formato_metal(code)}: {data.change_percent:+.2f}%")

        if period == "asia":
            msg = formatter.format_digest_asia(prices_dict, highlights)
        elif period == "eu_us":
            msg = formatter.format_digest_eu_us(prices_dict, highlights)
        else:
            msg = formatter.format_digest_weekly({"performance": prices_dict})

        await self.bot.send_message(msg)

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def _configure_scheduler(self) -> None:
        # Global defaults: avoid overlap and collapse bursts after downtime.
        job_defaults = {
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 30,
        }
        self.scheduler.configure(job_defaults=job_defaults)

        # High-frequency price job
        self.scheduler.add_job(self.job_collect_prices, "interval", seconds=30, id="prices")

        # Technical analysis (5 min)
        self.scheduler.add_job(self.job_collect_technical, "interval", minutes=5, id="technical")

        # Macro (30 min)
        self.scheduler.add_job(self.job_collect_macro, "interval", minutes=30, id="macro")

        # Institutional (1 hour)
        self.scheduler.add_job(self.job_collect_institutional, "interval", hours=1, id="institutional")

        # Digests based on market closes (UTC)
        # Asia close: ~07:30 UTC
        self.scheduler.add_job(self.job_digest_asia, "cron", hour=7, minute=30, id="digest_asia")

        # COMEX close: ~21:30 UTC
        self.scheduler.add_job(self.job_digest_eu_us, "cron", hour=21, minute=30, id="digest_eu_us")

        # Weekly digest: Saturday night São Paulo (~20:00 local) -> 23:00 UTC
        self.scheduler.add_job(self.job_digest_weekly, "cron", day_of_week="sat", hour=23, minute=0, id="digest_weekly")

        # Keepalive (anti-sleep) – from config
        self.scheduler.add_job(
            self.job_keepalive,
            "interval",
            seconds=int(BOT_CONFIG.get("ping_interval_seconds", 240)),
            id="keepalive",
        )

        # Cleanup daily at 03:20 UTC (quiet window)
        self.scheduler.add_job(self.job_cleanup, "cron", hour=3, minute=20, id="cleanup")

    async def start(self) -> None:
        logger.info("starting OpusDeiTradeMetaL...")

        await self.bot.start()

        # Optional startup message (PT is fine; user-facing)
        await self.bot.send_message("🤖 OpusDeiTradeMetaL iniciado e monitorando!")

        # Warmup (best-effort)
        try:
            await self.price_collector.collect_all_prices()
            await self.technical.update_all_levels()
        except Exception as exc:
            logger.warning("warmup failed (continuing): %s", exc)

        self._configure_scheduler()
        self.scheduler.start()
        logger.info("scheduler started with %d jobs", len(self.scheduler.get_jobs()))

        await self._stopped.wait()

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

        logger.info("stopped")


def _install_signal_handlers(app: OpusDeiTradeMetaLApp) -> None:
    def _handler(signum, _frame) -> None:
        logger.info("signal received: %s", signum)
        # We cannot await here; schedule stop safely.
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(app.stop())
        except RuntimeError:
            pass

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


async def main() -> None:
    app = OpusDeiTradeMetaLApp()
    _install_signal_handlers(app)

    try:
        await app.start()
    finally:
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())