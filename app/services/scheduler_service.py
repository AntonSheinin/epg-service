import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import settings
from app.services.epg_fetch_service import fetch_and_process


logger = logging.getLogger(__name__)

class EPGScheduler:
    """Scheduler for automatic EPG fetching"""

    def __init__(self):
        self.scheduler: AsyncIOScheduler | None = None

    async def _fetch_job(self) -> None:
        """Background job that runs the EPG fetch"""
        logger.info("Scheduled EPG fetch triggered")
        try:
            result = await fetch_and_process()
            if "error" in result:
                logger.error(f"Scheduled fetch failed: {result['error']}")
        except Exception as e:
            logger.error(f"Exception in scheduled fetch: {e}", exc_info=True)

    def start(self) -> None:
        """Start the scheduler with EPG fetch job"""
        if self.scheduler and self.scheduler.running:
            logger.warning("Scheduler already running")
            return

        try:
            trigger = CronTrigger.from_crontab(settings.epg_fetch_cron)
        except (ValueError, KeyError) as exc:
            logger.error("Invalid cron expression '%s': %s", settings.epg_fetch_cron, exc)
            raise

        self.scheduler = AsyncIOScheduler(timezone='UTC')
        self.scheduler.add_job(
            self._fetch_job,
            trigger=trigger,
            id='epg_fetch',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=settings.epg_fetch_misfire_grace_sec
        )

        self.scheduler.start()
        next_time = self.get_next_run_time()
        logger.info(
            "Scheduler started. Next fetch: %s",
            next_time.isoformat() if next_time else "unknown"
        )

    def shutdown(self) -> None:
        """Shutdown the scheduler"""
        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")
            self.scheduler = None

    def get_next_run_time(self) -> datetime | None:
        """Get next scheduled fetch time"""
        if not self.scheduler:
            return None
        job = self.scheduler.get_job('epg_fetch')
        return job.next_run_time if job else None


epg_scheduler = EPGScheduler()
