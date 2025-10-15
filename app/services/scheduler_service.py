import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import settings
from app.services.epg_fetch_service import fetch_and_process


logger = logging.getLogger("epg_service.scheduler")

class EPGScheduler:
    """Scheduler for automatic EPG fetching"""

    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self._setup_jobs()

    def _setup_jobs(self) -> None:
        """Setup scheduled jobs"""
        try:
            trigger = CronTrigger.from_crontab(settings.epg_fetch_cron)

            self.scheduler.add_job(
                self._fetch_job,
                trigger=trigger,
                id='epg_fetch',
                name='EPG Fetch Job',
                replace_existing=True,
                misfire_grace_time=3600
            )

            logger.info(f"Scheduled EPG fetch with cron: {settings.epg_fetch_cron}")

        except Exception as e:
            logger.error(f"Failed to setup scheduler: {e}", exc_info=True)
            raise

    async def _fetch_job(self) -> None:
        """Job that runs the EPG fetch"""
        logger.info("Scheduled EPG fetch triggered")
        try:
            result = await fetch_and_process()
            if "error" in result:
                logger.error(f"Scheduled fetch failed: {result['error']}")
            else:
                logger.info(f"Scheduled fetch completed: {result}")
        except Exception as e:
            logger.error(f"Exception in scheduled fetch: {e}", exc_info=True)

    def start(self) -> None:
        """Start the scheduler"""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler started")

    def shutdown(self) -> None:
        """Shutdown the scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")

    def get_next_run_time(self) -> datetime | None:
        """Get next scheduled run time"""
        job = self.scheduler.get_job('epg_fetch')
        return job.next_run_time if job else None


epg_scheduler = EPGScheduler()
