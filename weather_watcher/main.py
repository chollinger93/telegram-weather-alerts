import argparse
import asyncio
import os
from datetime import datetime
from pathlib import Path

import pause
import telegram
from croniter import croniter
from loguru import logger

from weather_watcher.model.stats import WeatherStats
from weather_watcher.parser.parser import WeatherAPIParser, WeatherParser
from weather_watcher.sinks.sink import FigureSink, ParquetSink, Sink, StatsJSONSink


class WeatherWatcher:
    def __init__(
        self,
        parser: WeatherParser = WeatherAPIParser(),
        sinks: list[type[Sink]] = [ParquetSink, StatsJSONSink, FigureSink],
    ) -> None:
        self.parser = parser
        self._sinks = sinks

    async def run(
        self,
        telegram_token: str,
        weather_api_key: str,
        chat_id: int,
        zip_code: str,
        out_dir: Path,
        skip_telegram: bool = False,
    ) -> list[str]:
        # Paths
        out_dir.mkdir(parents=True, exist_ok=True)
        now = datetime.now().isoformat()
        # Telegram
        bot = telegram.Bot(telegram_token)
        # Get data
        raw = self.parser.get_forecast(weather_api_key, zip_code=zip_code)
        if not raw:
            logger.warning("Failed to get forecast")
            return []
        # Parse
        hourly = self.parser.parse_forecast(raw, max_hrs=48)
        stats = WeatherStats.apply(hourly, raw, zip_code)
        msgs = stats.build_msgs()
        logger.info(msgs)
        # Cache, send to telegram
        self._sink_all(stats, now, out_dir, bot, chat_id, skip_telegram)
        return msgs

    def _sink_all(
        self,
        st: WeatherStats,
        now: str,
        out_path: Path,
        bot: telegram.Bot,
        chat_id: int,
        skip_telegram: bool,
    ) -> None:
        for sink in self._sinks:
            sink(
                st=st,
                now=now,
                out_path=out_path,
                bot=bot,
                chat_id=chat_id,
                skip_telegram=skip_telegram,
            ).sink()

    async def main(self):
        parser = argparse.ArgumentParser(description="Grab weather, send to telegram")
        parser.add_argument(
            "-c", "--chat", help="Chat ID", required=True, type=int, dest="chat_id"
        )
        parser.add_argument(
            "-z", "--zip", help="ZIP", required=True, type=str, dest="zip_code"
        )
        parser.add_argument(
            "-s",
            "--cron",
            help="Crontab schedule",
            required=True,
            type=str,
            dest="cron",
        )
        parser.add_argument(
            "-o", "--out", help="Out dir", required=True, type=str, dest="out_dir"
        )
        parser.add_argument(
            "-f",
            "--force",
            help="Send immediately",
            required=False,
            action="store_true",
        )
        parser.add_argument(
            "--no-telegram",
            help="Skip telegram",
            required=False,
            action="store_true",
            dest="skip_telegram",
        )
        args = vars(parser.parse_args())

        weather_api_key = os.getenv("WEATHER_API_KEY")
        if not weather_api_key:
            raise ValueError("WEATHER_API_KEY not set")
        telegram_token = os.getenv("TELEGRAM_TOKEN")
        if not telegram_token:
            raise ValueError("TELEGRAM_TOKEN not set")

        iter = croniter(args["cron"], datetime.now())
        while True:
            dt = iter.get_next(datetime)
            if not args["force"]:
                logger.info(f"Next run at {dt} for {args['zip_code']}")
                pause.until(dt)
            else:
                logger.warning("Force mode, skipping cron")
            logger.info("Running...")
            await self.run(
                weather_api_key=weather_api_key,
                telegram_token=telegram_token,
                chat_id=args["chat_id"],
                zip_code=args["zip_code"],
                skip_telegram=args["skip_telegram"],
                out_dir=Path(args["out_dir"]),
            )
            if args["force"]:
                break


if __name__ == "__main__":
    watcher = WeatherWatcher()
    asyncio.run(watcher.main())
