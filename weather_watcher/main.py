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
from weather_watcher.sinks.sink import (
    FigureSink,
    InfluxDBSink,
    ParquetSink,
    Sink,
    StatsJSONSink,
)


class WeatherWatcher:
    def __init__(
        self,
        parser: WeatherParser = WeatherAPIParser(),
        sinks: list[type[Sink]] = [
            ParquetSink,
            StatsJSONSink,
            FigureSink,
            InfluxDBSink,
        ],
    ) -> None:
        self.parser = parser
        self._sinks = sinks

    async def run(
        self,
        telegram_token: str | None,
        weather_api_key: str,
        chat_id: int | None,
        zip_code: str,
        out_dir: Path,
        skip_telegram: bool = False,
    ) -> list[str]:
        # Paths
        out_dir.mkdir(parents=True, exist_ok=True)
        now = datetime.now().isoformat()
        # Telegram
        bot: telegram.Bot | None = None
        if not skip_telegram and telegram_token:
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
        await self._sink_all(stats, now, out_dir, bot, chat_id, skip_telegram)
        return msgs

    async def _sink_all(
        self,
        st: WeatherStats,
        now: str,
        out_path: Path,
        bot: telegram.Bot | None,
        chat_id: int | None,
        skip_telegram: bool,
    ) -> None:
        for sink in self._sinks:
            s = sink(
                st=st,
                now=now,
                out_path=out_path,
                bot=bot,
                chat_id=chat_id,
                skip_telegram=skip_telegram,
            )
            s.sink()
            await s.send_to_telegram()

    def _validate_args(
        self,
        cron: str | None,
        chat_id: str | None,
        force: bool,
        skip_telegram: bool,
        telegram_token: str | None,
        weather_api_key: str | None,
    ) -> None:
        if not weather_api_key:
            raise ValueError("WEATHER_API_KEY not set")

        if not force and not cron:
            raise ValueError("Either --cron or --force must be set")

        if not skip_telegram:
            if not telegram_token:
                raise ValueError("TELEGRAM_TOKEN not set")
            if not chat_id:
                raise ValueError("Chat ID must be provided if telegram is enabled")

    async def main(self):
        parser = argparse.ArgumentParser(description="Grab weather, send to telegram")
        parser.add_argument(
            "-z", "--zip", help="ZIP", required=True, type=str, dest="zip_code"
        )
        parser.add_argument(
            "-s",
            "--cron",
            help="Crontab schedule; required if --force isn't set",
            required=False,
            type=str,
            dest="cron",
        )
        parser.add_argument(
            "-o",
            "--out",
            help="Out dir for storing sink data",
            required=True,
            type=str,
            dest="out_dir",
        )
        parser.add_argument(
            "-f",
            "--force",
            help="Send immediately",
            required=False,
            action="store_true",
        )
        parser.add_argument(
            "-c",
            "--chat",
            help="Chat ID; required if --skip-telegram isn't set",
            required=False,
            type=int,
            dest="chat_id",
        )
        parser.add_argument(
            "--no-telegram",
            help="Skip telegram",
            required=False,
            action="store_true",
            dest="skip_telegram",
        )
        args = vars(parser.parse_args())
        force = args.get("force", False)
        cron = args.get("cron", "")
        skip_telegram = args.get("skip_telegram", False)
        chat_id = args.get("chat_id", None)
        weather_api_key = os.getenv("WEATHER_API_KEY", "")
        telegram_token = os.getenv("TELEGRAM_TOKEN", "")

        self._validate_args(
            cron=cron,
            chat_id=chat_id,
            force=force,
            skip_telegram=skip_telegram,
            weather_api_key=weather_api_key,
            telegram_token=telegram_token,
        )

        if force:
            logger.warning("Force mode, running immediately")
            await self.run(
                weather_api_key=weather_api_key,
                telegram_token=telegram_token,
                chat_id=chat_id,
                zip_code=args["zip_code"],
                skip_telegram=skip_telegram,
                out_dir=Path(args["out_dir"]),
            )
            return

        iter = croniter(cron, datetime.now())
        while True:
            dt = iter.get_next(datetime)
            logger.info(f"Next run at {dt} for {args['zip_code']}")
            pause.until(dt)
            logger.info("Running...")
            await self.run(
                weather_api_key=weather_api_key,
                telegram_token=telegram_token,
                chat_id=args.get("chat_id", None),
                zip_code=args["zip_code"],
                skip_telegram=args["skip_telegram"],
                out_dir=Path(args["out_dir"]),
            )


if __name__ == "__main__":
    watcher = WeatherWatcher()
    asyncio.run(watcher.main())
