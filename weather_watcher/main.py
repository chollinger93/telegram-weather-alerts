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
from weather_watcher.utils import escape_telegram_markdown_v2


class WeatherWatcher:
    def __init__(
        self,
        parser: WeatherParser = WeatherAPIParser(),
        sinks: list[type[Sink]] = [ParquetSink, StatsJSONSink, FigureSink],
    ) -> None:
        self.parser = parser
        self._sinks = sinks

    async def send_photo_to_bot(self, bot: telegram.Bot, chat_id: int, img_path: Path):
        async with bot:
            await bot.send_photo(chat_id=chat_id, photo=open(img_path, "rb"))  # type: ignore

    async def send_msg_to_bot(self, bot: telegram.Bot, chat_id: int, msg: str):
        async with bot:
            escaped_msg = escape_telegram_markdown_v2(msg)
            logger.debug(f"Sending msg: {escaped_msg}")
            await bot.send_message(
                text=escaped_msg, chat_id=chat_id, parse_mode="MarkdownV2"
            )  # type: ignore

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
        # Build image
        img_path = out_dir / f"{now}_weather.png"
        self._cache_all(stats, now, out_dir)
        # send msg
        if not skip_telegram:
            logger.info(f"Sending to chat id {chat_id}..")
            await self.send_msg_to_bot(bot, chat_id, "\n".join(msgs))
            await self.send_photo_to_bot(bot, chat_id, img_path)
        else:
            logger.warning("Skipping telegram")
        return msgs

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

    def _cache_all(
        self,
        st: WeatherStats,
        now: str,
        out_path: Path,
    ) -> None:
        for sink in self._sinks:
            sink(out_path).sink(st, now)


if __name__ == "__main__":
    watcher = WeatherWatcher()
    asyncio.run(watcher.main())
