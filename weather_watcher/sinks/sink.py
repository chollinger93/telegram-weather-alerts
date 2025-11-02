import json
import os
import time
from abc import ABC, abstractmethod
from dataclasses import asdict
from pathlib import Path

import influxdb_client
import telegram
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from loguru import logger
from overrides import overrides

from weather_watcher.model.stats import WeatherStats
from weather_watcher.utils import escape_telegram_markdown_v2


class Sink(ABC):
    def __init__(
        self,
        st: WeatherStats,
        now: str,
        out_path: Path | None = None,
        bot: telegram.Bot | None = None,
        chat_id: int | None = None,
        skip_telegram: bool = False,
    ):
        self.st = st
        self.now = now
        self.out_path = out_path
        self.bot = bot
        self.chat_id = chat_id
        self.skip_telegram = skip_telegram

    @abstractmethod
    def sink(self) -> None:
        pass

    async def send_to_telegram(self) -> None:
        pass


class ParquetSink(Sink):
    def __init__(
        self,
        st: WeatherStats,
        now: str,
        out_path: Path,
        bot: telegram.Bot | None = None,
        chat_id: int | None = None,
        skip_telegram: bool = False,
    ):
        super().__init__(
            st=st,
            now=now,
            out_path=out_path,
            bot=bot,
            chat_id=chat_id,
            skip_telegram=skip_telegram,
        )
        self.out_path = out_path
        self.data_path = self.out_path / f"{now}_weather.parquet"

    @overrides
    def sink(self):
        # Data
        self.st.raw_df.to_parquet(self.data_path)


class StatsJSONSink(Sink):
    def __init__(
        self,
        st: WeatherStats,
        now: str,
        out_path: Path,
        bot: telegram.Bot,
        chat_id: int,
        skip_telegram: bool = False,
    ):
        super().__init__(
            st=st,
            now=now,
            out_path=out_path,
            bot=bot,
            chat_id=chat_id,
            skip_telegram=skip_telegram,
        )
        self.out_path = out_path
        self.stats_path = self.out_path / f"{self.now}_weather_stats.json"

    @overrides
    def sink(self) -> None:
        with open(self.stats_path, "w") as f:
            f.write(json.dumps(asdict(self.st), indent=4, sort_keys=True, default=str))

    @overrides
    async def send_to_telegram(self) -> None:
        if not self.bot or not self.chat_id or self.skip_telegram:
            logger.warning(
                f"Skipping telegram stats sink: {self.skip_telegram} skip, bot {self.bot}, chat_id {self.chat_id}"
            )
            return
        async with self.bot:
            msgs = self.st.build_msgs()
            logger.info(msgs)
            msg = "\n".join(msgs)
            escaped_msg = escape_telegram_markdown_v2(msg)
            logger.debug(f"Sending msg: {escaped_msg}")
            await self.bot.send_message(
                text=escaped_msg, chat_id=self.chat_id, parse_mode="MarkdownV2"
            )  # type: ignore


class FigureSink(Sink):
    def __init__(
        self,
        st: WeatherStats,
        now: str,
        out_path: Path,
        bot: telegram.Bot,
        chat_id: int,
        skip_telegram: bool = False,
    ):
        super().__init__(
            st=st,
            now=now,
            out_path=out_path,
            bot=bot,
            chat_id=chat_id,
            skip_telegram=skip_telegram,
        )
        self.out_path = out_path
        # Pre-plot/cache image
        self.img_path = self.out_path / f"{now}_weather.png"
        self._plot()

    @overrides
    def sink(self) -> None:
        self._plot()

    @overrides
    async def send_to_telegram(self) -> None:
        if not self.bot or not self.chat_id or self.skip_telegram:
            logger.warning(
                f"Skipping telegram figure sink: {self.skip_telegram} skip, bot {self.bot}, chat_id {self.chat_id}"
            )
            return
        async with self.bot:
            await self.bot.send_photo(
                chat_id=self.chat_id, photo=open(self.img_path, "rb")
            )  # type: ignore

    def _plot(self):
        if self.img_path.exists():
            return
        self.fig = self.st.plot_weather()
        self.fig.write_image(self.img_path)


class InfluxDBSink(Sink):
    def __init__(
        self,
        st: WeatherStats,
        now: str,
        out_path: Path,
        bot: telegram.Bot | None = None,
        chat_id: int | None = None,
        skip_telegram: bool = False,
    ):
        super().__init__(
            st=st,
            now=now,
            out_path=out_path,
            bot=bot,
            chat_id=chat_id,
            skip_telegram=skip_telegram,
        )
        self.out_path = out_path
        self.url = os.environ.get("INFLUX_DB_URL")
        self.org = os.environ.get("INFLUX_DB_ORG")
        self.token = os.environ.get("INFLUX_DB_TOKEN")
        self.bucket = os.environ.get("INFLUX_DB_BUCKET", "weather")
        self.client: InfluxDBClient | None = None
        if not self.url or not self.org or not self.token:
            logger.warning("InfluxDB credentials not set, skipping InfluxDB sink")
            return
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)

    @overrides
    def sink(self):
        if not self.client or not self.org:
            return

        write_api = self.client.write_api(write_options=SYNCHRONOUS)

        sent = 0
        for rec in self.st.raw_df.to_dict(orient="records"):
            point = Point("weather")
            point = point.time(rec["time_epoch"], write_precision=WritePrecision.S)
            for key in ["temp_f", "humidity", "wind_mph", "precip_mm"]:
                if key in rec:
                    point = point.field(key, rec[key])
            for k, v in self.st.meta.location.as_tags().items():
                point = point.tag(k, v)

            # logger.debug(f"Writing value to InfluxDB: {point}")
            write_api.write(bucket=self.bucket, org=self.org, record=point)
            sent += 1

        logger.info(
            f"Sent {sent} records to InfluxDB at {self.url} in bucket {self.bucket}"
        )
