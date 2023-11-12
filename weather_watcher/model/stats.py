from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from overrides import overrides
from utils import time_to_str

WeatherData = dict[str, Any]


@dataclass
class Location:
    zip_code: str
    lat: float
    lon: float
    name: str
    region: str
    country: str
    tz_id: str

    def __str__(self) -> str:
        return f"{self.zip_code}: {self.name}, {self.region}"


@dataclass
class MetaStats:
    observed_hrs: int
    location: Location


@dataclass
class GeneralStat(ABC):
    @staticmethod
    def apply(df: pd.DataFrame) -> Any:
        raise NotImplementedError()

    @property
    @abstractmethod
    def name(self) -> Optional[str]:
        raise NotImplementedError()

    @abstractmethod
    def get_msg(self, meta: MetaStats) -> list[str]:
        raise NotImplementedError()


@dataclass
class TimeStats(GeneralStat):
    observed_hrs: int
    from_time: datetime
    to_time: datetime
    sunset: datetime
    sunrise: datetime

    @property
    @overrides
    def name(self) -> Optional[str]:
        return None

    @overrides
    def get_msg(self, meta: MetaStats) -> list[str]:
        msgs = [
            f"🌡️ Weather Report for {meta.location} 🌡️",
            f"Generated at: {datetime.now().strftime('%a, %b %d %Y @ %I:%M%p')}",
            f"From: {time_to_str(self.from_time)} to {time_to_str(self.to_time)}",
            "",
        ]
        msgs.append(f"🌅 Sunrise: {time_to_str(self.sunrise)}")
        msgs.append(f"🌇 Sunset: {time_to_str(self.sunset)}")
        return msgs

    @staticmethod
    @overrides
    def apply(df: pd.DataFrame) -> "TimeStats":
        observed_hrs = (
            int((df["time"].max() - df["time"].min()).total_seconds() / 60 / 60) + 1
        )
        return TimeStats(
            observed_hrs=observed_hrs,
            from_time=df["time"].min(),
            to_time=df["time"].max(),
            sunset=df["sunset"].max(),
            sunrise=df["sunrise"].min(),
        )


@dataclass
class TempStats(GeneralStat):
    min_temp: float
    min_temp_time: datetime
    max_temp: float
    max_temp_time: datetime
    avg_temp: float
    avg_humidity: float

    @property
    @overrides
    def name(self) -> Optional[str]:
        return "Temperatures"

    @overrides
    def get_msg(self, meta: MetaStats) -> list[str]:
        msgs = []
        msgs.append(f"⬇️ Lowest temp: {self.min_temp}F at {self.min_temp_time}")
        msgs.append(f"⬆️ Highest temp: {self.max_temp}F at {self.max_temp_time}")
        msgs.append(f"🦆 Average humidity: {self.avg_humidity:.1f}%")
        return msgs

    @staticmethod
    @overrides
    def apply(df: pd.DataFrame) -> "TempStats":
        return TempStats(
            min_temp=df["temp_f"].min(),
            max_temp_time=df.loc[df["temp_f"] == df["temp_f"].max()].iloc[0]["time"],
            max_temp=df["temp_f"].max(),
            min_temp_time=df.loc[df["temp_f"] == df["temp_f"].min()].iloc[0]["time"],
            avg_temp=df["temp_f"].mean(),
            avg_humidity=df["humidity"].mean(),
        )


@dataclass
class RainStats(GeneralStat):
    has_rain: bool
    total_rain_mm: float
    rain_start_time: Optional[datetime]
    rain_end_time: Optional[datetime]

    @property
    @overrides
    def name(self) -> Optional[str]:
        return "Rainfall"

    @overrides
    def get_msg(self, meta: MetaStats) -> list[str]:
        msgs = []
        if not self.has_rain:
            msgs.append(f"🌵 No rain in the next {meta.observed_hrs} hours")
        else:
            msgs.append(f"⚠️ Total rain: {self.total_rain_mm:.2f}mm")
            msgs.append(f"☔️ Rain starts: {self.rain_start_time}")  # type: ignore
            msgs.append(f"☔️ Rain ends: {self.rain_end_time}")  # type: ignore
        return msgs

    @staticmethod
    @overrides
    def apply(df: pd.DataFrame) -> "RainStats":
        # Rain
        rain_start_time = None
        rain_end_time = None
        if df["precip_mm"].max() > 0:
            rain_start_time = df.loc[df["precip_mm"] > 0]["time"].min()
            rain_end_time = df.loc[df["precip_mm"] > 0]["time"].max()

        return RainStats(
            has_rain=df["precip_mm"].max() > 0,
            total_rain_mm=df["precip_mm"].sum(),
            rain_start_time=rain_start_time,
            rain_end_time=rain_end_time,
        )


@dataclass
class WindStats(GeneralStat):
    avg_wind_mph: float
    max_wind_mph: float
    max_wind_at: datetime

    @property
    @overrides
    def name(self) -> Optional[str]:
        return "Wind"

    @overrides
    def get_msg(self, meta: MetaStats) -> list[str]:
        msgs = []
        msgs.append(f"🌬️ Average wind: {self.avg_wind_mph:.1f}mph")
        msgs.append(f"🌬️ Max wind: {self.max_wind_mph:.1f}mph at {self.max_wind_at}")
        return msgs

    @staticmethod
    @overrides
    def apply(df: pd.DataFrame) -> "WindStats":
        return WindStats(
            avg_wind_mph=df["wind_mph"].mean(),
            max_wind_mph=df["wind_mph"].max(),
            max_wind_at=df.loc[df["wind_mph"] == df["wind_mph"].max()].iloc[0]["time"],
        )


@dataclass
class FreezingStats(GeneralStat):
    is_freezing: bool
    avg_low_during_freezing: Optional[float]
    freezing_hrs: int
    first_safe_temp: Optional[float]
    first_safe_temp_time: Optional[datetime]

    @property
    @overrides
    def name(self) -> Optional[str]:
        return "Frost"

    @overrides
    def get_msg(self, meta: MetaStats) -> list[str]:
        msgs = []
        if not self.is_freezing:
            msgs.append(f"✅ No freezing temps in the next {meta.observed_hrs} hours")
        else:
            msgs.append(
                f"⚠️ {self.freezing_hrs} hours of freezing temps in the next {meta.observed_hrs} hours! 🥶"
            )
            msgs.append(
                f"❄️ Average low will be: {self.avg_low_during_freezing:.1f}F during that time!"
            )
            # Back to safety
            msgs.append(
                f"🌤️ Safe temperature of {self.first_safe_temp}F reached at {self.first_safe_temp_time}"
            )
        return msgs

    @staticmethod
    @overrides
    def apply(df: pd.DataFrame) -> "FreezingStats":
        min_temp_f: float = 32.00
        # Defaults
        avg_low_during_freezing = None
        freezing_hrs = 0
        first_safe_temp = None
        first_safe_temp_time = None
        # Calc
        freezing_hrs_df = df.loc[df["temp_f"] <= min_temp_f]
        is_freezing = len(freezing_hrs_df) > 0
        if is_freezing:
            min_tmp_df = df.loc[df["temp_f"] == df["temp_f"].min()]
            freezing_hrs_df = df.loc[
                (df["temp_f"] <= min_temp_f)
                & (df["time"] <= min_tmp_df.iloc[0]["time"])
            ]
            avg_low_during_freezing = freezing_hrs_df["temp_f"].mean()
            freezing_hrs = len(freezing_hrs_df)
            safe_temps_reached_df = df.loc[
                (df["temp_f"] >= min_temp_f)
                & (df["time"] >= min_tmp_df.iloc[0]["time"])
            ]
            first_safe_temp_df = safe_temps_reached_df.loc[
                safe_temps_reached_df["time"] == safe_temps_reached_df["time"].min()
            ].iloc[0]
            first_safe_temp = first_safe_temp_df["temp_f"]
            first_safe_temp_time = first_safe_temp_df["time"]

        return FreezingStats(
            is_freezing=is_freezing,
            avg_low_during_freezing=avg_low_during_freezing,
            freezing_hrs=freezing_hrs,
            first_safe_temp=first_safe_temp,
            first_safe_temp_time=first_safe_temp_time,
        )


@dataclass
class SummaryStat(TimeStats, TempStats, RainStats, WindStats, FreezingStats):
    @staticmethod
    def apply(df: pd.DataFrame) -> "SummaryStat":
        return SummaryStat(
            **asdict(TimeStats.apply(df)),
            **asdict(TempStats.apply(df)),
            **asdict(RainStats.apply(df)),
            **asdict(WindStats.apply(df)),
            **asdict(FreezingStats.apply(df)),
        )


@dataclass
class WeatherStats:
    time: TimeStats
    temp: TempStats
    rain: RainStats
    wind: WindStats
    freezing: FreezingStats
    meta: MetaStats
    all: SummaryStat

    def build_msgs(self) -> list[str]:
        msgs = []
        for stat in [self.time, self.temp, self.rain, self.wind, self.freezing]:
            header = stat.name
            if header:
                msgs.append(f"## {header}")
            msgs.extend(stat.get_msg(self.meta))
            msgs.append("")
        return msgs

    @staticmethod
    def apply(df: pd.DataFrame, raw: WeatherData, zip_code: str) -> "WeatherStats":
        observed_hrs = (
            int((df["time"].max() - df["time"].min()).total_seconds() / 60 / 60) + 1
        )
        location = Location(
            zip_code=zip_code,
            lat=raw["location"]["lat"],
            lon=raw["location"]["lon"],
            name=raw["location"]["name"],
            region=raw["location"]["region"],
            country=raw["location"]["country"],
            tz_id=raw["location"]["tz_id"],
        )
        return WeatherStats(
            time=TimeStats.apply(df),
            temp=TempStats.apply(df),
            rain=RainStats.apply(df),
            wind=WindStats.apply(df),
            freezing=FreezingStats.apply(df),
            meta=MetaStats(observed_hrs=observed_hrs, location=location),
            all=SummaryStat.apply(df),
        )
