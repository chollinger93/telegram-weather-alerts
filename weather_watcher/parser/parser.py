from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import urllib3
from loguru import logger
from overrides import overrides

from weather_watcher.model.stats import WeatherData


class WeatherParser(ABC):
    @abstractmethod
    def get_forecast(
        self, key: str, zip_code: str, days: int = 2
    ) -> Optional[WeatherData]:
        pass

    @abstractmethod
    def parse_forecast(self, raw: WeatherData, max_hrs: int) -> pd.DataFrame:
        pass


class WeatherAPIParser(WeatherParser):
    @overrides
    def get_forecast(
        self, key: str, zip_code: str, days: int = 2
    ) -> Optional[WeatherData]:
        try:
            url = f"https://api.weatherapi.com/v1/forecast.json?q={zip_code}&days={days}&key={key}"
            headers = {"Content-Type": "application/json"}
            resp = urllib3.request("GET", url, retries=10, timeout=10, headers=headers)
            if resp.status != 200:
                raise ValueError(f"Bad status code {resp.status}: {resp.data}")
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to get forecast: {e}")
            return None

    @overrides
    def parse_forecast(self, raw: WeatherData, max_hrs: int) -> pd.DataFrame:
        """Weather data, hourly, for the next 48 hours, if in the future

        Args:
            raw (WeatherData): Raw
            max_hrs (int): Max hours look into

        Returns:
            pd.DataFrame: DF
        """
        forecast_days = raw["forecast"]["forecastday"]
        dfs = []
        for day in forecast_days:
            df = pd.DataFrame(day["hour"])
            # Sunset/Sunrise
            cur_dt = datetime.strptime(day["date"], "%Y-%m-%d")
            sunrise = datetime.strptime(day["astro"]["sunrise"], "%I:%M %p").replace(
                year=cur_dt.year, month=cur_dt.month, day=cur_dt.day
            )
            sunset = datetime.strptime(day["astro"]["sunset"], "%I:%M %p").replace(
                year=cur_dt.year, month=cur_dt.month, day=cur_dt.day
            )
            df["sunrise"] = sunrise
            df["sunset"] = sunset
            dfs.append(df)

        hourly = pd.concat(dfs)
        # Time
        now = datetime.now()
        hourly["time"] = pd.to_datetime(hourly["time"])
        hourly = hourly.sort_values(by=["time"])
        hourly = hourly.loc[hourly["time"] <= now + timedelta(days=1)]
        return hourly.reset_index().iloc[0:max_hrs]
