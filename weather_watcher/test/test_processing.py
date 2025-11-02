import json
import os
import tempfile
from pathlib import Path
from typing import Optional

import pytest
from freezegun import freeze_time
from overrides import overrides

from weather_watcher.main import WeatherWatcher
from weather_watcher.model.stats import WeatherData
from weather_watcher.parser.parser import WeatherAPIParser


class MockWeatherAPIParser(WeatherAPIParser):
    @overrides
    def get_forecast(
        self, key: str, zip_code: str, days: int = 2
    ) -> Optional[WeatherData]:
        with open(
            f"{os.path.join(os.path.dirname(__file__))}/resources/sample_ky.json"
        ) as f:
            return json.load(f)


@freeze_time("2025-02-22 10:15:00")
@pytest.mark.asyncio
async def test_run():
    exp = [
        "🌡️ *Weather Report for 40601: Frankfort, Kentucky* 🌡️",
        "*Generated at*: Sat, Feb 22 2025 @ 10:15AM",
        "*From*: 2025-02-22T00:00:00 to 2025-02-23T10:00:00",
        "",
        "🌅 Sunrise: 07:21AM",
        "🌇 Sunset: 06:27PM",
        "",
        "*Temperatures*",
        "⬇️ Lowest temp: 19.1F at 2025-02-22 07:00:00",
        "⬆️ Highest temp: 34.6F at 2025-02-22 16:00:00",
        "🦆 Average humidity: 83.7%",
        "",
        "*Rainfall*",
        "🌵 No rain in the next 35 hours",
        "",
        "*Wind*",
        "🌬️ Average wind: 5.2mph",
        "🌬️ Max wind: 9.6mph at 2025-02-22 17:00:00",
        "",
        "*Frost*",
        "⚠️ 8 hours of freezing temps in the next 35 hours! 🥶",
        "❄️ Average low will be: 20.9F during that time!",
        "🌤️ No safe temperatures in the next 35 hours!",
        "",
    ]

    # Test
    watcher = WeatherWatcher(parser=MockWeatherAPIParser())
    tmp_dir = Path(tempfile.TemporaryDirectory().name)
    res = await watcher.run(
        telegram_token="fake",
        weather_api_key="fake",
        chat_id=0,
        zip_code="40601",
        out_dir=tmp_dir,
        skip_telegram=True,
    )
    assert res == exp
    assert len(list(tmp_dir.glob("*.png"))) == 1
    assert len(list(tmp_dir.glob("*.parquet"))) == 1
    assert len(list(tmp_dir.glob("*.json"))) == 1
