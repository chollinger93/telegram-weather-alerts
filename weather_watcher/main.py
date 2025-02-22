import argparse
import asyncio
import json
import os
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import pause
import plotly.graph_objects as go
import telegram
import urllib3
from croniter import croniter
from loguru import logger
from model.stats import WeatherData, WeatherStats
from plotting.plots import plot_weather
from utils import escape_telegram_markdown_v2


def get_forecast(key: str, zip_code: str, days: int = 2) -> Optional[WeatherData]:
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


def parse_forecast(raw: WeatherData, max_hrs: int) -> pd.DataFrame:
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


def cache_all(
    st: WeatherStats, df: pd.DataFrame, fig: go.Figure, now: str, out_path: Path
) -> None:
    # Img
    img_path = out_path / f"{now}_weather.png"
    fig.write_image(img_path)
    # Data
    data_path = out_path / f"{now}_weather.parquet"
    df.to_parquet(data_path)
    # Stats
    stats_path = out_path / f"{now}_weather_stats.json"
    with open(stats_path, "w") as f:
        f.write(json.dumps(asdict(st), indent=4, sort_keys=True, default=str))


async def send_photo_to_bot(bot: telegram.Bot, chat_id: int, img_path: Path):
    async with bot:
        await bot.send_photo(chat_id=chat_id, photo=open(img_path, "rb"))  # type: ignore


async def send_msg_to_bot(bot: telegram.Bot, chat_id: int, msg: str):
    async with bot:
        escaped_msg = escape_telegram_markdown_v2(msg)
        logger.debug(f"Sending msg: {escaped_msg}")
        await bot.send_message(
            text=escaped_msg, chat_id=chat_id, parse_mode="MarkdownV2"
        )  # type: ignore


async def run(
    telegram_token: str,
    weather_api_key: str,
    chat_id: int,
    zip_code: str,
    out_dir: Path,
    skip_telegram: bool = False,
):
    # Paths
    out_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now().isoformat()
    # Telegram
    bot = telegram.Bot(telegram_token)
    # Get data
    raw = get_forecast(weather_api_key, zip_code=zip_code)
    if not raw:
        logger.warning("Failed to get forecast")
        return
    # Parse
    hourly = parse_forecast(raw, max_hrs=48)
    stats = WeatherStats.apply(hourly, raw, zip_code)
    msgs = stats.build_msgs()
    logger.info(msgs)
    # Build image
    img_path = out_dir / f"{now}_weather.png"
    fig = plot_weather(hourly, stats)
    cache_all(stats, hourly, fig, now, out_dir)
    # send msg
    if not skip_telegram:
        logger.info(f"Sending to chat id {chat_id}..")
        await send_msg_to_bot(bot, chat_id, "\n".join(msgs))
        await send_photo_to_bot(bot, chat_id, img_path)
    else:
        logger.warning("Skipping telegram")


async def main():
    parser = argparse.ArgumentParser(description="Grab weather, send to telegram")
    parser.add_argument(
        "-c", "--chat", help="Chat ID", required=True, type=int, dest="chat_id"
    )
    parser.add_argument(
        "-z", "--zip", help="ZIP", required=True, type=str, dest="zip_code"
    )
    parser.add_argument(
        "-s", "--cron", help="Crontab schedule", required=True, type=str, dest="cron"
    )
    parser.add_argument(
        "-o", "--out", help="Out dir", required=True, type=str, dest="out_dir"
    )
    parser.add_argument(
        "-f", "--force", help="Send immediately", required=False, action="store_true"
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
        await run(
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
    asyncio.run(main())
