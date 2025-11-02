import json
from abc import ABC, abstractmethod
from dataclasses import asdict
from pathlib import Path


from weather_watcher.model.stats import WeatherStats


class Sink(ABC):
    @abstractmethod
    def sink(self, st: WeatherStats, now: str):
        pass


class ParquetSink(Sink):
    def __init__(self, out_path: Path):
        self.out_path = out_path

    def sink(self, st: WeatherStats, now: str):
        # Data
        data_path = self.out_path / f"{now}_weather.parquet"
        st.raw_df.to_parquet(data_path)


class StatsJSONSink(Sink):
    def __init__(self, out_path: Path):
        self.out_path = out_path

    def sink(self, st: WeatherStats, now: str):
        # Stats
        stats_path = self.out_path / f"{now}_weather_stats.json"
        with open(stats_path, "w") as f:
            f.write(json.dumps(asdict(st), indent=4, sort_keys=True, default=str))


class FigureSink(Sink):
    def __init__(self, out_path: Path):
        self.out_path = out_path

    def sink(self, st: WeatherStats, now: str):
        # Img
        fig = st.plot_weather()
        img_path = self.out_path / f"{now}_weather.png"
        fig.write_image(img_path)
