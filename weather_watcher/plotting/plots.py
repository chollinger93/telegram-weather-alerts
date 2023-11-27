import pandas as pd
import plotly.graph_objects as go
from model.stats import WeatherStats
from plotly.subplots import make_subplots
from utils import time_to_str


def plot_weather(hourly: pd.DataFrame, st: WeatherStats) -> go.Figure:
    dt_str = list(pd.to_datetime(hourly["time"]).dt.date.unique())[0].strftime(
        "%A, %B %d, %Y"
    )
    heading = f"Weather in {st.meta.location}"
    subtitle = f"{dt_str} from {time_to_str(st.time.from_time)} to {time_to_str(st.time.to_time)}"
    title = f"{heading}<br><sup>{subtitle}</sup>"

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Humidity / Rain
    if st.rain.has_rain:
        fig.add_trace(
            go.Bar(
                x=hourly["time"],
                y=hourly["precip_mm"],
                name="Rainfall (mm)",
                marker=dict(color="blue", opacity=0.5),
            ),
            secondary_y=True,
        )
        fig.update_yaxes(title_text="<b>Rainfall</b> (mm)", secondary_y=True)
    else:
        fig.add_trace(
            go.Scatter(
                x=hourly["time"],
                y=hourly["humidity"],
                name="Humidity",
                opacity=0.5,
                line=dict(color="blue"),
            ),
            secondary_y=True,
        )
        fig.update_yaxes(
            title_text="<b>Humidity</b> (%)", secondary_y=True, range=[20, 100]
        )

    # Temp
    fig.add_trace(
        go.Scatter(
            x=hourly["time"],
            y=hourly["temp_f"],
            name="Temp (F)",
            line=dict(color="darkred"),
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=hourly["time"],
            y=hourly["feelslike_f"],
            name="Feels like (F)",
            line=dict(color="red", dash="dot"),
        ),
        secondary_y=False,
    )
    # Freezing
    hourly["freezing"] = 32.0
    fig.add_trace(
        go.Scatter(
            x=hourly["time"],
            y=hourly["freezing"],
            name="Freezing Point",
            opacity=0.5,
            line=dict(color="red"),
        ),
        secondary_y=False,
    )
    hourly["danger_zone"] = 34.00
    fig.add_trace(
        go.Scatter(
            x=hourly["time"],
            y=hourly["danger_zone"],
            name="Danger Zone",
            opacity=0.5,
            line=dict(color="orange"),
        ),
        secondary_y=False,
    )

    # Sunset/Sunrise
    fig.add_vline(
        x=hourly["sunset"].min().to_pydatetime(),
        line_width=1.5,
        line_color="#FFC000",
        line_dash="dash",
    )
    fig.add_annotation(
        x=hourly["sunset"].min(),
        y=hourly["temp_f"].max() + 5,
        text="Sunset",
        showarrow=False,
    )
    fig.add_vline(
        x=hourly["sunrise"].max(),
        line_width=1.5,
        line_color="#FCF55F",
        line_dash="dash",
    )
    fig.add_annotation(
        x=hourly["sunrise"].min(),
        y=hourly["temp_f"].max() + 5,
        text="Sunrise",
        showarrow=False,
    )

    fig.update_layout(title_text=title)

    # Set y-axes titles
    fig.update_yaxes(title_text="<b>Temperature</b> (F)", secondary_y=False)

    return fig
