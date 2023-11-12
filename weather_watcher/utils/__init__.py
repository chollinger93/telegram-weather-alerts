from datetime import datetime


def time_to_str(dt: datetime) -> str:
    return dt.strftime("%I:%M%p")
