import re
from datetime import datetime


def time_to_str(dt: datetime) -> str:
    return dt.strftime("%I:%M%p")


def escape_telegram_markdown_v2(text: str) -> str:
    special_chars = r"[]()~`>#+-=|{}.!"
    return re.sub(rf"([{re.escape(special_chars)}])", r"\\\1", text)
