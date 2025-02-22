FROM python:3.11-slim

WORKDIR /app
ENV TZ="America/New_York"

COPY . /app
RUN apt-get update
RUN apt-get upgrade -y
RUN pip3 install poetry
RUN poetry config virtualenvs.create false

RUN poetry install --no-ansi --no-interaction --no-root

# Run
ENV PYTHONPATH=/app
ENTRYPOINT ["python3", "/app/weather_watcher/main.py"]
