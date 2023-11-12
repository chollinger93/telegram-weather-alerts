FROM python:3.11-slim

WORKDIR /app
ENV TZ="America/New_York"

COPY . /app
RUN apt-get update 
RUN apt-get upgrade -y
RUN pip3 install poetry
RUN poetry config virtualenvs.create false

RUN poetry install --no-dev --no-ansi --no-interaction 

# Run
ENTRYPOINT ["python3", "/app/weather_watcher/main.py"]
