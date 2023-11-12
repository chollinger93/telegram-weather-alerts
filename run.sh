#!/bin/bash

set -eo pipefail

if [[ -z $WEATHER_API_KEY || -z $TELEGRAM_TOKEN || -z $TELEGRAM_CHAT_ID || -z $WEATHER_ZIP_CODE ]]; then
    echo "Usage: WEATHER_API_KEY=??? TELEGRAM_TOKEN=??? \$TELEGRAM_CHAT_ID \$WEATHER_ZIP_CODE run.sh"
    exit 1
fi

mkdir -p data/
docker-compose up -d
