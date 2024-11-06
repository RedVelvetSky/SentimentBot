#!/bin/sh

set -e

if [ -f /app/.env ]; then
    set -a
    . /app/.env
    set +a
fi

exec python3 SentimentBot/main.py