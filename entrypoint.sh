#!/usr/bin/env sh
set -e

python bot_main.py &
MAIN_PID=$!

python bot_consumer.py &
CONSUMER_PID=$!

# Wait for both to exit
wait $MAIN_PID
wait $CONSUMER_PID
