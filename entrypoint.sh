#!/usr/bin/env sh
set -e

# start both bots in the background
python bot_test.py &
python bot_consumer.py &

# wait for either to exit
wait
