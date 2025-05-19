from telegram.ext import Updater, CommandHandler
from kafka import KafkaProducer
import logging
import json
from datetime import datetime

# pip install python-telegram-bot==13.7

# Basic config
TOKEN = "7481539143:AAHnfuFMW-Qn_yvMBbZsh-p3odqtajXfdYc"
KAFKA_SERVER = 'kafka:9092'
KAFKA_TOPIC = 'ufc'

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set up Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

def send_subscription_status(update, context, subscribe):
    """Send subscription/unsubscription event to Kafka."""
    user_info = {
        "user_id": update.message.chat_id,
        "username": update.effective_user.username,
        "action": "subscribe" if subscribe else "unsubscribe",
        "timestamp": datetime.now().isoformat(),
        "subscribe": int(subscribe)
    }
    producer.send(KAFKA_TOPIC, json.dumps(user_info).encode('utf-8'))
    logger.info(f"Sent to Kafka: {user_info}")


def start(update, context):
    update.message.reply_text(
        "Welcome to the UFC Tracker Bot! Use /subscribe to get UFC Events updates, or /unsubscribe to stop."
    )

def subscribe(update, context):
    send_subscription_status(update, context, subscribe=True)
    update.message.reply_text("You've subscribed! 👊")

def unsubscribe(update, context):
    send_subscription_status(update, context, subscribe=False)
    update.message.reply_text("You've unsubscribed. 😵")

def main():
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("subscribe", subscribe))
    dp.add_handler(CommandHandler("unsubscribe", unsubscribe))

    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()
