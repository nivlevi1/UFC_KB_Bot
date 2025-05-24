import os
import re
import logging
import json
from datetime import datetime

from openai import OpenAI
import psycopg2
from kafka import KafkaProducer

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackContext,
    CallbackQueryHandler,
)


# === CONFIG ===
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7981064950:AAGNJIoxIwDhzAYN6eiXhZnitXH4K8m4iC4")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY",     "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")
DB_PARAMS = dict(
    host="postgres_db",
    port="5432",
    database="ufc_db",
    user="postgres",
    password="postgres"
)
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC",  "ufc")


# === SETUP ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)

# PostgreSQL connection
try:
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    cursor = conn.cursor()
    logger.info("Connected to PostgreSQL database.")
except Exception as e:
    logger.error(f"PostgreSQL connection failed: {e}")
    exit(1)

# Kafka producer for subscription events
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

bot = Bot(token=TELEGRAM_BOT_TOKEN)


# === SQL GENERATION ===
def get_sql_from_question(question: str) -> str:
    schema = """
-- Table: events
-- Columns:
--   event_name    TEXT
--   date          DATE
--   location      TEXT
--   link          TEXT
--   last_updated  TIMESTAMP WITHOUT TIME ZONE
-- Sample rows:
--   'UFC Fight Night: Sandhagen vs. Figueiredo' | '2025-05-03' | 'Des Moines, Iowa, USA'    | 'http://www.ufcstats.com/event-details/de20ffb3fc2e7629' | '2025-05-24T12:43:40.46921Z'
--   'UFC 315: Muhammad vs. Della Maddalena'     | '2025-05-10' | 'Montreal, Quebec, Canada' | 'http://www.ufcstats.com/event-details/118463dd8db16e7f' | '2025-05-24T12:43:40.46921Z'
--   'UFC Fight Night: Burns vs. Morales'        | '2025-05-17' | 'Las Vegas, Nevada, USA'   | 'http://www.ufcstats.com/event-details/8ad022dd81224f61' | '2025-05-24T12:43:40.46921Z'

-- Table: fighters
-- Columns:
--   first_name  TEXT
--   last_name   TEXT
--   nickname    TEXT
--   height      TEXT
--   weight      TEXT
--   reach       DOUBLE PRECISION
--   stance      TEXT
--   wins           INTEGER
--   losses           INTEGER
--   draws           INTEGER
--   belt        TEXT
--   link        TEXT
--   last_run    TIMESTAMP WITHOUT TIME ZONE
-- Sample rows:
--   'Magomed' | 'Ankalaev'    | ''                | '6'' 3"'     | '205 lbs.' |  75.0 | 'Orthodox' | 21 | 1 | 1 | 'Yes' | 'http://ufcstats.com/fighter-details/d802174b0c0c1f4e' | '2025-05-24T12:43:40.46921Z'
--   'Tom'     | 'Aspinall'    | ''                | '6'' 5"'     | '256 lbs.' |  78.0 | 'Orthodox' | 15 | 3 | 0 | 'Yes' | 'http://ufcstats.com/fighter-details/399afbabc02376b5' | '2025-05-24T12:43:40.46921Z'
--   'Ben'     | 'Askren'      | 'Funky'           | '5'' 11"'    | '170 lbs.' |  73.0 | 'Orthodox' | 19 | 2 | 0 | ''    | 'http://ufcstats.com/fighter-details/0b31f87be71ebbb1' | '2025-05-24T12:43:40.46921Z'
--   'Ibo'     | 'Aslan'       | 'The Last Ottoman'| '6'' 3"'     | '205 lbs.' |  77.0 | 'Orthodox' | 14 | 2 | 0 | ''    | 'http://ufcstats.com/fighter-details/6e8f1bcdda94ff45' | '2025-05-24T12:43:40.46921Z'

-- Table: fights
-- Columns:
--   event_name               TEXT
--   fight_id                 INTEGER
--   fight_result             TEXT
--   fighter_1                TEXT
--   fighter_2                TEXT
--   weight_class             TEXT
--   method                   TEXT
--   description              TEXT
--   round                    INTEGER
--   time                     TEXT
--   championship_fight       INTEGER
--   fight_of_the_night       INTEGER
--   performance_of_the_night INTEGER
--   sub_of_the_night         INTEGER
--   ko_of_the_night          INTEGER
--   fight_stats_link         TEXT
--   last_run                 TIMESTAMP WITHOUT TIME ZONE
-- Sample rows:
--   'UFC Fight Night: Burns vs. Morales' | 1 | 'Michael Morales' | 'Michael Morales' | 'Gilbert Burns' | 'Welterweight' | 'KO/TKO' | 'Punches' | 1 | '3:39' | 0 | 0 | 1 | 0 | 0 | 'http://www.ufcstats.com/fight-details/a81ad236a2f221f2' | '2025-05-24T12:43:40.46921Z'
--   'UFC Fight Night: Burns vs. Morales' | 2 | 'Mairon Santos'   | 'Mairon Santos'   | 'Sodiq Yusuff'  | 'Lightweight'  | 'U-DEC'  | ''        | 3 | '5:00' | 0 | 0 | 0 | 0 | 0 | 'http://www.ufcstats.com/fight-details/dcb27eef6441268c' | '2025-05-24T12:43:40.46921Z'
--   'UFC Fight Night: Burns vs. Morales' | 3 | 'Nursulton Ruziboev' | 'Dustin Stoltzfus' | 'Nursulton Ruziboev' | 'Middleweight' | 'U-DEC' | ''     | 3 | '5:00' | 0 | 0 | 0 | 0 | 0 | 'http://www.ufcstats.com/fight-details/b3044bf646306e7f' | '2025-05-24T12:43:40.46921Z'

-- Table: fight_stats
-- Columns:
--   event                             TEXT
--   fight_id                          INTEGER
--   fighter                           TEXT
--   knockdowns                        INTEGER
--   significant_strikes               TEXT
--   significant_strike_accuracy_pct   TEXT
--   total_strikes                     TEXT
--   takedowns                         TEXT
--   takedown_accuracy_pct             TEXT
--   submission_attempts               INTEGER
--   reversals                         INTEGER
--   control_time                      TEXT
--   significant_head_strikes          TEXT
--   significant_body_strikes          TEXT
--   significant_leg_strikes           TEXT
--   distance_strikes                  TEXT
--   clinch_strikes                    TEXT
--   ground_strikes                    TEXT
--   last_run                          TIMESTAMP WITHOUT TIME ZONE
-- Sample rows:
--   'UFC Fight Night: Burns vs. Morales' | 1 | 'Gilbert Burns'       | 0 | '5 of 18'  | '27%' | '5 of 18' | '1 of 2' | '50%' | 0 | 0 | '0:30' | '4 of 13' | '0 of 3' | '1 of 2' | '5 of 18' | '0 of 0' | '0 of 0' | '2025-05-24T12:43:40.46921Z'
--   'UFC Fight Night: Burns vs. Morales' | 1 | 'Michael Morales'     | 2 | '33 of 56' | '58%' | '35 of 58'| '0 of 0' | ''    | 0 | 0 | '0:10' | '31 of 52'| '0 of 0' | '2 of 4' | '25 of 44'| '2 of 3' | '6 of 9' | '2025-05-24T12:43:40.46921Z'
--   'UFC Fight Night: Burns vs. Morales' | 2 | 'Sodiq Yusuff'        | 0 | '36 of 86' | '41%' | '60 of 115'| '1 of 3'| '33%'| 0 | 0 | '2:02' | '9 of 44' | '9 of 19'| '18 of 23'| '35 of 84'| '1 of 2' | '0 of 0' | '2025-05-24T12:43:40.46921Z'
--   'UFC Fight Night: Burns vs. Morales' | 2 | 'Mairon Santos'      | 0 | '40 of 83' | '48%' | '71 of 114'| '0 of 0'| ''   | 0 | 0 | '1:57' | '22 of 56'| '10 of 18'| '8 of 9'  | '35 of 77'| '5 of 6'  | '0 of 0' | '2025-05-24T12:43:40.46921Z'
--   'UFC Fight Night: Burns vs. Morales' | 3 | 'Dustin Stoltzfus'   | 0 | '21 of 37' | '56%' | '41 of 62'| '2 of 5' | '40%'| 2 | 0 | '5:11' | '5 of 19' | '4 of 6' | '12 of 12'| '19 of 34'| '1 of 2'  | '1 of 1' | '2025-05-24T12:43:40.46921Z'
--   'UFC Fight Night: Burns vs. Morales' | 3 | 'Nursulton Ruziboev'  | 0 | '35 of 68' | '51%' | '53 of 94'| '1 of 2' | '50%'| 0 | 2 | '3:21' | '24 of 53'| '8 of 11'| '3 of 4'  | '24 of 52'| '1 of 1'  | '10 of 15'| '2025-05-24T12:43:40.46921Z'

-- Table: round_stats
-- Columns:
--   event                             TEXT
--   fight_id                          INTEGER
--   round                             TEXT
--   fighter                           TEXT
--   knockdowns                        INTEGER
--   significant_strikes               TEXT
--   significant_strike_accuracy_pct   TEXT
--   total_strikes                     TEXT
--   takedowns                         TEXT
--   takedown_accuracy_pct             TEXT
--   submission_attempts               INTEGER
--   reversals                         INTEGER
--   control_time                      TEXT
--   significant_head_strikes          TEXT
--   significant_body_strikes          TEXT
--   significant_leg_strikes           TEXT
--   distance_strikes                  TEXT
--   clinch_strikes                    TEXT
--   ground_strikes                    TEXT
--   last_run                          TIMESTAMP WITHOUT TIME ZONE
-- Sample rows:
--   ('UFC Fight Night: Burns vs. Morales', 1, 'Round 1', 'Gilbert Burns',    0, '5 of 18', '27%', '5 of 18', '1 of 2', '50%', 0, 0, '0:30', '4 of 13', '0 of 3',  '1 of 2',  '5 of 18', '0 of 0', '0 of 0', '2025-05-24T12:43:40.46921Z')
--   ('UFC Fight Night: Burns vs. Morales', 1, 'Round 1', 'Michael Morales',  2, '33 of 56','58%', '35 of 58','0 of 0',  '',   0, 0, '0:10', '31 of 52','0 of 0',  '2 of 4',  '25 of 44','2 of 3',  '6 of 9', '2025-05-24T12:43:40.46921Z')
--   ('UFC Fight Night: Burns vs. Morales', 2, 'Round 1', 'Sodiq Yusuff',     0, '19 of 30','63%', '19 of 30','0 of 1', '0%',  0, 0, '0:42', '4 of 11', '7 of 10',  '8 of 9',  '18 of 29','1 of 1',  '0 of 0', '2025-05-24T12:43:40.46921Z')
--   ('UFC Fight Night: Burns vs. Morales', 2, 'Round 1', 'Mairon Santos',    0, '9 of 27', '33%', '9 of 27', '0 of 0',  '',   0, 0, '0:03', '4 of 18', '2 of 5',   '3 of 4',  '8 of 26', '1 of 1',  '0 of 0', '2025-05-24T12:43:40.46921Z')
--   ('UFC Fight Night: Burns vs. Morales', 2, 'Round 2', 'Sodiq Yusuff',     0, '10 of 26','38%', '29 of 48','0 of 1', '0%',  0, 0, '0:01', '3 of 15', '2 of 6',   '5 of 5',  '10 of 25','0 of 1',  '0 of 0', '2025-05-24T12:43:40.46921Z')
--   ('UFC Fight Night: Burns vs. Morales', 2, 'Round 2', 'Mairon Santos',    0, '16 of 27','59%', '26 of 37','0 of 0',  '',   0, 0, '1:35', '11 of 21','4 of 5',   '1 of 1',  '12 of 23','4 of 4',  '0 of 0', '2025-05-24T12:43:40.46921Z')
--   ('UFC Fight Night: Burns vs. Morales', 2, 'Round 3', 'Sodiq Yusuff',     0, '7 of 30', '23%', '12 of 37','1 of 1', '100%',0, 0, '1:19', '2 of 18', '0 of 3',   '5 of 9',  '7 of 30', '0 of 0',  '0 of 0', '2025-05-24T12:43:40.46921Z')
--   ('UFC Fight Night: Burns vs. Morales', 2, 'Round 3', 'Mairon Santos',    0, '15 of 29','51%', '36 of 50','0 of 0',  '',   0, 0, '0:19', '7 of 17', '4 of 8',   '4 of 4',  '15 of 28','0 of 1',  '0 of 0', '2025-05-24T12:43:40.46921Z')
--   ('UFC Fight Night: Burns vs. Morales', 3, 'Round 1', 'Dustin Stoltzfus', 0, '5 of 13','38%', '10 of 18','0 of 1', '0%',  1, 0, '1:14', '2 of 10', '1 of 1',   '2 of 2',  '5 of 13', '0 of 0',  '0 of 0', '2025-05-24T12:43:40.46921Z')
--   ('UFC Fight Night: Burns vs. Morales', 3, 'Round 1', 'Nursulton Ruziboev',0, '13 of 27','48%', '21 of 36','1 of 1', '100%',0, 1, '1:24', '8 of 19', '2 of 4',   '3 of 4',  '11 of 25','1 of 1',  '1 of 1', '2025-05-24T12:43:40.46921Z')
--   ('UFC Fight Night: Burns vs. Morales', 3, 'Round 2', 'Dustin Stoltzfus', 0, '2 of 3', '66%', '14 of 18','1 of 2', '50%', 0, 0, '2:13', '1 of 2',  '1 of 1',   '0 of 0',  '1 of 2',  '0 of 0',  '1 of 1', '2025-05-24T12:43:40.46921Z')
--   ('UFC Fight Night: Burns vs. Morales', 3, 'Round 2', 'Nursulton Ruziboev',0, '11 of 19','57%', '21 of 35','0 of 0',  '',   0, 1, '1:55', '9 of 17',  '2 of 2',   '0 of 0',  '2 of 5',  '0 of 0',  '9 of 14','2025-05-24T12:43:40.46921Z')
--   ('UFC Fight Night: Burns vs. Morales', 3, 'Round 3', 'Dustin Stoltzfus', 0, '14 of 21','66%', '17 of 26','1 of 2', '50%', 1, 0, '1:44', '2 of 7',   '2 of 4',   '10 of 10', '13 of 19','1 of 2',  '0 of 0', '2025-05-24T12:43:40.46921Z')
--   ('UFC Fight Night: Burns vs. Morales', 3, 'Round 3', 'Nursulton Ruziboev',0, '11 of 22','50%', '11 of 23','0 of 1', '0%',  0, 0, '0:02', '7 of 17',  '4 of 5',   '0 of 0',  '11 of 22','0 of 0',  '0 of 0', '2025-05-24T12:43:40.46921Z')
"""
    prompt = f"""
You are a SQL assistant. Here are the schemas and sample rows:

{schema}

Important:
- `fight_result` is the winner’s full name.
- fighters.belt = 'Yes' indicates the fighter is a current champion.   
- To find their division, look up fights.weight_class where fights.championship_fight = 1.   
- fight_stats: each fight generates exactly two rows—one per fighter.   
- round_stats: each round of a fight has two rows—one per fighter per roun
- Fighters’ W/L/D are career totals; UFC‐only records come from `fights`.
- If names don’t match exactly, pick the closest or ask for clarification.
- Return exactly one SQL SELECT (PostgreSQL), no fences or explanation.
- When you run quries always run with SELECT * ...

User question: {question}
"""
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful SQL generator."},
            {"role": "user",   "content": prompt},
        ],
    )
    raw_sql = resp.choices[0].message.content.strip()
    return re.sub(r"```(?:sql)?\s*", "", raw_sql).replace("```", "").strip()


# === LAST RESULT SENDER ===
def send_last_result_to_chat(chat_id: int, bot_instance: Bot):
    cursor.execute("""
        SELECT event_name, location
        FROM events
        ORDER BY date DESC
        LIMIT 1;
    """)
    ev = cursor.fetchone()
    if not ev:
        bot_instance.send_message(chat_id, "No event found.")
        return

    name, loc = ev
    bot_instance.send_message(
        chat_id,
        f"<b>🏟️ Last UFC Event:</b>\nEvent: {name}\nLocation: {loc}",
        parse_mode="HTML"
    )

    cursor.execute("""
        SELECT weight_class,
               CASE WHEN fight_result=fighter_1 THEN fighter_1 ELSE fighter_2 END AS winner,
               CASE WHEN fight_result=fighter_1 THEN fighter_2 ELSE fighter_1 END AS loser,
               method, description, round, time
        FROM fights
        WHERE event_name=%s
        ORDER BY fight_id;
    """, (name,))
    fights = cursor.fetchall()
    lines = [
        f"{wc}: {win} def. {lose}, {m} ({d}), R{r} {t}"
        for wc, win, lose, m, d, r, t in fights
    ]
    bot_instance.send_message(
        chat_id,
        "<b>🥊 Fight Results:</b>\n" + "\n".join(lines),
        parse_mode="HTML"
    )


# === LIVE‐UPDATES SUBSCRIPTION ===
def send_subscription_status(user_id, username, subscribe: bool):
    info = {
        "user_id": user_id,
        "username": username or "",
        "action": "subscribe" if subscribe else "unsubscribe",
        "timestamp": datetime.now().isoformat(),
        "subscribe": int(subscribe)
    }
    producer.send(KAFKA_TOPIC, json.dumps(info).encode('utf-8'))
    logger.info(f"Sent to Kafka: {info}")


# === BOT HANDLERS ===
def start(update: Update, context: CallbackContext):
    keyboard = [
        [InlineKeyboardButton("💥 Ask a question", callback_data='gpt')],
        [InlineKeyboardButton("📊 Get last results", callback_data='last')],
        [
            InlineKeyboardButton("🔔 Subscribe",   callback_data='sub'),
            InlineKeyboardButton("🔕 Unsubscribe", callback_data='unsub'),
        ],
    ]
    update.message.reply_text(
        "Welcome! What would you like to do?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


def choose_action(update: Update, context: CallbackContext):
    start(update, context)


def button_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    data = query.data

    if data == 'last':
        send_last_result_to_chat(query.message.chat_id, context.bot)

    elif data == 'gpt':
        query.message.reply_text("Sure—what would you like to ask?")
        context.user_data['awaiting_gpt'] = True

    elif data in ('sub', 'unsub'):
        user = query.from_user
        send_subscription_status(
            user_id=user.id,
            username=user.username,
            subscribe=(data == 'sub')
        )
        reply = "You've subscribed! 👊" if data == 'sub' else "You've unsubscribed. 😵"
        query.message.reply_text(reply)

    else:
        query.message.reply_text("Sorry, I didn't understand that.")


def handle_followup(update: Update, context: CallbackContext):
    if context.user_data.pop('awaiting_gpt', False):
        question = update.message.text
        chat_id = update.effective_chat.id

        try:
            sql = get_sql_from_question(question)
            logger.info(f"Executing SQL: {sql}")
            cursor.execute(sql)
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()

            if not rows:
                text = "No results found."
            else:
                table = [dict(zip(cols, r)) for r in rows]
                resp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role":"system","content":"Summarize these records in plain English."},
                        {"role":"user",  "content":f"Data: {table}\nQuestion: {question}"}
                    ]
                )
                text = resp.choices[0].message.content.strip()

        except Exception as e:
            conn.rollback()
            logger.error(e)
            text = f"Sorry, an error occurred: {e}"

        context.bot.send_message(chat_id=chat_id, text=text)
    else:
        choose_action(update, context)


def main():
    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler('start', start))
    dp.add_handler(CallbackQueryHandler(button_handler))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_followup))

    logger.info("Bot started; polling…")
    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()
