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
from dotenv import load_dotenv
load_dotenv()

# === CONFIG ===
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "TELEGRAM_BOT_TOKEN")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY",     "OPENAI_API_KEY")
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

# === Formatting messages ===
# Format fight card
def format_fight_card(fights):
    lines = []
    for f in fights:
        desc = f" ({f['description']})" if f['description'] else ""
        title = f" - {f['title']}" if f['title'] else ""
        line = (
            f"{f['weight_class']} - Winner: <b>{f['winner']}</b> | "
            f"Loser: {f['loser']} | {f['method']}{desc} "
            f"R{f['round']} @ {f['time']}{title}"
        )
        lines.append(line)
    return "\n\n".join(lines)

# Format achievements
def format_achievements(rows):
    return "\n".join([f"{r[0].replace('_', ' ').title()}: {r[1]}" for r in rows])


# === CONVERSATION CONTEXT STORAGE ===
# Store conversation history per user
conversation_contexts = {}

def get_conversation_context(user_id):
    """Get or create conversation context for a user"""
    if user_id not in conversation_contexts:
        conversation_contexts[user_id] = {
            'messages': [],
            'last_query_result': None,
            'last_fighter_mentioned': None
        }
    return conversation_contexts[user_id]

def add_to_conversation_context(user_id, role, content, query_result=None):
    """Add a message to conversation context"""
    context = get_conversation_context(user_id)
    context['messages'].append({
        'role': role,
        'content': content,
        'timestamp': datetime.now().isoformat()
    })
    
    if query_result:
        context['last_query_result'] = query_result
        # Extract fighter names from the result for context
        if isinstance(query_result, list) and query_result:
            for row in query_result:
                if isinstance(row, dict):
                    for key, value in row.items():
                        if 'name' in key.lower() and isinstance(value, str):
                            context['last_fighter_mentioned'] = value
                            break
    
    # Keep only last 10 messages to avoid token limits
    if len(context['messages']) > 10:
        context['messages'] = context['messages'][-10:]

def clear_conversation_context(user_id):
    """Clear conversation context for a user"""
    if user_id in conversation_contexts:
        del conversation_contexts[user_id]

# === SQL GENERATION WITH CONTEXT ===
def get_sql_from_question(question: str, user_id: int) -> str:
    context = get_conversation_context(user_id)
    
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
--   'UFC 315: Muhammad vs. Della Maddalena'     | '2025-05-10' | 'Montreal, Quebec, Canada' | 'http://www.ufcstats.com/event-details/118463dd8427d2f' | '2025-05-24T12:43:40.46921Z'
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
--   wins        INTEGER
--   losses      INTEGER
--   draws       INTEGER
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
"""

    # Build context information
    context_info = ""
    if context['last_fighter_mentioned']:
        context_info += f"Last fighter mentioned: {context['last_fighter_mentioned']}\n"
    
    if context['messages']:
        recent_messages = context['messages'][-3:]  # Last 3 messages for context
        context_info += "Recent conversation:\n"
        for msg in recent_messages:
            context_info += f"- {msg['role']}: {msg['content'][:100]}...\n"

    prompt = f"""
You are a SQL assistant. Here are the schemas and sample rows:

{schema}

{context_info}

Important:
- fight_result is the winner's full name.
- fighters.belt = 'Yes' indicates the fighter is a current champion.   
- To find their division, look up fights.weight_class where fights.championship_fight = 1.   
- fight_stats: each fight generates exactly two rows—one per fighter.   
- round_stats: each round of a fight has two rows—one per fighter per round
- Fighters' wins/losses/draws coloumns are career totals; UFC‐only records come from fights.
- If names don't match exactly, pick the closest or ask for clarification.
- Return exactly one SQL SELECT (PostgreSQL), no fences or explanation.
- When you run queries always run with SELECT * ...
- Pay attention to the conversation context - if the user asks "his last fight" or "when was his last fight", they're referring to the previously mentioned fighter.

User question: {question}
"""
    
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful SQL generator that considers conversation context."},
            {"role": "user",   "content": prompt},
        ],
    )
    raw_sql = resp.choices[0].message.content.strip()
    return re.sub(r"```(?:sql)?\s*", "", raw_sql).replace("```", "").strip()

# === LAST RESULT SENDER ===
def send_last_result_to_chat(chat_id: int, bot_instance: Bot):
    cursor.execute("""
    SELECT "event_name", "location", "date"
    FROM events
    ORDER BY "date" DESC
    LIMIT 1;
    """)
    ev = cursor.fetchone()
    if not ev:
        logging.info("No event found to process.")
        return
    event_name, event_location, event_date = ev

    # Fetch subscribed users
    cursor.execute("""
        SELECT DISTINCT ON (user_id) user_id
        FROM user_actions
        WHERE action = 'subscribe'
        ORDER BY user_id, timestamp DESC;
    """)
    users = [u[0] for u in cursor.fetchall()]

    # Send update to each subscribed user
    event_msg = f"<b>🏟️ Last UFC Event:</b>\nEvent: {event_name}\nLocation: {event_location}\nDate: {event_date}"

    for user_id in users:
        bot.send_message(chat_id=user_id, text=event_msg, parse_mode="HTML")

        # Fetch fight results
        cursor.execute("""
            SELECT
                "weight_class",
                CASE WHEN "fight_result" = 'fighter_1' THEN "fighter_1" ELSE "fighter_2" END AS winner,
                CASE WHEN "fight_result" = 'fighter_1' THEN "fighter_2" ELSE "fighter_1" END AS loser,
                "method", "description", "round", "time",
                CASE
                    WHEN "championship_fight" = '1' AND "weight_class" = 'Open Weight' THEN 'UFC Championship'
                    WHEN "championship_fight" = '1' THEN "weight_class" || ' Championship Fight'
                    ELSE NULL
                END AS title
            FROM fights
            WHERE event_name = %s
            ORDER BY fight_id DESC;
        """, (event_name,))
        fight_results = cursor.fetchall()

        fight_card_data = [
            {
                "weight_class": f[0],
                "winner": f[1],
                "loser": f[2],
                "method": f[3],
                "description": f[4],
                "round": f[5],
                "time": f[6],
                "title": f[7]
            }
            for f in fight_results
        ]

        fight_card_msg = "<b>🥊 Fight Card Results:</b>\n" + format_fight_card(fight_card_data)
        bot.send_message(chat_id=user_id, text=fight_card_msg, parse_mode="HTML")

        # Fetch achievements
        cursor.execute("""
            SELECT
            'Performance of the Night'    AS type,
            fighter_1 || ' vs ' || fighter_2 AS matchup
            FROM fights
            WHERE performance_of_the_night = '1'
            AND event_name = %s

            UNION ALL

            SELECT
            'KO of the Night'    AS type,
            fighter_1 || ' vs ' || fighter_2 AS matchup
            FROM fights
            WHERE ko_of_the_night = '1'
            AND event_name = %s

            UNION ALL

            SELECT
            'Submission of the Night'    AS type,
            fighter_1 || ' vs ' || fighter_2 AS matchup
            FROM fights
            WHERE sub_of_the_night = '1'
            AND event_name = %s;
        """, (event_name, event_name, event_name))
        achievements = cursor.fetchall()

        if achievements:
            # format_achievements will now get [(type,matchup), ...]
            achievements_msg = "<b>🏆 Event Highlights:</b>\n" + format_achievements(achievements)
            bot.send_message(chat_id=user_id, text=achievements_msg, parse_mode="HTML")

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
    user_id = update.effective_user.id
    clear_conversation_context(user_id)  # Clear any existing conversation
    
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

def done(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    context.user_data['in_conversation'] = False
    clear_conversation_context(user_id)  # Clear conversation context
    
    update.message.reply_text(
        "Conversation ended. What would you like to do next?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💥 Ask a question", callback_data='gpt')],
            [InlineKeyboardButton("📊 Get last results", callback_data='last')],
            [
                InlineKeyboardButton("🔔 Subscribe",   callback_data='sub'),
                InlineKeyboardButton("🔕 Unsubscribe", callback_data='unsub'),
            ],
        ])
    )

def button_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    data = query.data
    user_id = query.from_user.id

    if data == 'last':
        send_last_result_to_chat(query.message.chat_id, context.bot)

    elif data == 'gpt':
        clear_conversation_context(user_id)  # Start fresh conversation
        query.message.reply_text("Sure—what would you like to ask? (Type /done to end the conversation)")
        context.user_data['in_conversation'] = True

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

def handle_message(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    
    if context.user_data.get('in_conversation', False):
        question = update.message.text
        chat_id = update.effective_chat.id

        # Add user question to conversation context
        add_to_conversation_context(user_id, 'user', question)

        try:
            sql = get_sql_from_question(question, user_id)
            logger.info(f"Executing SQL: {sql}")
            cursor.execute(sql)
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()

            if not rows:
                text = "No results found."
                add_to_conversation_context(user_id, 'assistant', text)
            else:
                table = [dict(zip(cols, r)) for r in rows]
                
                # Add query result to context
                add_to_conversation_context(user_id, 'assistant', '', query_result=table)
                
                # Get conversation context for GPT
                conv_context = get_conversation_context(user_id)
                context_messages = [
                    {"role": "system", "content": "Summarize these records in plain English. Consider the conversation context to provide relevant answers."}
                ]
                
                # Add recent conversation messages for context
                for msg in conv_context['messages'][-5:]:  # Last 5 messages
                    if msg['content'].strip():  # Only add non-empty messages
                        context_messages.append({
                            "role": msg['role'],
                            "content": msg['content']
                        })
                
                context_messages.append({
                    "role": "user",
                    "content": f"Data: {table}\nCurrent question: {question}"
                })
                
                resp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=context_messages
                )
                text = resp.choices[0].message.content.strip()
                
                # Update the assistant's response in context
                conv_context['messages'][-1]['content'] = text

        except Exception as e:
            conn.rollback()
            logger.error(e)
            text = f"Sorry, an error occurred: {e}"
            add_to_conversation_context(user_id, 'assistant', text)

        context.bot.send_message(chat_id=chat_id, text=text)
    else:
        start(update, context)

def main():
    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler('start', start))
    dp.add_handler(CommandHandler('done', done))
    dp.add_handler(CallbackQueryHandler(button_handler))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    logger.info("Bot started; polling…")
    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()