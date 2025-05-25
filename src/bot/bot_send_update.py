import datetime
import psycopg2
import logging
import s3fs
from telegram import Bot

# Set up logging
logging.basicConfig(level=logging.INFO)

# S3 filesystem configuration for log & state
S3_BUCKET = 'ufc'
LOG_PATH = f'{S3_BUCKET}/logs/bot_update_log.log'
STATE_PATH = f'{S3_BUCKET}/logs/bot_update_state.log'

S3_OPTS = {
    'key': 'minioadmin',
    'secret': 'minioadmin',
    'client_kwargs': {'endpoint_url': 'http://minio-dev:9000'}
}
fs = s3fs.S3FileSystem(**S3_OPTS)

# Telegram Bot Setup
TELEGRAM_BOT_TOKEN = "7981064950:AAGNJIoxIwDhzAYN6eiXhZnitXH4K8m4iC4"
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Connect to PostgreSQL
def connect_db():
    try:
        conn = psycopg2.connect(
            host='postgres_db',
            port='5432',
            database='ufc_db',
            user='postgres',
            password='postgres'
        )
        logging.info("Connected to the database.")
        return conn, conn.cursor()
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        exit(1)

# Append events to S3 log and update state

def update_log_and_state(events):
    last_line = None
    with fs.open(LOG_PATH, mode='a') as log_file:
        for ev in events:
            timestamp = datetime.datetime.utcnow().isoformat()
            line = f"{timestamp},{ev['event_name']},{ev['date']}\n"
            log_file.write(line)
            last_line = line
    if last_line:
        with fs.open(STATE_PATH, mode='w') as state_file:
            state_file.write(last_line)
    return last_line

# Format fight card for Telegram message

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

# Format achievements for Telegram message

def format_achievements(rows):
    return "\n".join([f"{r[0].replace('_', ' ').title()}: {r[1]}" for r in rows])

# Main function to send updates conditionally

def send_last_event_update(cursor):
    try:
        # Fetch latest event
        cursor.execute(
            """
            SELECT "event_name", "location", "date"
            FROM events
            ORDER BY "date" DESC
            LIMIT 1;
            """
        )
        ev = cursor.fetchone()
        if not ev:
            logging.info("No event found to process.")
            return
        event_name, event_location, event_date = ev

        # Read last sent state (if exists)
        last_state = None
        if fs.exists(STATE_PATH):
            with fs.open(STATE_PATH, mode='r') as state_file:
                line = state_file.read().strip()
                parts = line.split(',', 2)
                if len(parts) >= 2:
                    last_state = parts[1]

        # Skip if already sent
        if last_state == event_name:
            logging.info(f"No new event: '{event_name}' already sent.")
            return

        # Log and update state
        update_log_and_state([{
            'event_name': event_name,
            'date': (event_date.isoformat() if hasattr(event_date, 'isoformat') else str(event_date))
        }])

        # Fetch subscribed users
        cursor.execute(
            """
            SELECT DISTINCT ON (user_id) user_id
            FROM user_actions
            WHERE action = 'subscribe'
            ORDER BY user_id, timestamp DESC;
            """
        )
        users = [u[0] for u in cursor.fetchall()]

        # Prepare and send messages
        event_msg = (
            f"<b>🏟️ Last UFC Event:</b>\n"
            f"Event: {event_name}\n"
            f"Location: {event_location}\n"
            f"Date: {event_date}"
        )

        for user_id in users:
            bot.send_message(chat_id=user_id, text=event_msg, parse_mode="HTML")

            # Fetch fight results
            cursor.execute(
                """
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
                """, (event_name,)
            )
            fights = cursor.fetchall()
            fight_card_data = [
                {
                    'weight_class': f[0],
                    'winner': f[1],
                    'loser': f[2],
                    'method': f[3],
                    'description': f[4],
                    'round': f[5],
                    'time': f[6],
                    'title': f[7]
                }
                for f in fights
            ]
            bot.send_message(
                chat_id=user_id,
                text="<b>🥊 Fight Card Results:</b>\n" + format_fight_card(fight_card_data),
                parse_mode="HTML"
            )

            # Fetch achievements
            cursor.execute(
                """
                SELECT 'Performance of the Night' AS type, fighter_1 || ' vs ' || fighter_2 AS matchup
                FROM fights WHERE performance_of_the_night = '1' AND event_name = %s
                UNION ALL
                SELECT 'KO of the Night', fighter_1 || ' vs ' || fighter_2
                FROM fights WHERE ko_of_the_night = '1' AND event_name = %s
                UNION ALL
                SELECT 'Submission of the Night', fighter_1 || ' vs ' || fighter_2
                FROM fights WHERE sub_of_the_night = '1' AND event_name = %s;
                """, (event_name, event_name, event_name)
            )
            achievements = cursor.fetchall()
            if achievements:
                bot.send_message(
                    chat_id=user_id,
                    text="<b>🏆 Event Highlights:</b>\n" + format_achievements(achievements),
                    parse_mode="HTML"
                )

            logging.info(f"All updates sent to user: {user_id}")

    except Exception as e:
        logging.error(f"Error during message sending: {e}")


if __name__ == '__main__':
    conn, cursor = connect_db()
    send_last_event_update(cursor)
    cursor.close()
    conn.close()
    logging.info("Database connection closed.")
