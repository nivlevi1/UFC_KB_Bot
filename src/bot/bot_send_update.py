import psycopg2
from telegram import Bot
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Telegram Bot Setup
TELEGRAM_BOT_TOKEN = "7481539143:AAHnfuFMW-Qn_yvMBbZsh-p3odqtajXfdYc"
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        host='postgres_db',
        port='5432',
        database='ufc_db',
        user='postgres',
        password='postgres'
    )
    cursor = conn.cursor()
    logging.info("Connected to the database.")
except Exception as e:
    logging.error(f"Database connection failed: {e}")
    exit(1)

# Format fight card with bold winner name and line breaks between fights
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

# Main function to send updates
def send_last_event_update():
    try:
        cursor.execute("""
            SELECT DISTINCT ON (user_id) user_id, action
            FROM user_actions
            ORDER BY user_id, timestamp DESC;
        """)
        users = cursor.fetchall()

        for user_id, action in users:
            if action != 'subscribe':
                continue

            # 1. Last Event Info
            cursor.execute("""
                SELECT "event_name", "location"
                FROM events
                ORDER BY "date" DESC
                LIMIT 1;
            """)
            event = cursor.fetchone()
            if not event:
                continue

            event_msg = f"<b>🏟️ Last UFC Event:</b>\nEvent: {event[0]}\nLocation: {event[1]}"
            bot.send_message(chat_id=user_id, text=event_msg, parse_mode="HTML")

            # 2. Fight Card Results
            cursor.execute("""
                SELECT
                "weight_class",
                CASE 
                    WHEN "fight_result" = "fighter_1" THEN "fighter_1"
                    ELSE "fighter_2"
                END AS winner,
                CASE 
                    WHEN "fight_result" = "fighter_1" THEN "fighter_2"
                    ELSE "fighter_1"
                END AS loser,
                "method",
                "description",
                "round",
                "time",
                CASE 
                    WHEN "championship_fight" = '1' AND "weight_class" = 'Open Weight' THEN 'UFC Championship'
                    WHEN "championship_fight" = '1' AND "weight_class" != 'Open Weight' THEN "weight_class" || ' Championship Fight'
                    ELSE NULL
                END AS title
                FROM fights
                WHERE event_name = %s
                ORDER BY fight_id DESC;
            """, (event[0],))
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

            # 3. Achievements
            cursor.execute("""
                SELECT 'Performance of the Night' AS type, "fighter_1" || ' vs ' || "fighter_2" AS fight
                FROM fights
                WHERE "performance_of_the_night" = '1' AND "event_name" = %s
                UNION ALL
                SELECT 'KO of the Night', "fighter_1" || ' vs ' || "fighter_2"
                FROM fights
                WHERE "ko_of_the_night" = '1' AND "event_name" = %s
                UNION ALL
                SELECT 'Submission of the Night', "fighter_1" || ' vs ' || "fighter_2"
                FROM fights
                WHERE "sub_of_the_night" = '1' AND "event_name" = %s;
            """, (event[0], event[0], event[0]))
            achievements = cursor.fetchall()

            if achievements:
                achievements_msg = "<b>🏆 Event Highlights:</b>\n" + format_achievements(achievements)
                bot.send_message(chat_id=user_id, text=achievements_msg, parse_mode="HTML")

            logging.info(f"All updates sent to user: {user_id}")

    except Exception as e:
        logging.error(f"Error during message sending: {e}")

# Run the function
send_last_event_update()

# Clean up
cursor.close()
conn.close()
logging.info("Database connection closed.")
