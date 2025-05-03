from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import json
from datetime import datetime
import s3fs

#S3 filesystem
S3_BUCKET = 'ufc'
LOG_PATH   = f'{S3_BUCKET}/logs/ufc_fight_scrape.log'
S3_OPTS    = {
    'key': 'minioadmin',
    'secret': 'minioadmin',
    'client_kwargs': {'endpoint_url': 'http://minio-dev:9000'}
}

def main():
    # start timer
    start_time = time.time()

    # URL of the webpage to scrape
    url = 'http://www.ufcstats.com/statistics/events/completed?page=all'
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        tbody = soup.find('tbody')

        if tbody:
            # Extract all rows
            rows = tbody.find_all('tr')

            # Get all the event links
            events = []
            for row in rows:
                event_link_tag = row.find('a', href=True)
                event_link = event_link_tag['href'] if event_link_tag else None
                event_name = event_link_tag.text.strip() if event_link_tag else None
                if event_link:
                    events.append([event_name, event_link])
            events = events[1:]

            # Initialize the final dataset
            all_data = []

            # Loop through event links and extract fight details
            for event in events:
                fight_id = 0
                event_name = event[0]
                resp_event = requests.get(event[1])
                resp_event.raise_for_status()

                # Parse the HTML content of the event page
                soup_event = BeautifulSoup(resp_event.content, 'html.parser')

                # Find all fight rows in the table
                fight_rows = soup_event.find_all('tr', class_='b-fight-details__table-row')[1:]

                # Extract fight details
                for row in fight_rows:
                    fight_id += 1
                    fight_stats_link = row.get('data-link')
                    fighter_links = row.find_all('a', class_='b-link b-link_style_black')
                    fighter_1 = fighter_links[0].text.strip() if len(fighter_links) > 0 else None
                    fighter_2 = fighter_links[1].text.strip() if len(fighter_links) > 1 else None
                    fight_result = fighter_1 if row.find('i', class_='b-flag__text').text.strip() == 'win' else 'nc'

                    td_elements = row.find_all('td')
                    weight_class = td_elements[6].text.strip() if len(td_elements) > 6 else None
                    championship_fight = 1 if 'belt.png' in str(row) else 0

                    method, description = None, None
                    if len(td_elements) > 7 and td_elements[7]:
                        method_details = td_elements[7].find_all('p')
                        method = method_details[0].text.strip() if len(method_details) > 0 else None
                        description = method_details[1].text.strip() if len(method_details) > 1 else None

                    round_ = td_elements[8].text.strip() if len(td_elements) > 8 else None
                    time_ = td_elements[9].text.strip() if len(td_elements) > 9 else None

                    fight_of_the_night = 1 if 'fight.png' in str(row) else 0
                    performance_of_the_night = 1 if 'perf.png' in str(row) else 0
                    sub_of_the_night = 1 if 'sub.png' in str(row) else 0
                    ko_of_the_night = 1 if 'ko.png' in str(row) else 0

                    all_data.append((
                        event_name,
                        fight_id,
                        fight_result,
                        fighter_1,
                        fighter_2,
                        weight_class,
                        method,
                        description,
                        round_,
                        time_,
                        championship_fight,
                        fight_of_the_night,
                        performance_of_the_night,
                        sub_of_the_night,
                        ko_of_the_night,
                        fight_stats_link
                    ))

            # Create a Pandas DataFrame
            columns = [
                'event_name', 'fight_id', 'fight_result', 'fighter_1', 'fighter_2',
                'weight_class', 'method', 'description', 'round', 'time',
                'championship_fight', 'fight_of_the_night', 'performance_of_the_night',
                'sub_of_the_night', 'ko_of_the_night', 'fight_stats_link'
            ]
            df = pd.DataFrame(all_data, columns=columns)

            # write full dump to S3
            df.to_csv(
                f's3://{S3_BUCKET}/UFC_Fights.csv',
                index=True,
                storage_options=S3_OPTS
            )

            # measure and prepare state
            duration_s = time.time() - start_time
            last_event = df['event_name'].iloc[0] if not df.empty else None
            # capture last fight participants
            if not df.empty:
                last_fight = f"{df['fighter_1'].iloc[-1]} vs {df['fighter_2'].iloc[-1]}"
            else:
                last_fight = None
            timestamp = datetime.utcnow().isoformat() + 'Z'

            # build our log line and state dict
            log_line = (
                f"{timestamp}  duration={duration_s:.2f}s  "
                f"last_event={last_event!r}  last_fight={last_fight!r}\n"
            )
            # push to S3
            fs = s3fs.S3FileSystem(**S3_OPTS)
            with fs.open(LOG_PATH, mode='a') as log_f:
                log_f.write(log_line)
            print(df)
        else:
            print("<tbody> not found.")
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")

if __name__ == '__main__':
    main()
