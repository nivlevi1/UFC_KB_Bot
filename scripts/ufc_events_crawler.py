import time
import json
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import pandas as pd
import s3fs

# configure your S3 filesystem
S3_BUCKET = 'ufc'
LOG_PATH   = f'{S3_BUCKET}/logs/ufc_event_scrape.log'
STATE_PATH = f'{S3_BUCKET}/logs/ufc_state.json'
S3_OPTS    = {
    'key': 'minioadmin',
    'secret': 'minioadmin',
    'client_kwargs': {'endpoint_url': 'http://minio-dev:9000'}
}

def main():
    # start timer
    start_time = time.time()

    # scrape
    url      = 'http://www.ufcstats.com/statistics/events/completed?page=all'
    resp     = requests.get(url)
    resp.raise_for_status()
    soup     = BeautifulSoup(resp.content, 'html.parser')
    tbody    = soup.find('tbody')
    rows     = tbody.find_all('tr', class_='b-statistics__table-row')[1:]
    data     = []

    for row in rows:
        # Extract the Event Name and Link
        event_link_tag = row.find('a', class_='b-link b-link_style_black')
        event_name = event_link_tag.text.strip() if event_link_tag else None
        event_link = event_link_tag['href'] if event_link_tag else None
                
        # Extract the Date
        date_tag = row.find('span', class_='b-statistics__date')
        event_date = date_tag.text.strip() if date_tag else None
                
        # Extract the Location
        location_tag = row.find('td', class_='b-statistics__table-col b-statistics__table-col_style_big-top-padding')
        event_location = location_tag.text.strip() if location_tag else None
                
        # Append the data as a tuple
        data.append((event_name, event_date, event_location, event_link))

    df = pd.DataFrame(data, columns=['Event Name','Date','Location','Link'])


    # write full dump
    df.to_csv(
        f's3://{S3_BUCKET}/UFC_Events.csv',
        index=False,
        storage_options=S3_OPTS
    )

    # measure and prepare state
    duration_s = time.time() - start_time
    last_event = df['Event Name'].iloc[0] if not df.empty else None
    timestamp  = datetime.utcnow().isoformat() + 'Z'

    # build our log line
    log_line = f"{timestamp}  duration={duration_s:.2f}s  last_event={last_event!r}\n"
    state     = {
        'last_run':   timestamp,
        'duration_s': duration_s,
        'last_event': last_event
    }

    # push to S3
    fs = s3fs.S3FileSystem(**S3_OPTS)
    # 1) append to log
    with fs.open(LOG_PATH, mode='a') as f:
        f.write(log_line)
    # 2) overwrite state JSON
    with fs.open(STATE_PATH, mode='w') as f:
        json.dump(state, f)

    print(df)

if __name__ == '__main__':
    main()
