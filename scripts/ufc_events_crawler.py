import time
import json
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import pandas as pd
import s3fs

# configure your S3 filesystem
S3_BUCKET  = 'ufc'
LOG_PATH   = f'{S3_BUCKET}/logs/ufc_event_scrape.log'
STATE_PATH = f'{S3_BUCKET}/logs/ufc_state.log'
CSV_PATH   = f'{S3_BUCKET}/UFC_Events.csv'

S3_OPTS = {
    'key': 'minioadmin',
    'secret': 'minioadmin',
    'client_kwargs': {'endpoint_url': 'http://minio-dev:9000'}
}

def load_last_state(fs):
    """Load the last_event from state JSON, or return None if missing."""
    try:
        with fs.open(STATE_PATH, 'r') as f:
            state = json.load(f)
        return state.get('last_event')
    except FileNotFoundError:
        return None

def main():
    start_time = time.time()
    fs = s3fs.S3FileSystem(**S3_OPTS)

    # 1) load last_event from state
    last_event = load_last_state(fs)

    # 2) scrape all completed events
    resp = requests.get('http://www.ufcstats.com/statistics/events/completed?page=all')
    resp.raise_for_status()
    soup  = BeautifulSoup(resp.content, 'html.parser')
    rows  = soup.find('tbody').find_all('tr', class_='b-statistics__table-row')[1:]

    data = []
    for row in reversed(rows[-22:]):  # oldest → newest
        link_tag = row.find('a', class_='b-link b-link_style_black')
        name     = link_tag.text.strip() if link_tag else None
        link     = link_tag['href']    if link_tag else None
        date     = row.find('span', class_='b-statistics__date').text.strip()
        loc      = row.find('td', class_='b-statistics__table-col b-statistics__table-col_style_big-top-padding').text.strip()
        data.append((name, date, loc, link))

    df_all = pd.DataFrame(data, columns=['Event Name','Date','Location','Link'])

    # 3) figure out which rows are new
    if last_event and last_event in df_all['Event Name'].values:
        idx    = df_all.index[df_all['Event Name'] == last_event][0]
        df_new = df_all.iloc[idx+1:]
    else:
        df_new = df_all

    # prepare state values (always)
    duration_s  = time.time() - start_time
    most_recent = df_all['Event Name'].iloc[-1] if not df_all.empty else None
    timestamp   = datetime.utcnow().isoformat() + 'Z'
    state = {
        'last_run':   timestamp,
        'duration_s': duration_s,
        'last_event': most_recent
    }

    # 4) if there *are* new events, append + log
    if not df_new.empty:
        df_new.to_csv(
            f's3://{CSV_PATH}',
            mode='a',
            header=not fs.exists(CSV_PATH),
            index=False,
            storage_options=S3_OPTS
        )

        log_line = (
            f"{timestamp}  duration={duration_s:.2f}s  "
            f"new_events={len(df_new)}  last_event={most_recent!r}\n"
        )
        with fs.open(LOG_PATH, 'a') as f:
            f.write(log_line)
        print(f"Appended {len(df_new)} new events and logged update.")
    else:
        print("No new events to scrape. (No log entry was written.)")

    # 5) always overwrite state
    with fs.open(STATE_PATH, 'w') as f:
        json.dump(state, f)
    print(f"State file updated; last_event is now {most_recent!r}.")

if __name__ == '__main__':
    main()
