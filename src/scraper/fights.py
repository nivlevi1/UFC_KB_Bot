import time
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import s3fs
import json

# S3 configuration
S3_BUCKET         = 'ufc'
MASTER_EVENTS_CSV = f'{S3_BUCKET}/UFC_Events.csv'
FIGHTS_CSV        = f'{S3_BUCKET}/UFC_Fights.csv'
LOG_PATH          = f'{S3_BUCKET}/logs/ufc_fights_scrape.log'
STATE_PATH = f'{S3_BUCKET}/logs/ufc_state.log'
S3_OPTS           = {
    'key': 'minioadmin',
    'secret': 'minioadmin',
    'client_kwargs': {'endpoint_url': 'http://minio-dev:9000'}
}

def get_bookmarks(fs):
    """
    Load:
      - prev_last_fights_event: the last event_name in the fights CSV
      - prev_last_master_event: the last Event Name in the master events CSV
      - prev_last_fight: the last "fighter1 vs fighter2" scraped
    If any file is missing or malformed, returns Nones.
    """
    # default values
    prev_last_fights_event = None
    prev_last_master_event = None
    prev_last_fight        = None

    # master events CSV → last master
    try:
        ev_df = pd.read_csv(f's3://{MASTER_EVENTS_CSV}', storage_options=S3_OPTS)
        prev_last_master_event = ev_df['event_name'].iloc[-1]
    except Exception:
        pass

    # fights CSV → last fights event and last fight detail
    try:
        fights_df = pd.read_csv(f's3://{FIGHTS_CSV}', storage_options=S3_OPTS)
        # last event_name
        prev_last_fights_event = fights_df['event_name'].iloc[-1]
        # last fight participants
        r = fights_df.iloc[-1]
        prev_last_fight = f"{r['fighter_1']} vs {r['fighter_2']}"
    except Exception:
        pass

    return prev_last_fights_event, prev_last_master_event, prev_last_fight

def main():
    start_time = time.time()
    fs = s3fs.S3FileSystem(**S3_OPTS)

    # load previous run's bookmarks
    prev_last_fights_event, prev_last_master_event, prev_last_fight = get_bookmarks(fs)

    # 1) scrape the full list (newest→oldest)
    url  = 'http://www.ufcstats.com/statistics/events/completed?page=all'
    resp = requests.get(url); resp.raise_for_status()
    soup = BeautifulSoup(resp.content, 'html.parser')
    rows = soup.find('tbody') \
               .find_all('tr', class_='b-statistics__table-row')[1:]
    events = [(r.find('a').text.strip(), r.find('a')['href'])
              for r in rows if r.find('a')]

    # 2) chronological order (oldest→newest)
    events_chrono = list(reversed(events))
    names         = [e[0] for e in events_chrono]

    # 3) filtering logic
    if prev_last_master_event:
        if prev_last_fights_event and prev_last_fights_event in names:
            idx_master = names.index(prev_last_master_event) if prev_last_master_event in names else -1
            idx_fights = names.index(prev_last_fights_event)
            if idx_master > idx_fights:
                new_events = events_chrono[idx_fights+1 : idx_master+1]
            else:
                new_events = []
        else:
            # first time scraping fights or master not in list
            idx_master = names.index(prev_last_master_event) if prev_last_master_event in names else -1
            new_events = events_chrono[: idx_master+1] if idx_master >= 0 else events_chrono[:]
    else:
        new_events = events_chrono[:]

    all_data   = []
    last_fight = None

    # 4) scrape each new event
    for event_name, event_link in new_events:
        print(f"Scraping event: {event_name}")
        ev_r = requests.get(event_link); ev_r.raise_for_status()
        fight_rows = BeautifulSoup(ev_r.content, 'html.parser') \
                         .find_all('tr', class_='b-fight-details__table-row')[1:]
        fight_id = 0

        for row in fight_rows:
            fight_id += 1
            links      = row.find_all('a', class_='b-link')
            fighter_1  = links[0].text.strip()
            fighter_2  = links[1].text.strip()
            result_txt = row.find('i', class_='b-flag__text').text.strip()
            fight_result = fighter_1 if result_txt == 'win' else 'nc'

            td          = row.find_all('td')
            weight_cls  = td[6].text.strip()
            champ       = int('belt.png' in str(row))
            method_p    = td[7].find_all('p')
            method      = method_p[0].text.strip()
            description = method_p[1].text.strip()
            rnd         = td[8].text.strip()
            time_       = td[9].text.strip()
            fotn        = int('fight.png' in str(row))
            potn        = int('perf.png' in str(row))
            sotn        = int('sub.png' in str(row))
            kotn        = int('ko.png' in str(row))
            link        = row.get('data-link')

            all_data.append((
                event_name, fight_id, fight_result,
                fighter_1, fighter_2, weight_cls,
                method, description, rnd, time_,
                champ, fotn, potn, sotn, kotn, link
            ))
            last_fight = f"{fighter_1} vs {fighter_2}"
            print(f"Parsed fight {fight_id}")

    # 5) append to UFC_Fights.csv
    df_new = pd.DataFrame(all_data, columns=[
        'event_name','fight_id','fight_result','fighter_1','fighter_2',
        'weight_class','method','description','round','time',
        'championship_fight','fight_of_the_night',
        'performance_of_the_night','sub_of_the_night',
        'ko_of_the_night','fight_stats_link'
    ])

    df_new.replace('---', np.nan, inplace=True)

    with fs.open(STATE_PATH, 'r') as f:
        state = json.load(f)
    last_run = state.get('last_run')
    df_new['last_run'] = last_run

    df_new.to_csv(
        f's3://{FIGHTS_CSV}',
        mode='a',
        header=not fs.exists(FIGHTS_CSV),
        index=False,
        storage_options=S3_OPTS
    )

    # 6) always log (even when new_events is empty)
    ne_count   = len(new_events)
    duration_s = time.time() - start_time
    timestamp  = datetime.utcnow().isoformat() + 'Z'

    if ne_count > 0:
        # use the newly scraped values
        last_evt   = new_events[-1][0]
        # last_fight already set in loop
    else:
        # fall back to whatever was in the last successful run
        last_evt   = prev_last_fights_event
        last_fight = prev_last_fight

    log_line = (
        f"{timestamp} duration={duration_s:.2f}s "
        f"new_events={ne_count} last_event={last_evt!r} "
        f"last_fight={last_fight!r}\n"
    )
    with fs.open(LOG_PATH, 'a') as lf:
        lf.write(log_line)

    print(f"Done: {len(all_data)} fights scraped from {ne_count} new events.")
    print(log_line, end='', flush=True)

if __name__ == '__main__':
    main()
