import time
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import pandas as pd
import s3fs

# S3 configuration
S3_BUCKET         = 'ufc'
MASTER_EVENTS_CSV = f'{S3_BUCKET}/UFC_Events.csv'
FIGHTS_CSV        = f'{S3_BUCKET}/UFC_Fights.csv'
LOG_PATH          = f'{S3_BUCKET}/logs/ufc_fight_scrape.log'
S3_OPTS           = {
    'key': 'minioadmin',
    'secret': 'minioadmin',
    'client_kwargs': {'endpoint_url': 'http://minio-dev:9000'}
}

def get_bookmarks(fs):
    """Return (last_fights_event, last_master_event) or (None, None)."""
    try:
        ev_df = pd.read_csv(f's3://{MASTER_EVENTS_CSV}', storage_options=S3_OPTS)
        last_master = ev_df['Event Name'].iloc[-1]
    except Exception:
        last_master = None

    try:
        fights_df = pd.read_csv(f's3://{FIGHTS_CSV}', storage_options=S3_OPTS)
        last_fights = fights_df['event_name'].iloc[-1]
    except Exception:
        last_fights = None

    return last_fights, last_master

def main():
    start_time = time.time()
    fs = s3fs.S3FileSystem(**S3_OPTS)

    last_fights, last_master = get_bookmarks(fs)

    # 1) scrape the full list (newest→oldest)
    url  = 'http://www.ufcstats.com/statistics/events/completed?page=all'
    resp = requests.get(url); resp.raise_for_status()
    soup = BeautifulSoup(resp.content, 'html.parser')
    rows = soup.find('tbody')\
               .find_all('tr', class_='b-statistics__table-row')[1:]
    events = [(r.find('a').text.strip(), r.find('a')['href']) for r in rows if r.find('a')]

    # 2) chronological order (oldest→newest)
    events_chrono = list(reversed(events))
    names         = [e[0] for e in events_chrono]

    # 3) filtering logic:
    if last_master:
        if last_fights:
            # both exist → if master comes after fights, take the slice between them
            idx_master = names.index(last_master)
            idx_fights = names.index(last_fights)
            if idx_master > idx_fights:
                # include everything from just after last_fights up through last_master
                new_events = events_chrono[idx_fights+1 : idx_master+1]
            else:
                # fights is at or newer than master → nothing to catch up on
                new_events = []
        else:
            # no fights file → take all up through master
            idx_master = names.index(last_master)
            new_events = events_chrono[: idx_master+1]
    else:
        # no master file → grab everything
        new_events = events_chrono[:]

    if not new_events:
        print("No new events to scrape.")
        return

    all_data  = []
    last_fight = None

    # 4) scrape each new event
    for event_name, event_link in new_events:
        print(f"Scraping event: {event_name}")
        ev_r = requests.get(event_link); ev_r.raise_for_status()
        fight_rows = BeautifulSoup(ev_r.content, 'html.parser')\
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
    df_new.to_csv(
        f's3://{FIGHTS_CSV}',
        mode='a',
        header=not fs.exists(FIGHTS_CSV),
        index=False,
        storage_options=S3_OPTS
    )

    # 6) log
    duration_s = time.time() - start_time
    timestamp  = datetime.utcnow().isoformat() + 'Z'
    ne_count   = len(new_events)
    last_evt   = new_events[-1][0]
    log_line = (
        f"{timestamp} duration={duration_s:.2f}s "
        f"new_events={ne_count} last_event={last_evt!r} "
        f"last_fight={last_fight!r}\n"
    )
    with fs.open(LOG_PATH, 'a') as lf:
        lf.write(log_line)

    print(f"Done: {len(all_data)} fights scraped from {ne_count} new events.")

if __name__ == '__main__':
    main()