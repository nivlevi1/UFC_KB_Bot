from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
from datetime import datetime
import s3fs
from concurrent.futures import ThreadPoolExecutor
from ufc_fights_stats_functions import (
    parse_overall_totals,
    parse_significant_strikes_overall,
    parse_totals_by_round,
    parse_significant_by_round
)

# configure your S3 filesystem
S3_BUCKET   = 'ufc'
FIGHTS_CSV  = f'{S3_BUCKET}/UFC_Fights.csv'           # <-- master events
STATS_CSV   = f'{S3_BUCKET}/UFC_Fights_stats.csv'     # <-- already-scraped stats
ROUND_CSV   = f'{S3_BUCKET}/UFC_Round_stats.csv'
LOG_PATH    = f'{S3_BUCKET}/logs/ufc_fights_stats_scrape.log'
S3_OPTS     = {
    'key': 'minioadmin',
    'secret': 'minioadmin',
    'client_kwargs': {'endpoint_url': 'http://minio-dev:9000'}
}

# shared session for HTTP connection pooling
session = requests.Session()
session.headers.update({'User-Agent': 'ufc-scraper/1.0'})

def fetch_fight(event_name, fight_id, fight_link):
    try:
        resp = session.get(fight_link); resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        bodies   = soup.find_all('tbody', class_='b-fight-details__table-body')
        totals   = parse_overall_totals(bodies[0], event_name, fight_id)
        sig      = parse_significant_strikes_overall(bodies[2], event_name, fight_id)
        totals_r = parse_totals_by_round(bodies[1], event_name, fight_id)
        sig_r    = parse_significant_by_round(bodies[3], event_name, fight_id)
        print(f"  Parsed fight {fight_id}", flush=True)
        return totals, sig, totals_r, sig_r
    except Exception:
        print(f"Error parsing fight_id={fight_id} for {event_name}", flush=True)
        return None

def main():
    start_time = time.time()
    fs = s3fs.S3FileSystem(**S3_OPTS)

    # load master list of events
    try:
        df_master   = pd.read_csv(f's3://{FIGHTS_CSV}', storage_options=S3_OPTS)
        last_master = df_master['event_name'].iloc[-1]
    except Exception:
        last_master = None

    # load already-scraped stats
    try:
        df_stats    = pd.read_csv(f's3://{STATS_CSV}', storage_options=S3_OPTS)
        last_stats  = df_stats['Event'].iloc[-1]
    except Exception:
        last_stats  = None

    # scrape the full list of completed events (newest→oldest)
    url = 'http://www.ufcstats.com/statistics/events/completed?page=all'
    response = session.get(url); response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.find('tbody')\
               .find_all('tr', class_='b-statistics__table-row')[1:]
    events = [(r.find('a').text.strip(), r.find('a')['href'])
              for r in rows if r.find('a')]

    # chronological order (oldest→newest)
    events_chrono = list(reversed(events))
    names         = [e[0] for e in events_chrono]

    # compute only the “gap” between last_stats and last_master
    if last_master and last_master in names:
        im = names.index(last_master)
        if last_stats and last_stats in names and im > names.index(last_stats):
            is_ = names.index(last_stats)
            new_events = events_chrono[is_+1 : im+1]
        else:
            new_events = events_chrono[:im+1] if not last_stats else []
    elif last_stats and last_stats in names:
        is_ = names.index(last_stats)
        new_events = events_chrono[is_+1 :]
    else:
        new_events = events_chrono

    # bail out if there's nothing new—leave log untouched
    if not new_events:
        print("No new events to scrape.", flush=True)
        return

    # Initialize containers for the new stats data
    list_totals_only     = []
    list_sig_only        = []
    list_totals_by_round = []
    list_sig_by_round    = []

    # Loop through only the new events
    for event_name, event_url in new_events:
        print(f"Scraping event: {event_name}", flush=True)
        ev_resp = session.get(event_url); ev_resp.raise_for_status()
        ev_soup = BeautifulSoup(ev_resp.content, 'html.parser')
        fight_rows = ev_soup.find_all(
            'tr', class_='b-fight-details__table-row'
        )[1:]

        tasks = [
            (event_name, idx + 1, row.get('data-link'))
            for idx, row in enumerate(fight_rows)
        ]
        with ThreadPoolExecutor(max_workers=8) as executor:
            for result in executor.map(lambda args: fetch_fight(*args), tasks):
                if result:
                    t, s, tr, sr = result
                    list_totals_only     += t
                    list_sig_only        += s
                    list_totals_by_round += tr
                    list_sig_by_round    += sr

    # DataFrame column definitions
    list_totals_only_names     = ["Event","Fight_Id","Fighter","KD","Sig. Str","Sig. Str %","Total Str","Td","Td %","Sub. att","Rev.","Ctrl"]
    list_sig_only_names        = ["Event","Fight_Id","Fighter","Sig. Head","Sig. Body","Sig. Leg","Sig. Distance","Sig. Clinch","Sig. Ground"]
    list_totals_by_round_names = ["Event","Fight_Id","Round","Fighter","KD","Sig. Str","Sig. Str %","Total Str","Td","Td %","Sub. att","Rev.","Ctrl"]
    list_sig_by_round_names    = ["Event","Fight_Id","Round","Fighter","Sig. Head","Sig. Body","Sig. Leg","Sig. Distance","Sig. Clinch","Sig. Ground"]

    # Build DataFrames from the scraped lists
    df_tot  = pd.DataFrame(list_totals_only,     columns=list_totals_only_names)
    df_sig  = pd.DataFrame(list_sig_only,        columns=list_sig_only_names)
    df_tr   = pd.DataFrame(list_totals_by_round, columns=list_totals_by_round_names)
    df_sigr = pd.DataFrame(list_sig_by_round,    columns=list_sig_by_round_names)

    df_Fight_stats = pd.merge(df_tot, df_sig,
                              on=["Event","Fight_Id","Fighter"], how='left')
    df_Round_stats = pd.merge(df_tr, df_sigr,
                              on=["Event","Fight_Id","Round","Fighter"], how='left')

    # Append new stats to the existing CSVs
    df_Fight_stats.to_csv(
        f's3://{STATS_CSV}',
        mode='a',
        header=not fs.exists(STATS_CSV),
        index=False,
        storage_options=S3_OPTS
    )
    df_Round_stats.to_csv(
        f's3://{ROUND_CSV}',
        mode='a',
        header=not fs.exists(ROUND_CSV),
        index=False,
        storage_options=S3_OPTS
    )

    # Log the run
    duration_s   = time.time() - start_time
    last_event   = new_events[-1][0]
    last_fighter = df_Fight_stats['Fighter'].iloc[-1] if not df_Fight_stats.empty else None
    timestamp    = datetime.utcnow().isoformat() + 'Z'
    log_line     = (
        f"{timestamp} duration={duration_s:.2f}s "
        f"new_events={len(new_events)} last_event={last_event!r} "
        f"last_fighter={last_fighter!r}\n"
    )
    with fs.open(LOG_PATH, 'a') as log_f:
        log_f.write(log_line)

    print(log_line, end='', flush=True)

if __name__ == '__main__':
    main()
