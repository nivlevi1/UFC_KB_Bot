from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
import s3fs
import json
from concurrent.futures import ThreadPoolExecutor
from .stats_functions import (
    parse_overall_totals,
    parse_significant_strikes_overall,
    parse_totals_by_round,
    parse_significant_by_round
)

# configure your S3 filesystem
S3_BUCKET   = 'ufc'
FIGHTS_CSV  = f'{S3_BUCKET}/UFC_Fights.csv'           # <-- master events (results)
STATS_CSV   = f'{S3_BUCKET}/UFC_Fights_stats.csv'     # <-- already-scraped stats
ROUND_CSV   = f'{S3_BUCKET}/UFC_Round_stats.csv'
LOG_PATH    = f'{S3_BUCKET}/logs/ufc_fights_stats_scrape.log'
STATE_PATH = f'{S3_BUCKET}/logs/ufc_state.log'
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

    # --- load what we scraped last time ---
    try:
        df_stats        = pd.read_csv(f's3://{STATS_CSV}', storage_options=S3_OPTS)
        prev_last_event = df_stats['Event'].iloc[-1]
        prev_last_fighter = df_stats['Fighter'].iloc[-1]
    except Exception:
        prev_last_event   = None
        prev_last_fighter = None

    # --- load master list of events, plus find where we left off ---
    try:
        df_master   = pd.read_csv(f's3://{FIGHTS_CSV}', storage_options=S3_OPTS)
        last_master = df_master['event_name'].iloc[-1]
    except Exception:
        last_master = None

    # pull full events list (newest→oldest)
    url = 'http://www.ufcstats.com/statistics/events/completed?page=all'
    response = session.get(url); response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.find('tbody')\
               .find_all('tr', class_='b-statistics__table-row')[1:]
    events = [(r.find('a').text.strip(), r.find('a')['href'])
              for r in rows if r.find('a')]

    # chronological (oldest→newest)
    events_chrono = list(reversed(events))
    names = [e[0] for e in events_chrono]

    # pick up the gap between last_stats and last_master
    if last_master and last_master in names:
        im = names.index(last_master)
        if prev_last_event and prev_last_event in names and im > names.index(prev_last_event):
            is_ = names.index(prev_last_event)
            new_events = events_chrono[is_+1 : im+1]
        else:
            new_events = events_chrono[:im+1] if not prev_last_event else []
    elif prev_last_event and prev_last_event in names:
        is_ = names.index(prev_last_event)
        new_events = events_chrono[is_+1 :]
    else:
        new_events = events_chrono

    # if no new events → log and exit, but keep the old last_event/fighter
    if not new_events:
        duration_s = time.time() - start_time
        timestamp  = datetime.utcnow().isoformat() + 'Z'
        log_line   = (
            f"{timestamp} duration={duration_s:.2f}s "
            f"new_events=0 last_event={prev_last_event!r} "
            f"last_fighter={prev_last_fighter!r}\n"
        )
        with fs.open(LOG_PATH, 'a') as log_f:
            log_f.write(log_line)
        print("No new events to scrape.", flush=True)
        print(log_line, end='', flush=True)
        return

    # --- otherwise scrape stats for each new event ---
    list_totals_only     = []
    list_sig_only        = []
    list_totals_by_round = []
    list_sig_by_round    = []

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

    # build DataFrames
    names_tot     = ["Event","Fight_Id","Fighter","KD","Sig. Str","Sig. Str %","Total Str","Td","Td %","Sub. att","Rev.","Ctrl"]
    names_sig     = ["Event","Fight_Id","Fighter","Sig. Head","Sig. Body","Sig. Leg","Sig. Distance","Sig. Clinch","Sig. Ground"]
    names_tot_rnd = ["Event","Fight_Id","Round","Fighter","KD","Sig. Str","Sig. Str %","Total Str","Td","Td %","Sub. att","Rev.","Ctrl"]
    names_sig_rnd = ["Event","Fight_Id","Round","Fighter","Sig. Head","Sig. Body","Sig. Leg","Sig. Distance","Sig. Clinch","Sig. Ground"]

    df_tot  = pd.DataFrame(list_totals_only,     columns=names_tot)
    df_sig  = pd.DataFrame(list_sig_only,        columns=names_sig)
    df_tr   = pd.DataFrame(list_totals_by_round, columns=names_tot_rnd)
    df_sigr = pd.DataFrame(list_sig_by_round,    columns=names_sig_rnd)

    df_Fight_stats = pd.merge(df_tot, df_sig,
                              on=["Event","Fight_Id","Fighter"], how='left')
    df_Round_stats = pd.merge(df_tr, df_sigr,
                              on=["Event","Fight_Id","Round","Fighter"], how='left')
    with fs.open(STATE_PATH, 'r') as f:
        state = json.load(f)
    last_run = state.get('last_run')

    #Last run update
    df_Fight_stats['last_run'] = last_run
    df_Round_stats['last_run'] = last_run

    #Handling nulls
    df_Fight_stats.replace(['---', '--'], np.nan, inplace=True)
    df_Round_stats.replace(['---', '--'], np.nan, inplace=True)


    # append to CSVs
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

    # --- final log with the newly scraped last_event/fighter ---
    duration_s   = time.time() - start_time
    last_event   = new_events[-1][0]
    last_fight = df_Fight_stats['Fight_Id'].iloc[-1] if not df_Fight_stats.empty else None
    timestamp    = datetime.utcnow().isoformat() + 'Z'
    log_line     = (
        f"{timestamp} duration={duration_s:.2f}s "
        f"new_events={len(new_events)} last_event={last_event!r} "
        f"last_fight_id={last_fight!r}\n"
    )
    with fs.open(LOG_PATH, 'a') as log_f:
        log_f.write(log_line)

    print(log_line, end='', flush=True)

if __name__ == '__main__':
    main()
