from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import json
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
S3_BUCKET  = 'ufc'
LOG_PATH    = f'{S3_BUCKET}/logs/ufc_fights_scrape.log'
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
        resp = session.get(fight_link)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        bodies = soup.find_all('tbody', class_='b-fight-details__table-body')
        totals     = parse_overall_totals(bodies[0], event_name, fight_id)
        sig        = parse_significant_strikes_overall(bodies[2], event_name, fight_id)
        totals_r   = parse_totals_by_round(bodies[1], event_name, fight_id)
        sig_r      = parse_significant_by_round(bodies[3], event_name, fight_id)
        print(f"  Parsed fight {fight_id}", flush=True)
        return totals, sig, totals_r, sig_r
    except Exception:
        print(f"Error parsing fight_id={fight_id} for {event_name}", flush=True)
        return None


def main():
    start_time = time.time()

    # URL of the webpage to scrape
    url = 'http://www.ufcstats.com/statistics/events/completed?page=all'
    response = session.get(url)
    response.raise_for_status()

    # Parse the HTML content of the page
    soup = BeautifulSoup(response.content, 'html.parser')
    tbody = soup.find('tbody')
    if not tbody:
        print('<tbody> not found.', flush=True)
        return

    # Get all the event links
    rows = tbody.find_all('tr')
    events = []
    for row in rows:
        tag = row.find('a', href=True)
        if tag:
            events.append((tag.text.strip(), tag['href']))
    events = events[1:]

    # Initialize the final datasets
    list_totals_only     = []
    list_sig_only        = []
    list_totals_by_round = []
    list_sig_by_round    = []

    # Loop through event links and extract fight details in parallel
    for event_name, event_url in events:
        print(f"Scraping event: {event_name}", flush=True)
        ev_resp = session.get(event_url)
        ev_resp.raise_for_status()
        ev_soup = BeautifulSoup(ev_resp.content, 'html.parser')
        fight_rows = ev_soup.find_all(
            'tr', class_='b-fight-details__table-row'
        )[1:]

        # prepare tasks
        tasks = [
            (event_name, idx + 1, row.get('data-link'))
            for idx, row in enumerate(fight_rows)
        ]
        # process in thread pool
        with ThreadPoolExecutor(max_workers=8) as executor:
            for result in executor.map(lambda args: fetch_fight(*args), tasks):
                if result:
                    t, s, tr, sr = result
                    list_totals_only     += t
                    list_sig_only        += s
                    list_totals_by_round += tr
                    list_sig_by_round    += sr

    # Column names
    list_totals_only_names = ["Event", "Fight_Id", "Fighter", "KD", "Sig. Str", "Sig. Str %", "Total Str", "Td", "Td %", "Sub. att", "Rev.", "Ctrl"]
    list_sig_only_names    = ["Event", "Fight_Id", "Fighter", "Sig. Head", "Sig. Body", "Sig. Leg", "Sig. Distance", "Sig. Clinch", "Sig. Ground"]
    list_totals_by_round_names = ["Event", "Fight_Id", "Round", "Fighter", "KD", "Sig. Str", "Sig. Str %", "Total Str", "Td", "Td %", "Sub. att", "Rev.", "Ctrl"]
    list_sig_by_round_names    = ["Event", "Fight_Id", "Round", "Fighter", "Sig. Head", "Sig. Body", "Sig. Leg", "Sig. Distance", "Sig. Clinch", "Sig. Ground"]

    # Create DataFrames
    df_tot  = pd.DataFrame(list_totals_only, columns=list_totals_only_names)
    df_sig  = pd.DataFrame(list_sig_only, columns=list_sig_only_names)
    df_tr   = pd.DataFrame(list_totals_by_round, columns=list_totals_by_round_names)
    df_sigr = pd.DataFrame(list_sig_by_round, columns=list_sig_by_round_names)

    df_Fight_stats = pd.merge(df_tot, df_sig, on=["Event", "Fight_Id", "Fighter"], how='left')
    df_Round_stats = pd.merge(df_tr, df_sigr, on=["Event", "Fight_Id", "Round", "Fighter"], how='left')

    # Write to S3
    df_Fight_stats.to_csv(
        f's3://{S3_BUCKET}/UFC_Fights_stats.csv', index=True,
        storage_options=S3_OPTS
    )
    df_Round_stats.to_csv(
        f's3://{S3_BUCKET}/UFC_Round_stats.csv', index=True,
        storage_options=S3_OPTS
    )

    # Log & state
    duration_s = time.time() - start_time
    last_event = events[0] if events else None
    last_fighter = df_Fight_stats['Fighter'].iloc[-1] if not df_Fight_stats.empty else None
    timestamp = datetime.utcnow().isoformat() + 'Z'

    log_line = f"{timestamp}  duration={duration_s:.2f}s  last_event={last_event!r}  last_fighter={last_fighter!r}\n"

    fs = s3fs.S3FileSystem(**S3_OPTS)
    with fs.open(LOG_PATH, 'a') as log_f:
        log_f.write(log_line)

    # Final real-time print
    print(log_line, end='', flush=True)

if __name__ == '__main__':
    main()
