#!/usr/bin/env python3

import time
import string
from datetime import datetime
import numpy as np
import requests
from bs4 import BeautifulSoup
import pandas as pd
import s3fs
import json

# configure your S3 filesystem
S3_BUCKET = 'ufc'
LOG_PATH  = f'{S3_BUCKET}/logs/ufc_fighters_scrape.log'
STATE_PATH = f'{S3_BUCKET}/logs/ufc_state.log'
S3_OPTS = {
    'key': 'minioadmin',
    'secret': 'minioadmin',
    'client_kwargs': {'endpoint_url': 'http://minio-dev:9000'}
}

def main():
    start_time = time.time()
    data = []

    # create a single Session for connection reuse + retries
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (compatible; UFC-Fighter-Scraper/1.0)'
    })
    adapter = requests.adapters.HTTPAdapter(pool_connections=10,
                                            pool_maxsize=10,
                                            max_retries=2)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    for char in string.ascii_lowercase:
        url = f'http://ufcstats.com/statistics/fighters?char={char}&page=all'
        print(f"→ Fetching page for '{char}'…", flush=True)
        resp = session.get(url, timeout=10)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.content, 'html.parser')
        tbody = soup.find('tbody')
        if not tbody:
            print(f"[{char}] no table found, skipping", flush=True)
            continue

        rows = tbody.find_all('tr', class_='b-statistics__table-row')
        print(f"[{char}] found {len(rows)} rows", flush=True)

        before = len(data)
        for row in rows:
            cols = row.find_all('td', class_='b-statistics__table-col')
            if len(cols) < 11:
                continue

            first_name = cols[0].get_text(strip=True)
            last_name = cols[1].get_text(strip=True)
            nickname = cols[2].get_text(strip=True)

            raw_ht = cols[3].get_text(strip=True)
            height = raw_ht[:-1] if raw_ht.endswith('"') else raw_ht

            weight = cols[4].get_text(strip=True)

            raw_reach = cols[5].get_text(strip=True).rstrip('"')
            reach = raw_reach if raw_reach and raw_reach != '--' else None

            stance = cols[6].get_text(strip=True)
            wins = cols[7].get_text(strip=True)
            losses = cols[8].get_text(strip=True)
            draws = cols[9].get_text(strip=True)
            belt = 'Yes' if cols[10].find('img') else ''
            link = cols[0].find('a')['href']

            data.append({
                'first_name': first_name,
                'last_name': last_name,
                'nickname': nickname,
                'height': height,
                'weight': weight,
                'reach': reach,
                'stance': stance,
                'wins': wins,
                'losses': losses,
                'draws': draws,
                'belt': belt,
                'link': link
            })

        added = len(data) - before
        print(f"[{char}] appended {added} fighters (total so far: {len(data)})", flush=True)

    # build DataFrame with consistent column names
    df = pd.DataFrame(data, columns=[
        'first_name', 'last_name', 'nickname', 'height', 'weight',
        'reach', 'stance', 'wins', 'losses', 'draws', 'belt', 'link'
    ])
    df.replace(['--', '---'], np.nan, inplace=True)

    # write to S3
    fs = s3fs.S3FileSystem(**S3_OPTS)
    with fs.open(STATE_PATH, 'r') as f:
        state = json.load(f)
    last_run = state.get('last_run')
    df['last_run'] = last_run

    df.to_csv(
        f's3://{S3_BUCKET}/UFC_Fighters.csv',
        index=False,
        storage_options=S3_OPTS
    )

    # final summary & logging
    duration = time.time() - start_time
    timestamp = datetime.utcnow().isoformat() + 'Z'
    print(f"\nDone: scraped {len(df)} fighters in {duration:.1f}s", flush=True)

    log_line = f"{timestamp}  duration={duration:.2f}\n"
    with fs.open(LOG_PATH, 'a') as f:
        f.write(log_line)

if __name__ == '__main__':
    main()
