from bs4 import BeautifulSoup
import requests
import pandas as pd
from ufc_fights_stats_functions import parse_overall_totals, parse_significant_strikes_overall, parse_totals_by_round, parse_significant_by_round

def main():
    # URL of the webpage to scrape
    url = 'http://www.ufcstats.com/statistics/events/completed?page=all'

    # Send an HTTP GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the <tbody> element
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
            events = events[1:]  # First event is future one and irrlevant. Limit to first 3 events for testing

            # Initialize the final dataset
            list_totals_only = []
            list_sig_only = []
            list_totals_by_round = []
            list_sig_by_round = []


            # Loop through event links and extract fight details
            for event in events:
                fight_id = 0
                event_name = event[0]
                # Send an HTTP GET request to the event link
                response = requests.get(event[1])

                # Parse the HTML content of the event page
                soup = BeautifulSoup(response.content, 'html.parser')

                # Find all fight rows in the table
                fight_rows = soup.find_all('tr', class_='b-fight-details__table-row')
                fight_rows = fight_rows[1:]


                # Extract fight details
                for row in fight_rows:
                    fight_id += 1

                    # Extract winner and loser names
                    fight_stats_link = row.get('data-link')
                    response = requests.get(fight_stats_link) 
                    soup = BeautifulSoup(response.content, 'html.parser')
                    data = soup.find_all('tbody', class_='b-fight-details__table-body')

                    # Parse the tbody => Overall Totals
                    try:
                        list_totals_only += parse_overall_totals(data[0], event_name, fight_id)
                        list_sig_only += parse_significant_strikes_overall(data[2], event_name, fight_id)
                        list_totals_by_round += parse_totals_by_round(data[1], event_name, fight_id)
                        list_sig_by_round += parse_significant_by_round(data[3], event_name, fight_id)
                    except:
                        print(f"Error parsing fight_id={fight_id}: {event}")
                        continue

        #Column names:
        list_totals_only_names = ["Event", "Fight_Id", "Fighter", "KD","Sig. Str","Sig. Str %","Total Str","Td","Td %","Sub. att","Rev.","Ctrl"]
        list_sig_only_names = ["Event", "Fight_Id", "Fighter", "Sig. Head", "Sig. Body","Sig. Leg", "Sig. Distance", "Sig. Clinch", "Sig. Ground"]
        list_totals_by_round_names = ["Event", "Fight_Id", "Round", "Fighter", "KD","Sig. Str","Sig. Str %","Total Str", "Td","Td %","Sub. att","Rev.","Ctrl"]
        list_sig_by_round_names = ["Event", "Fight_Id", "Round", "Fighter", "Sig. Head", "Sig. Body", "Sig. Leg","Sig. Distance", "Sig. Clinch", "Sig. Ground"]

        list_totals_only_df = pd.DataFrame(list_totals_only, columns=list_totals_only_names)
        list_sig_only_df = pd.DataFrame(list_sig_only, columns=list_sig_only_names)
        list_totals_by_round_df = pd.DataFrame(list_totals_by_round, columns=list_totals_by_round_names)
        list_sig_by_round_df = pd.DataFrame(list_sig_by_round, columns=list_sig_by_round_names)

        df_Fight_stats = pd.merge(list_totals_only_df, list_sig_only_df, on=["Event", "Fight_Id", "Fighter"], how='left')
        df_Fight_stats.to_csv('../data/UFC_Fights_stats.csv', index=True)
        print(df_Fight_stats)

        df_Round_stats = pd.merge(list_totals_by_round_df, list_sig_by_round_df, on=["Event", "Fight_Id", "Round", "Fighter"], how='left')
        df_Round_stats.to_csv('../data/UFC_Round_stats.csv', index=True)
        print(df_Round_stats)

# Call the main function
if __name__ == '__main__':
    main()



# Error parsing fight_id=8: ['UFC - Ultimate Brazil', 'http://www.ufcstats.com/event-details/32a3025d5db456ae']
# Error parsing fight_id=8: ['UFC 17: Redemption', 'http://www.ufcstats.com/event-details/4a01dc8376736ef5']
# Error parsing fight_id=7: ['UFC 16: Battle in the Bayou', 'http://www.ufcstats.com/event-details/749685d24e2cac50']
# Error parsing fight_id=8: ['UFC 16: Battle in the Bayou', 'http://www.ufcstats.com/event-details/749685d24e2cac50']
# Error parsing fight_id=8: ['UFC 12: Judgement Day', 'http://www.ufcstats.com/event-details/96eff1a628adcc7f']
# Error parsing fight_id=9: ['UFC 12: Judgement Day', 'http://www.ufcstats.com/event-details/96eff1a628adcc7f']
# Error parsing fight_id=8: ["UFC - Ultimate Ultimate '96", 'http://www.ufcstats.com/event-details/9b5b5a75523728f3']
# Error parsing fight_id=9: ["UFC - Ultimate Ultimate '96", 'http://www.ufcstats.com/event-details/9b5b5a75523728f3']
# Error parsing fight_id=10: ["UFC - Ultimate Ultimate '96", 'http://www.ufcstats.com/event-details/9b5b5a75523728f3']
# Error parsing fight_id=7: ['UFC 11: The Proving Ground', 'http://www.ufcstats.com/event-details/6ceff86fae4f6b3b']
# Error parsing fight_id=8: ['UFC 11: The Proving Ground', 'http://www.ufcstats.com/event-details/6ceff86fae4f6b3b']
# Error parsing fight_id=8: ['UFC 10: The Tournament', 'http://www.ufcstats.com/event-details/aee8eecfc4bfb1e7']
# Error parsing fight_id=9: ['UFC 8: David vs Goliath', 'http://www.ufcstats.com/event-details/b63e800c18e011b5']
# Error parsing fight_id=8: ["UFC - Ultimate Ultimate '95", 'http://www.ufcstats.com/event-details/31bbd46d57dfbcb7']
# Error parsing fight_id=9: ["UFC - Ultimate Ultimate '95", 'http://www.ufcstats.com/event-details/31bbd46d57dfbcb7']
# Error parsing fight_id=10: ['UFC 7: The Brawl in Buffalo', 'http://www.ufcstats.com/event-details/5af480a3b2e1726b']
# Error parsing fight_id=11: ['UFC 7: The Brawl in Buffalo', 'http://www.ufcstats.com/event-details/5af480a3b2e1726b']
# Error parsing fight_id=9: ['UFC 6: Clash of the Titans', 'http://www.ufcstats.com/event-details/1c3f5e85b59ec710']
# Error parsing fight_id=10: ['UFC 6: Clash of the Titans', 'http://www.ufcstats.com/event-details/1c3f5e85b59ec710']
# Error parsing fight_id=9: ['UFC 4: Revenge of the Warriors', 'http://www.ufcstats.com/event-details/b60391da771deefe']