from bs4 import BeautifulSoup
import requests
import pandas as pd


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
            all_data = []

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
                    fighter_links = row.find_all('a', class_='b-link b-link_style_black')
                    fighter_1 = fighter_links[0].text.strip() if len(fighter_links) > 0 else None
                    fighter_2 = fighter_links[1].text.strip() if len(fighter_links) > 1 else None
                    fight_result = fighter_1 if row.find('i', class_='b-flag__text').text.strip() == 'win' else 'nc'

                    # Extract weight class
                    td_elements = row.find_all('td')
                    weight_class = None
                    if len(td_elements) > 6:
                        weight_class = td_elements[6].text.strip()

                    # Check for championship fight
                    championship_fight = 1 if 'belt.png' in str(row) else 0

                    # Extract method and description
                    method, description = None, None
                    if len(td_elements) > 7:
                        fight_details_td = td_elements[7]
                        if fight_details_td:
                            method_details = fight_details_td.find_all('p')
                            method = method_details[0].text.strip() if len(method_details) > 0 else None
                            description = method_details[1].text.strip() if len(method_details) > 1 else None

                    # Extract round and time
                    round_, time = None, None
                    if len(td_elements) > 9:
                        round_ = td_elements[8].text.strip()
                        time = td_elements[9].text.strip()

                    # Extract fight bonus flags
                    fight_of_the_night = 1 if 'fight.png' in str(row) else 0
                    performance_of_the_night = 1 if 'perf.png' in str(row) else 0
                    sub_of_the_night = 1 if 'sub.png' in str(row) else 0
                    ko_of_the_night = 1 if 'ko.png' in str(row) else 0

                    # Append the fight details as a tuple
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
                        time,
                        championship_fight,
                        fight_of_the_night,
                        performance_of_the_night,
                        sub_of_the_night,
                        ko_of_the_night,
                        fight_stats_link
                    ))

            # Create a Pandas DataFrame
            columns = ['event_name', 'fight_id', 'fight_result', 'fighter_1', 'fighter_2', 'weight_class', 'method', 'description',
                       'round', 'time', 'championship_fight', 'fight_of_the_night',
                       'performance_of_the_night', 'sub_of_the_night', 'ko_of_the_night', 'fight_stats_link']
            df = pd.DataFrame(all_data, columns=columns)

            # Display the DataFrame
            print(df)
            df.to_csv('data/UFC_Fights.csv', index=True)

        else:
            print("<tbody> not found.")
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")


# Call the main function
if __name__ == '__main__':
    main()
