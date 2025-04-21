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
            # Extract all rows with the specified class
            rows = tbody.find_all('tr', class_='b-statistics__table-row')
            rows = rows[1:]
            
            # Initialize a list to store the extracted data
            data = []
            
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
            
            # Create a pandas DataFrame
            df = pd.DataFrame(data, columns=['Event Name', 'Date', 'Location', 'Link'])
            df.to_csv('../data/UFC_Events.csv', index=True)
            
            # Display the DataFrame
            print(df)
        else:
            print("<tbody> not found.")
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")

# Call the main function
if __name__ == '__main__':
    main()
