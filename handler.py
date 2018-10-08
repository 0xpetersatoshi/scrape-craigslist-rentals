import re
import os
import json
import requests
import boto3
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime

# Create date
dt = datetime.now()
date = datetime.strftime(dt, '%Y_%m_%d')

# Set up S3 resource
s3 = boto3.resource("s3")
bucket = os.getenv("S3_BUCKET", "webscraping-data")
filepath = os.getenv("S3_FILE_PATH", "craigslist-rental-data")
filename = "rent_data_{}.json".format(date)
obj_path = os.path.join(filepath, filename)

# Define neighborhoods and URLs
hillcrest = 'https://sandiego.craigslist.org/search/apa?query=hillcrest&availabilityMode=0&sale_date=all+dates'
north_park = 'https://sandiego.craigslist.org/search/apa?query=north+park&availabilityMode=0&sale_date=all+dates'
south_park = 'https://sandiego.craigslist.org/search/apa?query=south+park&availabilityMode=0&sale_date=all+dates'
mission_hills = 'https://sandiego.craigslist.org/search/apa?query=mission+hills&availabilityMode=0&sale_date=all+dates'
golden_hill = 'https://sandiego.craigslist.org/search/apa?query=golden+hill&availabilityMode=0&sale_date=all+dates'
little_italy = 'https://sandiego.craigslist.org/search/apa?query=little+italy&availabilityMode=0&sale_date=all+dates'
east_village = 'https://sandiego.craigslist.org/search/apa?query=east+village&availabilityMode=0&sale_date=all+dates'
normal_heights = 'https://sandiego.craigslist.org/search/apa?query=normal+heights&availabilityMode=0&sale_date=all+dates'
university_heights = 'https://sandiego.craigslist.org/search/apa?query=university+heights&availabilityMode=0&sale_date=all+dates'
kensington = 'https://sandiego.craigslist.org/search/apa?query=kensington&availabilityMode=0&sale_date=all+dates'
bankers_hill = 'https://sandiego.craigslist.org/search/apa?query=bankers+hill&availabilityMode=0&sale_date=all+dates'

# Create dictionary of neighborhoods and URLs
neighborhoods = {
    "Hillcrest": hillcrest,
    "North Park": north_park,
    "South Park": south_park,
    "Mission Hills": mission_hills,
    "Golden Hill": golden_hill,
    "Little Italy": little_italy,
    "East Village": east_village,
    "Normal Heights": normal_heights,
    "University Heights": university_heights,
    "Kensington": kensington,
    "Bankers Hill": bankers_hill,
}

# Define regex pattern to extract numeric information
pattern = '([0-9]+)(ft2|br)'
regex = re.compile(pattern)


# Create function to run scrape_rentals on all neighborhoods
def get_rental_data(neighborhoods):
    """This function loops through all the items in neighborhoods,
    scrapes craiglist for date for that neighborhood, appends it to a list, 
    and uploads a json to s3.

    Args:
        event: An AWS Lambda event type that is passed to the handler.
        context: AWS runtime information passed to the handler.
        neighborhoods: neighborhoods is a dictionary containing 
            the names of the neighborhoods as keys and the craigslist URLs as values.
    """

    # Create list to hold all scraped data
    rental_data = []

    # Loop through neighborhoods dict
    for neighborhood, url in neighborhoods.items():

        # Retrieve page with the requests module
        response = requests.get(url)

        # Create BeautifulSoup object; parse with 'lxml'
        soup = BeautifulSoup(response.text, 'lxml')

        # results are returned as an iterable list
        results = soup.find_all('li', class_="result-row")

        # Loop through returned results
        for result in results:
            # Error handling
            try:
                # Identify and return bedrooms and footage
                raw_br = result.find(
                    'span', class_="housing").text.split("-")[0].strip()
                if regex.search(raw_br):
                    bedrooms = float(regex.search(raw_br).group(1))
                else:
                    continue

                raw_sqft = result.find(
                    'span', class_="housing").text.split("-")[1].strip()
                if regex.search(raw_sqft):
                    sqft = float(regex.search(raw_sqft).group(1))
                else:
                    continue

                # Get datetime of post
                datetime = result.find("time")["datetime"]

                # Identify and return title of listing
                title = result.find('a', class_="result-title").text

                # Identify and return price of listing
                price = float(result.a.span.text.strip("$"))

                # Identify and return link to listing
                link = result.a['href']

                # Create dictionary for result
                data = {
                    "neighborhood": neighborhood,
                    "datetime": datetime,
                    "title": title,
                    "price": price,
                    "bedrooms": bedrooms,
                    "sqft": sqft,
                    "link": link
                }

                # Append data to list
                rental_data.append(data)

            except:
                continue

    # Load rental data to s3
    # obj = s3.Object(bucket, filename)
    # obj.put(Body=json.dumps(rental_data, separators=(',', ':')))
    with open("../../../Desktop/{}".format(filename), "w") as outfile:
        json.dump(rental_data, outfile, indent=4)


def main(event, context):
    get_rental_data(neighborhoods)


if __name__ == '__main__':
    print("Scrapping Craigslist...")
    main("", "")

    print("Uploaded to:", os.path.join(bucket, obj_path))
