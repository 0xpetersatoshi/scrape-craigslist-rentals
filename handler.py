import re
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
bucket = "webscraping-data"
obj_path = "craigslist-rental-data/rent_data_" + date + ".json"

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


# Function to scrape data from Craigslist
def scrape_rentals(neighborhood, url):
    """Returns json of housing data from Craigslist.

    Args:
        neighborhood: The neighborhood name to search on craigslist.abs
        url: The url of the neighborhood to search for.

    Returns:
        A json object containing title, price, bedroom count, sqft, and posting link.
    """

    # Create empty lists to hold rental information
    rental_info = []
    prices = []
    bedrooms = []
    square_feet = []

    # Retrieve page with the requests module
    response = requests.get(url)

    # Create BeautifulSoup object; parse with 'lxml'
    soup = BeautifulSoup(response.text, 'lxml')

    # results are returned as an iterable list
    results = soup.find_all('li', class_="result-row")

    # Print out number of results
    print("Found {} results.".format(len(results)))

    # Loop through returned results
    for result in results:
        # Error handling
        try:
            # Identify and return bedrooms and footage
            raw_br = result.find(
                'span', class_="housing").text.split("-")[0].strip()
            if regex.search(raw_br):
                bdrms = float(regex.search(raw_br).group(1))
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
                "datetime": datetime,
                "title": title,
                "price": price,
                "bedrooms": bdrms,
                "sqft": sqft,
                "link": link
            }

            # Append data to list
            rental_info.append(data)

            # Lists to calculate some basic metrics
            prices.append(price)
            bedrooms.append(bdrms)
            square_feet.append(sqft)

        except:
            continue

    # Print for logging purposes
    print("Median Price for {}: ${:,.2f}".format(
        neighborhood, np.median(prices)))
    print("Median Bedrooms for {}: {:.0f} br".format(
        neighborhood, np.median(bedrooms)))
    print("Median Square Footage for {}: {} ft2".format(
        neighborhood, np.median(square_feet)))
    print("-" * 80)

    return rental_info

# this is a test
# Create function to run scrape_rentals on all neighborhoods
def get_rental_data(neighborhoods):
    """This function loops through all the items in neighborhood 
    and runs the scrape_rentals function on them and uploads a json to s3.

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

        # Scrape neighborhood from craigslist
        scraped_data = scrape_rentals(neighborhood, url)

        # Create dict with neighborhood name
        neighborhood_data = {
            "neighborhood": neighborhood,
            "data": scraped_data,
        }

        # Add scraped data to rental_data
        rental_data.append(neighborhood_data)

    # Load rental data to s3
    obj = s3.Object(bucket, obj_path)
    obj.put(Body=json.dumps(rental_data, indent=4))


def main(event, context):
    get_rental_data(neighborhoods)


if __name__ == '__main__':
    print("Scrapping Craigslist...")
    main("", "")

    print("Uploaded to:", bucket + "/" + obj_path)