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

# Define input and output buckets and file names
input_bucket = os.getenv("S3_INPUT_BUCKET")
input_folder_path = os.getenv("S3_INPUT_FOLDER_PATH")
neighborhoods_file = os.getenv("NEIGHBORHOODS_FILENAME")
input_obj_path = os.path.join(input_folder_path, neighborhoods_file)

output_bucket = os.getenv("S3_OUTPUT_BUCKET")
output_folder_path = os.getenv("S3_OUTPUT_FOLDER_PATH")
filename = "rent_data_{}.json".format(date)
ouput_obj_path = os.path.join(output_folder_path, filename)

default_url = "https://sandiego.craigslist.org/search/apa?query={}&availabilityMode=0&sale_date=all+dates"
url = os.getenv("CRAIGSLIST_URL", default_url)

# Define regex pattern to extract numeric information
pattern = '([0-9]+)(ft2|br)'
regex = re.compile(pattern)


# Gets list of neighborhoods from s3
def build_neighborhood_list():
    """Pulls file from s3 containing the names of neighborhoods to be scraped.

    Returns:
        Returns a list of neighborhoods to scrape.
    """

    obj = s3.Object(input_bucket, input_obj_path)
    contents = obj.get()['Body'].read().decode('utf-8')
    neighborhood_list = contents.split("\n")

    return neighborhood_list


# Creates dictionary with url links
def build_url_dic(neighborhood_list):
    """Builds dictionary with neighborhood name as key and craigslist url as value which is passed into get_rental_data() function.

    Args:
        neighborhood_list: List of neighborhoods to scrape.

    Returns:
        Dictionary of neighborhoods and url links.
    """

    neighborhoods = {}
    for neighborhood in neighborhood_list:
        fmt = "+".join(neighborhood.split()).lower()
        link = url.format(fmt)
        neighborhoods[neighborhood] = link

    return neighborhoods


# Create function to run scrape_rentals on all neighborhoods
def get_rental_data(neighborhoods):
    """This function loops through all the items in neighborhoods,
    scrapes craiglist for date for that neighborhood, appends it to a list, 
    and uploads a json to s3.

    Args:
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
    obj = s3.Object(output_bucket, ouput_obj_path)
    obj.put(Body=json.dumps(rental_data, separators=(',', ':')))


# Handler function
def main(event, context):
    """Lambda handler function that runs all the functions to scrape data.

    Args:
        event: An AWS Lambda event type that is passed to the handler.
        context: AWS runtime information passed to the handler.
    """

    neighborhood_list = build_neighborhood_list()
    neighborhoods = build_url_dic(neighborhood_list)
    get_rental_data(neighborhoods)


if __name__ == '__main__':

    # For testing locally
    print("Scrapping Craigslist...")
    main("", "")

    print("Uploaded:", os.path.join(output_bucket, ouput_obj_path))
