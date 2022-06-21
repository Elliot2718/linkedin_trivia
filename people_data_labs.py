"""Functions for accessing the People Data Labs API."""

import requests, json, csv, os

def auth() -> str:
    """
    Return the api key for People Data Labs. This token should be stored in an
    environment variable called "PDL_API_KEY".
    """
    try:
        return os.environ["PDL_API_KEY"]
    except KeyError as err:
        msg = (
            "Failed to find a People Data Labs api key on this machine."
            "Please create an API Key , then store it as an\n"
            "environment variable called 'PDL_API_KEY'.\n"
        )
        raise KeyError(msg) from err


def read_linkedin_profiles(filename):
    """
    Read in linkedin profile urls from a csv file with no headers
    and return json formatted for the People Data Labs Bulk Person Enrichment
    API.
    """
    data = {
                "requests": []
        }

    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter = ',')
        for row in csv_reader:     
            data['requests'] += [{'params': {'profile': row[0]}}]

    return data


def get_bulk_person_data(data):
    """Retrieve data from the Bulk Person Enrichment API."""
    PDL_URL = "https://api.peopledatalabs.com/v5/person/bulk"
    
    headers = {
        'Content-Type': "application/json",
        'X-api-key': auth()
    }

    return requests.post(
        PDL_URL,
        headers = headers,
        json = data
    ).json()


def get_linkedin_data(input_file, out_folder):
    """
    Parse the responses and save to a set of json files.
    
    Example:
    `get_linkedin_data("data/profile_urls.csv","./data/output")`
    """
    data = read_linkedin_profiles(input_file)

    for response in get_bulk_person_data(data):
        if response["status"] == 200:
            record = response['data']
            linkedin_username = response["data"]["linkedin_username"]
            out_file = os.path.join(out_folder, linkedin_username + '.json')
            with open(out_file, 'w') as f:
                json.dump(record, f)
        else:
            print("Bulk Person Enrichment Error:", linkedin_username, response)


def convert_json_to_csv(input_folder, out_folder):
    """Converts json files to csv to ease importing into duckdb"""
    with open(os.path.join(out_folder, "linkedin.csv"), "w", newline='', encoding="utf-8-sig") as out_file:
        csv_writer = csv.writer(out_file, delimiter = '\t')
        for idx, filename in enumerate(os.listdir(input_folder)):
            with open(os.path.join(input_folder, filename)) as f:
                data = json.load(f)
                if idx == 0:
                    csv_writer.writerow(data.keys())
                csv_writer.writerow(data.values())
