import requests
import pandas as pd
from flatten_dict import flatten

from dotenv import load_dotenv
import os

load_dotenv()

app_id = os.getenv("APP_ID")
api_key = os.getenv("API_KEY")

# TODO:
# Change the request to cover many different types of jobs... not just data engineer


def request_to_df():
    # make API call
    request = requests.get(
        f"https://api.adzuna.com/v1/api/jobs/us/search/1?app_id={app_id}&app_key={api_key}&results_per_page=20&what=data%20engineer&content-type=application/json"
    )
    # convert result into json
    result = request.json()
    # flatten nested objects
    flattened_results = [
        flatten(job, reducer="underscore") for job in result["results"]
    ]

    # turn into dataframe
    df = pd.DataFrame.from_dict(flattened_results)

    # rename columns
    df = df[
        [
            "id",
            "redirect_url",
            "title",
            "title",
            "category_tag",
            "description",
            "created",
            "location_area",
            "latitude",
            "longitude",
        ]
    ]
    df.columns = [
        "id",
        "post_url",
        "title",
        "title_keyword",
        "tags",
        "description",
        "publication_date",
        "location",
        "latitude",
        "longitude",
    ]
    return df
