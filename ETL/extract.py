import requests
import pandas as pd
from flatten_dict import flatten

from dotenv import load_dotenv
import os

load_dotenv()

app_id = os.getenv("APP_ID")
api_key = os.getenv("API_KEY")

# This is where we can define the titles that we want to search for
main_titles = ["data engineer", "data scientist"]


def adzuna():
    """
    Function to make HTTP requests to Adzuna API
    Returns dataframe that is the same format as others
    Current format:
        df.columns = [
        "id",
        "post_url",
        "title",
        "title_keyword",
        "tags",
        "description",
        "company",
        "publication_date",
        "location",
        "latitude",
        "longitude",
        "city",
        "state
    ]
    """
    adzuna_titles = [item.strip().replace(" ", "%20") for item in main_titles]
    appended_results = list()

    for title in adzuna_titles:

        # make the requests to Adzuna API
        request = requests.get(
            f"https://api.adzuna.com/v1/api/jobs/us/search/1?app_id={app_id}&app_key={api_key}&results_per_page=20&what={title}&content-type=application/json"
        )
        result = request.json()

        # flatten nested objects returned by API by looping over each job
        flattened_results = [
            flatten(job, reducer="underscore") for job in result["results"]
        ]

        # append those flattened results to make a list of flattened results
        # with each index being a different API
        appended_results.append(flattened_results)

        # turn 2D list with different API calls into a 1D list to be put into df
        final = [job for api in appended_results for job in api]

    df = pd.DataFrame.from_dict(final)

    # rename columns
    df = df[
        [
            "id",
            "redirect_url",
            "title",
            "title",
            "category_tag",
            "description",
            "company_display_name",
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
        "company",
        "publication_date",
        "location",
        "latitude",
        "longitude",
    ]

    # separate city and state
    # make sure that there is more than one value for location (some are only country)
    df["city"] = df["location"].apply(
        lambda x: x[-1].replace(" County", "") if len(x) > 1 else "unknown"
    )
    df["state"] = df["location"].apply(
        lambda x: x[1] if len(x) > 1 else "uknown"
    )

    print(df)

    return df


# def merge_dfs():
#     """
#     Merges all of the dfs!
#     """
#     adzuna_df = adzuna()
#     # merge
