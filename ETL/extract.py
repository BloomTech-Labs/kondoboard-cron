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
    df["state"] = df["location"].apply(lambda x: x[1] if len(x) > 1 else "uknown")

    print(df)

    return df


def jobsearcher():
    """
    Funciton to make API calls to Jobsearcher.com API
    Returns dataframe that has a matching format of the other API functions
    """

    jobsearcher_titles = [item.replace(" ", "&q[must][1]=")for item in main_titles]
    jobsearcher_titles = ["q[must][0]=" + item for item in jobsearcher_titles]

    for title in jobsearcher_titles:

        # make the requests to Adzuna API
        request = requests.get(f'https://api.jobsearcher.com/v1/jobs?q{jobsearcher_titles}&status=active&limit=100&type=organic&offset=0&distance=25&collapse=companyNameAndState&from_age=1&sortBy[0]=postedDate&sortOrder[0]=desc')

    # convert result into json
        result = request.json()
        # flatten nested objects
        flattened_results =[
            flatten(job, reducer="underscore") for job in result['data']]

        # append those flattened results to make a list of flattened results
        # with each index being a different API
        appended_results.append(flattened_results)

        # turn 2D list with different API calls into a 1D list to be put into df
        final = [job for api in appended_results for job in api]

        # turn into dataframe
    df = pd.DataFrame.from_dict(flattened_results)

        # rename columns
    df = df[['id', 'url', 'title', 'title', '{Need tags}', 'bullets', 'company', 'postedDate', 'location_latitude', 'location_longitude', 'location_city', 'location_state']]

    df.columns = [
        "id",
        "post_url",
        "title",
        "title_keyword",
        "tags",
        "description",
        "company",
        "publication_date",
        "latitude",
        "longitude",
        "city",
        "state"
    ]


    # 'title_keyword' and 'tags' are in Adzuna but NOT in Jobsearcher
    # Changed 'location' to two seperate columns 'city', 'state'
    return df
# def merge_dfs():
#     """
#     Merges all of the dfs!
#     """
#     adzuna_df = adzuna()
#     # merge
