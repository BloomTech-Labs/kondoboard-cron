import requests
import pandas as pd
from flatten_dict import flatten

import os

app_id = os.environ["APP_ID"]
api_key = os.environ["API_KEY"]

# This is where we can define the titles that we want to search for
main_titles = [
    "data engineer",
    "data scientist",
    "data analytics",
    "python",
    "machine learning",
    "sql",
    "pandas",
    "front end",
    "back end",
    "full stack",
    "react",
    "angular",
    "vue",
    "software engineer",
    "software developer",
]


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
        "latitude",
        "longitude",
        "city",
        "state
    ]
    """

    appended_results = list()

    for title in main_titles:

        request = requests.get(
            "https://api.adzuna.com/v1/api/jobs/us/search/1",
            params={
                "app_id": app_id,
                "app_key": api_key,
                "results_per_page": "50",
                "what": title,
            },
            headers={"content-type": "application/json"}
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

    # Append "A" to ID so that we know what API it's coming from (this prevents duplicates
    # and makes it so that the IDs from different APIs don't overlap)
    df["id"] = df["id"].apply(lambda x: "A" + str(x))

    # separate city and state
    # make sure that there is more than one value for location (some are only country)
    df["city"] = df["location"].apply(
        lambda x: x[-1].replace(" County", "") if len(x) > 1 else "unknown"
    )
    df["state"] = df["location"].apply(lambda x: x[1] if len(x) > 1 else "uknown")

    df = df.drop(["location"], axis=1)

    return df


def jobsearcher():
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
        "latitude",
        "longitude",
        "city",
        "state
    ]
    """

    jobsearcher_titles = [
        "q[must][0]=" + item.replace(" ", "&q[must][1]=") for item in main_titles
    ]
    appended_results = list()

    for title in jobsearcher_titles:
        """The API only returns 100 listings max.
        This loops through, increasing by 100 each time until all listings are retrived for 
        each individual job title search.
        """

        x = 1
        offset = 0
        while x > 0:
            request = requests.get(
                f"https://api.jobsearcher.com/v1/jobs?{title}",
                params={
                    "limit": 100,
                    "offset": {offset},
                    "from_age": 1,
                    "status": "active",
                    "sortBy": "postedDate",
                    "sortOrder": "desc",
                },
            )

            offset += 100
            result = request.json()

            x = len(result["data"])
            # flatten nested objects
            flattened_results = [
                flatten(job, reducer="underscore") for job in result["data"]
            ]

            # creates a list of tags from the list of dictionaries in 'keywords'
            for item in flattened_results:
                item.update(tags=[n["value"] for n in item["keywords"]])

            # append those flattened results to make a list of flattened results
            # with each index being a different API
            appended_results.append(flattened_results)

            final = [job for api in appended_results for job in api]

        # turn into dataframe
    df = pd.DataFrame.from_dict(final)

    # rename columns
    df = df[
        [
            "id",
            "url",
            "title",
            "title",
            "tags",
            "bullets",
            "company",
            "postedDate",
            "location_latitude",
            "location_longitude",
            "location_city",
            "location_state",
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
        "latitude",
        "longitude",
        "city",
        "state",
    ]
    # removes duplicates from overlapping job title searches
    df.drop_duplicates(subset="id", keep="first", inplace=True)

    df["id"] = df["id"].apply(lambda x: "JS" + str(x))

    return df


def merge_all_apis():
    #     """
    #     Merges all of the dfs!
    #     """
    return pd.concat([adzuna(), jobsearcher()])
