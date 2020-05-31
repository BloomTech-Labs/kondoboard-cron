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
            f"https://api.adzuna.com/v1/api/jobs/us/search/1?app_id={app_id}&app_key={api_key}&results_per_page=50&what={title}&content-type=application/json"
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

    df_a = pd.DataFrame.from_dict(final)

    # rename columns
    df_a = df_a[
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
    df_a.columns = [
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
    df_a["id"] = df_a["id"].apply(lambda x: "A" + str(x))

    # separate city and state
    # make sure that there is more than one value for location (some are only country)
    df_a["city"] = df_a["location"].apply(
        lambda x: x[-1].replace(" County", "") if len(x) > 1 else "unknown"
    )
    df_a["state"] = df_a["location"].apply(lambda x: x[1] if len(x) > 1 else "uknown")

    df_a = df_a.drop(["location"], axis=1)

    return df_a

adzuna()


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

    jobsearcher_titles = [item.replace(" ", "&q[must][1]=") for item in main_titles]
    jobsearcher_titles = ["q[must][0]=" + item for item in jobsearcher_titles]
    appended_results = list()

    for title in jobsearcher_titles:

        # make the requests to JobSearcher API
        offset = 0
        request = requests.get(
            f"https://api.jobsearcher.com/v1/jobs?{title}&status=active&limit=100&type=organic&offset={offset}&distance=25&collapse=companyNameAndState&from_age=1&sortBy[0]=postedDate&sortOrder[0]=desc"
        )

        # convert result into json
        result = request.json()
        while len(result['data']) > 0:
            offset += 100
            request = requests.get(
            f"https://api.jobsearcher.com/v1/jobs?q[must][0]=data&q[must][1]=scientist&status=active&limit=100&type=organic&offset={offset}&distance=25&collapse=companyNameAndState&from_age=1&sortBy[0]=postedDate&sortOrder[0]=desc"
            )
            result = request.json()
            
        # flatten nested objects
            flattened_results = [
                flatten(job, reducer="underscore") for job in result["data"]
            ]
        
        #creates a list of tags from the list of dictionaries in 'keywords'
            for item in flattened_results:
                item.update(tags = [n['value'] for n in item['keywords']])
            
        # append those flattened results to make a list of flattened results
        # with each index being a different API
            appended_results.append(flattened_results)

        # turn 2D list with different API calls into a 1D list to be put into df
            final = [job for api in appended_results for job in api]

        # turn into dataframe
    df_js = pd.DataFrame.from_dict(final)

    # rename columns
    df_js = df_js[
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

    df_js.columns = [
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

    df_js['id'] = df_js['id'].apply(
        lambda x: "JS" + str(x)
    )
    
    return df_js


def all_apis():
#     """
#     Merges all of the dfs!
#     """
    return pd.concat([adzuna(), jobsearcher()])

all_apis()

#     adzuna_df = adzuna()
#     # merge
