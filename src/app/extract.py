import logging
import os
import requests
import pandas as pd
from flatten_dict import flatten
import psycopg2
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(name)s:%(message)s")

app_id = os.environ["APP_ID"]
api_key = os.environ["API_KEY"]

# credentials for monster postgres
# DB_NAME = os.environ["DB_NAME"]
# DB_USER = os.environ["DB_USER"]
# DB_PASSWORD = os.environ["DB_PASSWORD"]
# DB_HOST = os.environ["DB_HOST"]

# This is where we can define the titles that we want to search for
main_titles = [
    "data engineer",
    # "data scientist",
    # "data analytics",
    # "python",
    # "machine learning",
    # "sql",
    # "pandas",
    # "front end",
    # "back end",
    # "full stack",
    # "react",
    # "angular",
    # "vue",
    # "software engineer",
    # "software developer",
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
        "state,
    ]
    """

    appended_results = list()

    for title in main_titles:

        logging.info("=" * 20)
        logging.info(f"Adzuna request for {title}:")

        request = requests.get(
            "https://api.adzuna.com/v1/api/jobs/us/search/1",
            params={
                "app_id": app_id,
                "app_key": api_key,
                "results_per_page": "50",
                "what": title,
            },
            headers={"content-type": "application/json"},
        )

        result = request.json()

        logging.info(f" Number of jobs pulled :{len(result['results'])}")

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
    df["description"] = df["description"].apply(lambda x: [x])

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
        "state,
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

        logging.info(f"JobSearcher request for {title}:")

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

            logging.info(f"Request code: {request}")

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


# def monster_scraper():
#     try:
#         today = print(date.today())
#         connection = psycopg2.connect(
#             dbname="DB_NAME", user="DB_USER", password="DB_PASSWORD", host="DB_HOST"
#         )
#         print("CONNECTION:", connection)
#         cursor = connection.cursor()
#         print(connection.get_dsn_parameters(), "\n")

#         cursor.execute("SELECT version();")
#         record = cursor.fetchone()
#         print("you are connected to - ", record, "\n")

#         query = """
#         SELECT
#     	    job_listings.id,
#     	    "post_date_utc",
#     	    "title",
#     	    "city",
#     	    "state_province",
#     	    "external_url",
#     	    job_descriptions.description,
#     	    companies.name
#         FROM
#     	    job_listings
#     	    FULL OUTER JOIN job_locations ON job_listings.id = job_locations.job_id
#     	    FULL OUTER JOIN locations ON job_locations.location_id = locations.id
#     	    FULL OUTER JOIN job_links ON job_listings.id = job_links.job_id
#     	    FULL OUTER JOIN job_descriptions ON job_listings.id = job_descriptions.job_id
#     	    FULL OUTER JOIN job_companies ON job_listings.id = job_companies.job_id
#     	    FULL OUTER JOIN companies ON job_companies.company_id = companies.id
#         WHERE
#     	    "post_date_utc" > '{today}'
#         ORDER BY
#     	    "post_date_utc" ASC
#             """.format(
#             today=str(date.today())
#         )

#         cursor.execute(query)
#         result = cursor.fetchall()
#         print("RESULT:", len(result))

#         job_list = []
#         for row in result:
#             list(row)
#             for x in row:
#                 x = {
#                     "id": row[0],
#                     "publication_date": row[1],
#                     "title": row[2],
#                     "title_keyword": row[2],
#                     "city": row[3],
#                     "state": row[4],
#                     "post_url": row[5],
#                     "description": row[6],
#                     "company": row[7],
#                 }
#             job_list.append(x)

#         df = pd.DataFrame.from_dict(job_list)
#         df["id"] = df["id"].apply(lambda x: "MS" + str(x))

#         return df

#     except (Exception, psycopg2.Error) as error:
#         print("Error while connecting to PostgreSQL", error)
#     finally:
#         if connection:
#             cursor.close()
#             connection.close()
#             print("PostgreSQL connection is closed")
