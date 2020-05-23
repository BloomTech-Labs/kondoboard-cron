import requests
import pandas as pd
from flatten_dict import flatten

from dotenv import load_dotenv
import os

load_dotenv()

app_id = os.getenv("APP_ID")
api_key = os.getenv("API_KEY")

def request_to_df():
    # make API call
    request = requests.get(f"""https://api.adzuna.com/v1/api/jobs/us/search/1?
                            app_id={app_id}&
                            app_key={api_key}&
                            results_per_page=20&
                            what=data&
                            content-type=application/json""")
    return request

    # # flatten 
    # flattened_results =[
    #     flatten(result, reducer="underscore")
    #     for result in 
    #     request['results']
    # ]

    # df = pd.DataFrame.from_dict(flattened_results)

    # df = df[['id', 'redirect_url', 'title', 'title', 'category_tag', 'description', 'created', 'location_area', 'latitude', 'longitude']]
    # df.columns = ['id', 'post_url', 'title', 'title_keyword', 'tags', 'description', 'publication_date', 'location', 'latitude', 'longitude']

    # return df