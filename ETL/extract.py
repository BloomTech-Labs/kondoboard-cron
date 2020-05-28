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

def adzuna():
    title = ["data%20engineer"]
    request = requests.get(f"https://api.adzuna.com/v1/api/jobs/us/search/1?app_id={app_id}&app_key={api_key}&results_per_page=20&what={title}&content-type=application/json")
    result = request.json()
    
    # flatten nested objects
    flattened_results =[
        flatten(job, reducer="underscore")
        for job in 
        result['results']
    ]

    # turn into dataframe
    df = pd.DataFrame.from_dict(flattened_results)

    # rename columns
    df = df[['id', 'redirect_url', 'title', 'title', 'category_tag', 'description', 'company_display_name', 'created', 'location_area', 'latitude', 'longitude']]
    df.columns = ['id', 'post_url', 'title', 'title_keyword', 'tags', 'description', 'company', 'publication_date', 'location', 'latitude', 'longitude']

    # separate city and state
    df['city'] = df['location'].apply(lambda x: x[-1].replace(' County', ''))
    df['state'] = df['location'].apply(lambda x: x[1])
    
    return df

def merge_dfs():
    """
    Merges all of the dfs!
    """
    adzuna_df = adzuna()
    # merge
    