import json
import requests
import os
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import bulk
import boto3

app_id = os.environ["APP_ID"]
api_key = os.environ["API_KEY"]

host = os.environ["AWS_ENDPOINT"]
region = os.environ["REGION"]

service = "es"
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    service,
    session_token=credentials.token,
)

es = Elasticsearch(
    send_get_body_as="POST",
    hosts=[host],
    #http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
)


def gendata(df):
    for index, row in df.iterrows():
        yield {
            "_op_type": "index",
            "_index": "jobs",
            "_id": row["id"],
            "post_url": row["post_url"],
            "title": row["title"],
            "title_keyword": row["title_keyword"],
            "tags": row["tags"],
            "company": row["company"],
            "description": row["description"],
            "publication_date": row["publication_date"],
            "inserted_date": row["inserted_date"],
            "location_city": row["city"],
            "location_state": row["state"],
            "location_point": f"{row['latitude']},{row['longitude']}",
        }


def query(df):

    return(bulk(es, gendata(df)))
