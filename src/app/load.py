import logging
import json
import requests
import os
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import bulk
import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(name)s:%(message)s")


app_id = os.environ["APP_ID"]
api_key = os.environ["API_KEY"]

host = os.environ["AWS_ENDPOINT"]
region = os.environ["REGION"]
service = "es"

class AssumeRoleAWS4Auth(AWS4Auth):
    """
    Subclass of AWS4Auth which accepts botocore credentials as its first argument
    Which allows us to handle assumed role sessions transparently
    """
    def __init__(self, credentials, region, service, **kwargs):
        self.credentials = credentials

        frozen_credentials = self.get_credentials()

        super(AssumeRoleAWS4Auth, self).__init__(
            frozen_credentials.access_key,
            frozen_credentials.secret_key,
            region,
            service,
            session_token=frozen_credentials.token,
            **kwargs
        )

    def get_credentials(self):
        if hasattr(self.credentials, 'get_frozen_credentials'):
            return self.credentials.get_frozen_credentials()
        return self.credentials

    def __call__(self, req):
        if hasattr(self.credentials, 'refresh_needed') and self.credentials.refresh_needed():

            frozen_credentials = self.get_credentials()

            self.access_id = frozen_credentials.access_key
            self.session_token = frozen_credentials.token
            self.regenerate_signing_key(secret_key=frozen_credentials.secret_key)
        return super(AssumeRoleAWS4Auth, self).__call__(req)

    def handle_date_mismatch(self, req):
        req_datetime = self.get_request_date(req)
        new_key_date = req_datetime.strftime('%Y%m%d')

        frozen_credentials = self.get_credentials()

        self.access_id = frozen_credentials.access_key
        self.session_token = frozen_credentials.token
        self.regenerate_signing_key(
            date=new_key_date,
            secret_key=frozen_credentials.secret_key
        )


session = boto3.Session()
credentials = session.get_credentials()
awsauth = AssumeRoleAWS4Auth(credentials, region, service)

es = Elasticsearch(
    hosts=[host],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
)

# This is the previous connection method
# service = "es"
# credentials = boto3.Session().get_credentials()
# awsauth = AWS4Auth(
#     credentials.access_key,
#     credentials.secret_key,
#     region,
#     service,
#     session_token=credentials.token,
# )

# es = Elasticsearch(
#     send_get_body_as="POST",
#     hosts=[host],
#     http_auth=awsauth,
#     use_ssl=True,
#     verify_certs=True,
#     connection_class=RequestsHttpConnection,
# )


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
    logging.info(df)
    logging.info(bulk(es, gendata(df)))
    return bulk(es, gendata(df))
