import json
import requests

from dotenv import load_dotenv
import os

load_dotenv()

app_id = os.getenv("APP_ID")
api_key = os.getenv("API_KEY")

es_user = os.getenv("ES_USER")
es_password = os.getenv("ES_PASSWORD")

es_host = f"https://{es_user}:{es_password}@b6ea0151e3bd4dcea32dc74a1093ba45.us-east-1.aws.found.io:9243/jobs/_bulk"


def load_query(df):
    headers = {"Content-Type": "application/json"}
    query = list()

    for index, row in df.iterrows():
        query.append({"index": {"_id": row["id"]}})
        query.append(
            {
                "post_url": row["post_url"],
                "title": row["title"],
                "title_keyword": row["title_keyword"],
                "tags": row["tags"],
                "company": row["company"],
                "description": row["description"],
                "publication_date": row["publication_date"],
                "location_raw": row["location"],
                "location_city": row["city"],
                "location_state": row["state"],
                "location_point": f"{row['latitude']},{row['longitude']}",
            }
        )

    with open("bulk_query.json", "w") as f:
        for item in query:
            f.write(f"{json.dumps(item)}\n")

    with open("./bulk_query.json", "r") as f:
        data = f.read()
        print(data)

    try:
        requests.post(es_host, headers=headers, data=data)
        print("Woooooooot!")
    except Exception:
        raise
