import json
import requests
import os

app_id = os.environ["APP_ID"]
api_key = os.environ["API_KEY"]

es_user = os.environ["ES_USER"]
es_password = os.environ["ES_PASSWORD"]
es_endpoint = os.environ["ES_ENDPOINT"]

uri = f"https://{es_user}:{es_password}@{es_endpoint}/jobs/_bulk"


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
                "location_city": row["city"],
                "location_state": row["state"],
                "location_point": f"{row['latitude']},{row['longitude']}",
            }
        )

    print(query)

    # DO THIS INSTEAD
    # Not working because it isn't a string....
    # Will come back to this later
    # data = "\n".join(query)

    with open("bulk_query.json", "w") as f:
        for item in query:
            f.write(f"{json.dumps(item)}\n")

    with open("./bulk_query.json", "r") as f:
        data = f.read()

    try:
        r = requests.post(uri, headers=headers, data=data)
        print(r)
    except Exception:
        raise
