from ETL.transform import transform

# get df

def load():
    query = list()
    
    for index, row in df.iterrows():
        query.append({"index": {"_id" : row['id']}})
    query.append({"post_url": row["post_url"], "title":row["title"], "title_keyword":row["title_keyword"], "tags":row["tags"], "description":row["description"], "publication_date":row["publication_date"], "location_raw":row["location"], "location_point":f"{row['latitude']},{row['longitude']}"})

    with open("./data/bulk_query.json", 'w') as f:
    for item in query:
        f.write(f'{json.dumps(item)}\n')
    