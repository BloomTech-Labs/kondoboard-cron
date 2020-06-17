from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import logging
from .extract import adzuna, jobsearcher, monster_scraper
from .transform import transform_df
from .load import query

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(name)s:%(message)s")

@app.get("/")
async def root():
    """
    Kondoboard Cron Deployed
    """
    return HTMLResponse(
        """
    <h1>Kondoboard Cron Deployed</h1>
    <p>Go to <a href="/docs">/docs</a> for documentation.</p>
    """
    )


@app.get("/start")
def start_upload():
    """
    Start the cron task to upload new jobs to the elasticsearch database
    """

    sources ={
        "adzuna":{
            "extract_func": adzuna,
       },
       "jobsearcher":{
           "extract_func": jobsearcher,
       },
       "monster":{
           "extract_func": monster_scraper
       }
    }

    for k, v in sources.items():
        print(k)
        try:
            # extract
            df = v["extract_func"]()
            logging.info(f"{k} extraced layer complete")
            # transform
            transformed_df = transform_df(df)
            logging.info(f"{k} transform layer complete")
            # load
            query(transformed_df)
            logging.info(f"{k} loaded layer complete")
        except Exception:
            logging.error(f"{k} unable to complete")
