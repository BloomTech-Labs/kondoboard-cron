import os
import logging
import time

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from .extract import merge_all_apis
from .transform import transform_df
from .load import query

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(name)s:%(message)s")

app = FastAPI()
logging.info("=" * 50)
logging.info("Starting FastAPI")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """
    Kondoboard Cron Deployed
    """
    return HTMLResponse("""
    <h1>Kondoboard Cron Deployed</h1>
    <p>Go to <a href="/docs">/docs</a> for documentation.</p>
    <br>
	Application logs: <a href="/logs?file=application.py&amp;lines=50">/logs?file=application.py</a>
    """)


@app.get("/start")
def start_upload(): #async 
    """
    Start the cron task to upload new jobs to the elasticsearch database
    """
	logging.info("=" * 50)
	logging.info("/start endpoint has been hit")
    df = merge_all_apis()
	logging.info("merge_all_api completed successfully")
    df = transform_df(df)
	logging.info("transform completed successfully")
    df = query(df)
	logging.info("query and job upload completed successfully")
    return 'Cron job complete'


@app.get("/logs")
def logs():
	"""
	Gets the last n lines of a given log
	"""
	return HTMLResponse("""
    <h1>Log File Page</h1>
    <br>
	Application logs: <a href="/logs?file=main.py&amp;lines=50">/logs?file=main.py</a>
    """)
	