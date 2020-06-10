from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware



app = FastAPI()

from .extract import merge_all_apis
from .transform import transform_df
from .load import query

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
    """)


@app.get("/start")
def start_upload(): #async 
    """
    Start the cron task to upload new jobs to the elasticsearch database
    """
    df = merge_all_apis()
    df = transform_df(df)
    df = query(df)
    return 'Cron job complete'