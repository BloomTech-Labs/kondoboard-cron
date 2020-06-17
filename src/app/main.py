from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from .extract import adzuna, jobsearcher #, monster_scraper
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
def start_upload():  # async
    """
    Start the cron task to upload new jobs to the elasticsearch database
    """
    df_adzuna = adzuna()
    #df_jobsearcher = jobsearcher()
    #df_monster = monster_scraper()

    transformed_adzuna = transform_df(df_adzuna)
    #transformed_jobsearcher = transform_df(df_jobsearcher)
    #transformed_monstser = transform_df(df_monster)

    query(transformed_adzuna)
    #query(transformed_jobsearcher)
    #query(transformed_monstser)
    return "Cron job complete"
