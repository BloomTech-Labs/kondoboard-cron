from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

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
    return HTMLResponse("""
    <h1>Kondoboard Cron Deployed</h1>
    <p>Go to <a href="/docs">/docs</a> for documentation.</p>
    """)