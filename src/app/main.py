import os
import logging
import time
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
formatter.converter = time.gmtime
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
handlers = [console_handler]
logging.basicConfig(handlers=handlers)

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from .extract import merge_all_apis
from .transform import transform_df
from .load import query

# from .log import start_log, get_log, tail_log

# start_log(get_log(__file__))
# APP_LOG = logging.getLogger(__name__)
# APP_LOG.info('Creating app...')

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# werkzeug_logger = logging.getLogger('werkzeug')
# for handler in APP_LOG.handlers:
# 	werkzeug_logger.addHandler(handler)
# 	application.logger.addHandler(handler)


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
    df = merge_all_apis()
    df = transform_df(df)
    df = query(df)
    return 'Cron job complete'


@app.get("/logs")
def logs():
	"""
	Gets the last n lines of a given log
	"""
	APP_LOG.info(f'/logs called with args {request.args}')
	logfile = request.args.get('file', None)
	lines = request.args.get('lines', 1000)

	if logfile is None:
		return('''
		<pre>
			Parameters:
				file: The file to get logs for
					Required
					Usually one of either application.py or run_scrapers.py
				lines: Number of lines to get
					Defaults to 1000
		</pre>
		''')

	try:
		res = tailLogFile(logfile, n_lines=lines)
		return (f'<pre>{res}</pre>')
	except Exception as e:
		return(f'Exception {type(e)} getting logs: {e}')