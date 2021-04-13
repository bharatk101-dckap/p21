import logging
from pathlib import Path
from app.database.vizb.connection import db_v_connect, load_staging_tables
from app.logs.app_logs import CustomizeLogger
from config import DevelopmentConfig
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from app.data_pull.pull_p21 import p21

# Initialize FastAPI


app = FastAPI(
    title="VIZB-P21 API",
    description="Backend P21-API",
    # version="2.0",
    docs_url="/docs",
    redoc_url=None,
    debug=DevelopmentConfig.DEBUG,
    # swagger="2.0"
)

logger = logging.getLogger(__name__)
config_path = Path(__file__).with_name("log_config.json")
logger = CustomizeLogger.make_logger(config_path)


def init_db():
    cur, conn = db_v_connect()
    load_staging_tables(cur, conn)


def create_app():
    init_db()
    # app.add_middleware(GZipMiddleware)
    app.add_middleware(CORSMiddleware,
                       allow_origins=['*'],
                       allow_credentials=True,
                       allow_methods=["*"],
                       allow_headers=["*"]
)
    app.include_router(p21)
    return app