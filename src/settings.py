from pathlib import Path
from loguru import logger

class Settings():
    basedir = Path.cwd()
    processeddir = Path("../data/raw/")
    logdir = basedir / "log"
    servername = 'LAPTOP-4SN45QMD\SQLEXPRESS'
    database = 'UnitedOutdoors'

settings = Settings()
logger.add('logfile.log')