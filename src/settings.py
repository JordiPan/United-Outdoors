from pathlib import Path
from loguru import logger

class Settings():
    basedir = Path.cwd()
    processeddir = Path("../data/raw/")
    logdir = basedir / "log"
    servername = 'LAPTOP-RPEMJO16\SQLEXPRESS'
    database = 'UnitedOutdoors'

settings = Settings()
logger.add('logfile.log')