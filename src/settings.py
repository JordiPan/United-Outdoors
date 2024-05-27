from pathlib import Path
from loguru import logger

class Settings():
    basedir = Path.cwd()
    processeddir = Path("../data/raw/")
    logdir = basedir / "log"
<<<<<<< HEAD
    servername = 'LAPTOP-4SN45QMD\SQLEXPRESS'
=======
    servername = 'LAPTOP-C1FMPSTV\SQLEXPRESS01'
>>>>>>> dashbord
    database = 'UnitedOutdoors'

settings = Settings()
logger.add('logfile.log')