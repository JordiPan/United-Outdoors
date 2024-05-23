from pathlib import Path
from loguru import logger

class Settings():
    basedir = Path.cwd()
    processeddir = Path("../data/raw/")
    logdir = basedir / "log"

settings = Settings()
logger.add('logfile.log')