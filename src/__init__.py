import time
from loguru import logger

start = time.time()
from src import config  # noqa: F401

logger.info(f"Imported config in {time.time() - start:.2f} seconds.")
