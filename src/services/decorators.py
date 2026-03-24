import logging
from functools import wraps


logging.basicConfig(
    filename='logs/app.log',
    filemode='a',
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
    level='INFO'
)
logger = logging.getLogger(__name__)

def app_logger(func):
    @wraps(func)
    def storage_wrapper(*args, **kwargs):
        logger.info(f"[{func.__name__}] Starting execution with args={args}, kwargs={kwargs}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"[{func.__name__}] Completed successfully")
            return result
        except Exception as e:
            logger.error(f"[{func.__name__}] Failed with {type(e).__name__}: {e}")
            raise
    return storage_wrapper