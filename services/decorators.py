import logging
from functools import wraps
from pathlib import Path
import time

# create logs folder if it doesn't already exist, if it does exist then it doesn't do anything
Path("logs").mkdir(exist_ok=True)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

def setup_logger(name: str) -> logging.Logger:
    """Initializes and configures app_logger to be used throughout the application:
    Args:
        name: Str - Name of the logger
    Returns:
        logger.Logger: A configured logger instance
    """

    # initialize logger
    logger = logging.getLogger(name)

    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    log_file_name = ''
    handler_level = None
    if(name == "app"):
        log_file_name = 'logs/app.log'
        handler_level = logging.INFO
    elif(name == 'audit'):
        log_file_name = 'logs/audit_logs.txt'
        handler_level = logging.DEBUG
    else:
        raise 
    handler = logging.FileHandler(log_file_name, mode='a', encoding='utf-8')
    handler.setFormatter(formatter)
    handler.setLevel(handler_level)

    logger.addHandler(handler)

    return logger

# initialize loggers
app_logger = setup_logger('app')
audit_logger = setup_logger('audit')

def log_to_app(func):
    """Creates basic logs for the general application with more specific logs being created
    in the func itself
    Args:
        func: The function to create logs for
    """
    @wraps(func)
    def storage_wrapper(*args, **kwargs):
        app_logger.info(f"[{func.__name__}] Starting execution with args={args}, kwargs={kwargs}")
        try:
            result = func(*args, **kwargs)
            app_logger.info(f"[{func.__name__}] Completed successfully")
            return result
        except Exception as e:
            app_logger.error(f"[{func.__name__}] Failed with {type(e).__name__}: {e}")
            raise Exception
    return storage_wrapper

def log_to_audit(func):
    """Creates basic logs for the audit file storing the hashes of objects uploaded to GCS
    Args:
        func: the function comparing the checksum hashes
    """
    @wraps(func)
    def audit_wrapper(*args, **kwargs):
        try:
            hash_hit, hash = func(*args, **kwargs)
            if(hash_hit):
                audit_logger.info(f"Prevented uploading bundle with hash: {hash}")
            else:
                audit_logger.info(f"Attempting to upload bundle")
            return hash_hit, hash
        except Exception:
            raise Exception
    return audit_wrapper