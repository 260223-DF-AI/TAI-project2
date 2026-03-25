import logging
from functools import wraps
from pathlib import Path
import time

Path("logs").mkdir(exist_ok=True)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

def setup_logger(name: str) -> logging.Logger:
    """Initializes and configures app_logger to be used throughout the application:
    Args:
        name: Str, Name of the logger
    Returns:
        logger.Logger: A configured logger instance """

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
    @wraps(func)
    def storage_wrapper(*args, **kwargs):
        app_logger.info(f"[{func.__name__}] Starting execution with args={args}, kwargs={kwargs}")
        try:
            result = func(*args, **kwargs)
            app_logger.info(f"[{func.__name__}] Completed successfully")
            return result
        except Exception as e:
            app_logger.error(f"[{func.__name__}] Failed with {type(e).__name__}: {e}")
            raise
    return storage_wrapper

def log_to_audit(func):
    @wraps(func)
    def audit_wrapper(*args, **kwargs):
        try:
            hash_hit, hash = func(*args, **kwargs)
            if(hash_hit):
                audit_logger.info(f"Prevented uploading bundle with hash: {hash}")
            else:
                audit_logger.info(f"Attempting to upload bundle with hash: {hash}")
            return hash_hit, hash
        except Exception as e:
            raise
    return audit_wrapper