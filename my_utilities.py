import json
import logging
from logging.handlers import RotatingFileHandler
from functools import wraps
from urllib.parse import urlparse

def retry(retry_num: int):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(retry_num):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

            # If all retries failed
            raise last_exception

        return wrapper
    return decorator


def get_configs(config_type: str):
    with open("config.json") as f: config_data = json.load(f)
    return config_data.get(config_type)


def setup_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Rotating file handler (10MB max, 5 backups)
    file_handler = RotatingFileHandler(
        'scraper.log',
        maxBytes=10 * 1024 * 1024,
        backupCount=5
    )
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter('%(levelname)s: %(message)s')
    )

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def get_domain(url: str) -> str:
    parsed = urlparse(url)
    domain = parsed.netloc

    # handle URLs without scheme (e.g. "example.com/path")
    if not domain:
        parsed = urlparse("http://" + url)
        domain = parsed.netloc

    # remove leading "www."
    if domain.startswith("www."):
        domain = domain[4:]

    return domain