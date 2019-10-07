from .settings import DEBUG
import logging

def _print(s):
    if DEBUG:
        print(s)
    else:
        logging.info(s)