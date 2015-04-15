import logging
from cloghandler import ConcurrentRotatingFileHandler
import os

default_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
logger = {}


def init(is_dev=False):

    logger['root_logger'] = logging.getLogger()

    logfile = os.path.abspath('debug.log')
    debug_handler = ConcurrentRotatingFileHandler(logfile, 'a', 10485760, 5)
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(default_formatter)
    logger['root_logger'].addHandler(debug_handler)

    logfile = os.path.abspath('info.log')
    info_handler = ConcurrentRotatingFileHandler(logfile, 'a', 10485760, 5)
    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(default_formatter)
    logger['root_logger'].addHandler(info_handler)

    logfile = os.path.abspath('error.log')
    error_handler = ConcurrentRotatingFileHandler(logfile, 'a', 10485760, 5)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(default_formatter)
    logger['root_logger'].addHandler(error_handler)

    set_level(is_dev)


def set_level(is_dev):
    if is_dev:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logger['root_logger'].setLevel(level)