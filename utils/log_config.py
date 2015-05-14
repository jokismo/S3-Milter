import logging
from cloghandler import ConcurrentRotatingFileHandler
import os
import datetime

default_formatter = logging.Formatter('<%(asctime)s><%(levelno)s>%(message)s')
logger = {}


def init():

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


def set_level(is_dev):
    if is_dev:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logger['root_logger'].setLevel(level)


def timestamp_start():
    return datetime.datetime.now()


def timestamp_end(start):
    return datetime.datetime.now() - start


def log_database_entry(**kwargs):
    substitute_missing(kwargs, ['module_name', 'function_name', 'table_name'])
    log_string = '<DB Update><{module_name}.{function_name}><Table:{table_name}>'.format(**kwargs)
    return _add_log_string_params(kwargs, log_string)


def log_error(**kwargs):
    substitute_missing(kwargs, ['module_name', 'function_name', 'error'])
    log_string = '<ERROR><{module_name}.{function_name}><{error}>'.format(**kwargs)
    return _add_log_string_params(kwargs, log_string)


def log_success(**kwargs):
    substitute_missing(kwargs, ['module_name', 'function_name', 'msg'])
    log_string = '<SUCCESS><{module_name}.{function_name}><{msg}>'.format(**kwargs)
    return _add_log_string_params(kwargs, log_string)


def log_status(**kwargs):
    substitute_missing(kwargs, ['module_name', 'function_name', 'msg'])
    log_string = '<STATUS><{module_name}.{function_name}><{msg}>'.format(**kwargs)
    return _add_log_string_params(kwargs, log_string)


def log_called(**kwargs):
    substitute_missing(kwargs, ['module_name', 'function_name'])
    log_string = '<CALLED><{module_name}.{function_name}>'.format(**kwargs)
    return _add_log_string_params(kwargs, log_string)


def substitute_missing(kwargs, params):
    for param in params:
        if kwargs.get(param) is None:
            kwargs[param] = 'FIELD NOT SUPPLIED'


def _add_log_string_params(kwargs, log_string):
    if kwargs.get('params') is not None:
        param_string = '<'
        for key, value in kwargs['params'].iteritems():
            param_string += str(key) + '=' + str(value) + ' '
        log_string += param_string[:-1] + '>'
    return log_string