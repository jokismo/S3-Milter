import logging
from psycopg2 import connect
from psycopg2 import pool
from psycopg2 import ProgrammingError

from utils.exceptions import MilterException
from utils.log_config import log_error


logger = logging.getLogger(__name__)


def handle_error(function_name, error, name='Postgre Service Error.'):
    raise MilterException(500, name, log_error(module_name=__name__, function_name=function_name,
                                               error=error))


def select_string(config):
    sql_string = 'SELECT {} FROM {table_name}'
    sql_string = sql_string.format('{}', **config)
    select_str = ''
    for var in config['columns']:
        select_str += '{}, '.format(var)
    sql_string = sql_string.format(select_str[:-2])
    return sql_string


def insert_string(config):
    sql_string = 'INSERT INTO {table_name}'
    sql_string = sql_string.format(**config)
    sql_string += ' ({}) VALUES ({})'
    column_names = ''
    values = ''
    for var in config['columns']:
        column_names += '{}, '.format(var)
        values += '%({})s, '.format(var)
    sql_string = sql_string.format(column_names[:-2], values[:-2])
    return sql_string


def update_string(config):
    sql_string = 'UPDATE {table_name} SET '
    sql_string = sql_string.format(**config)
    for var in config['columns']:
        sql_string += '{} = %({})s, '.format(var, var)
    sql_string = sql_string[:-2]
    return sql_string


def statement_type_func_map(statement_type):
    return {
        'insert': insert_string,
        'update': update_string,
        'select': select_string
    }[statement_type]


def add_conditions(config):
    sql_string = ''
    if config.get('conditions') is not None:
        sql_string += ' WHERE'
        for condition in config['conditions']:
            if condition.get('group_open') is not None:
                sql_string += ' ('
            else:
                sql_string += ' '
            sql_string += '{var_name} {operator} %({var_name})s'.format(**condition)
            if condition.get('group_close') is not None:
                sql_string += ')'
            if condition.get('next') is not None:
                sql_string += ' {next}'.format(**condition)
    return sql_string


def add_sorting(config):
    sql_string = ''
    if config.get('sort') is not None:
        sql_string += ' ORDER BY'
        for sort_clause in config['sort']:
            sql_string += ' {var_name},'.format(**sort_clause)
            if sort_clause.get('order') is not None:
                sql_string = sql_string[:-1] + ' {},'.format(sort_clause['order'].upper())
        sql_string = sql_string[:-1]
    return sql_string


def add_returning(config):
    sql_string = ''
    if config.get('return_values') is not None:
        sql_string += ' RETURNING'
        for return_value in config['return_values']:
            sql_string += ' {},'.format(return_value)
        sql_string = sql_string[:-1]
    return sql_string


def sql_config_to_string(config):
    try:
        sql_string = statement_type_func_map(config['type'])(config)
        sql_string += add_conditions(config)
        sql_string += add_sorting(config)
        sql_string += add_returning(config)
        sql_string += ';'
    except Exception as e:
        handle_error('sql_config_to_string', e)
    else:
        return sql_string


def execute_transaction(cursor, conn, trans_config, bind_vars, commit):
    try:
        sql_string = sql_config_to_string(trans_config)
        cursor.execute(sql_string, bind_vars)
        if commit:
            conn.commit()
        try:
            results_list = cursor.fetchall()
            column_names = [c.name for c in cursor.description]
        except ProgrammingError:
            pass
        else:
            return {
                'column_names': column_names,
                'results_list': results_list
            }
    except MilterException as e:
        raise e
    except Exception as e:
        handle_error('transaction', e)


class Postgre(object):

    def __init__(self, creds, is_pool=False, minconn=5, maxconn=10):
        self.conn = None
        self.pool = None
        self.creds = creds
        if is_pool:
            self.init_pool(minconn, maxconn)
        else:
            self.connect()

    def connect(self):
        try:
            self.conn = connect(database=self.creds['database'], user=self.creds['user'],
                                password=self.creds['password'], host=self.creds['host'],
                                port=self.creds['port'])
        except Exception as e:
            handle_error('connect', e)

    def init_pool(self, minconn, maxconn):
        try:
            self.pool = pool.ThreadedConnectionPool(minconn=minconn, maxconn=maxconn,
                                                    database=self.creds['database'],
                                                    user=self.creds['user'],
                                                    password=self.creds['password'],
                                                    host=self.creds['host'], port=self.creds['port'])
        except Exception as e:
            handle_error('connect', e)

    def transaction(self, trans_config, bind_vars=None, commit=False):
        try:
            if self.pool is not None:
                try:
                    conn = self.pool.getconn()
                    cursor = conn.cursor()
                except Exception as e:
                    if str(e) == 'connection pool exhausted':
                        return self.one_time_connection(trans_config, bind_vars)
                    raise e
            else:
                conn = self.conn
                if conn.closed != 0:
                    self.connect()
                    conn = self.conn
                cursor = conn.cursor()
        except Exception as e:
            handle_error('transaction', '<Connection Fail>' + str(e))
        else:
            return execute_transaction(cursor, conn, trans_config, bind_vars, commit)

    def one_time_connection(self, trans_config, bind_vars=None, commit=False):
        with connect(database=self.creds['database'], user=self.creds['user'],
                     password=self.creds['password'], host=self.creds['host'],
                     port=self.creds['port']) as conn:
            cursor = conn.cursor()
            return execute_transaction(cursor, conn, trans_config, bind_vars, commit)

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        if self.pool is not None:
            self.pool.closeall()
            self.pool = None