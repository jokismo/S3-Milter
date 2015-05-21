from psycopg2 import connect
from psycopg2 import pool
from psycopg2 import ProgrammingError
from contextlib import contextmanager

from utils.exceptions import MilterException
from utils.log_config import log_error


def handle_error(function_name, error, name='Postgre Service Error.'):
    if isinstance(error, MilterException):
        raise error
    else:
        error = str(error)
        error = error.replace('\n', ' ')
        raise MilterException(500, name, log_error(module_name=__name__, function_name=function_name,
                                                   error=error))


def list_to_list_of_dicts(column_names, results):
    return [dict(zip(column_names, values)) for values in results]


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


def delete_string(config):
    return 'DELETE FROM {table_name}'.format(**config)


def statement_type_func_map(statement_type):
    return {
        'insert': insert_string,
        'update': update_string,
        'select': select_string,
        'delete': delete_string
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


def transaction(conn, trans_config, bind_vars=None, return_dicts=False):
    try:
        cursor = conn.cursor()
        sql_string = sql_config_to_string(trans_config)
        cursor.execute(sql_string, bind_vars)
        try:
            results_list = cursor.fetchall()
            column_names = [c.name for c in cursor.description]
        except ProgrammingError:
            return 'No Return Values.'
        else:
            if return_dicts:
                return list_to_list_of_dicts(column_names, results_list)
            else:
                return {
                    'column_names': column_names,
                    'results_list': results_list
                }
    except Exception as e:
        handle_error('transaction', e)


def commit(conn):
    try:
        conn.commit()
    except Exception as e:
        handle_error('commit', e)


def rollback(conn):
    try:
        conn.rollback()
    except Exception as e:
        handle_error('commit', e)


def get_dsn(params):
    items = [(k, v) for (k, v) in params.iteritems()]
    return " ".join(["%s=%s" % (k, str(v)) for (k, v) in items])


class Postgre(object):

    def __init__(self, creds, is_pool=False, minconn=5, maxconn=10):
        self.conn = None
        self.pool = None
        self.creds = creds
        if is_pool:
            self.init_pool(minconn, maxconn)
        else:
            self.conn = self.make_connection()

    def init_pool(self, minconn, maxconn):
        try:
            self.pool = pool.SimpleConnectionPool(minconn=minconn, maxconn=maxconn,
                                                  dsn=get_dsn(self.creds))
        except Exception as e:
            handle_error('init_pool', e)

    @contextmanager
    def get_connection(self):
        got_from_pool = False
        temporary = False
        try:
            if self.pool is not None:
                try:
                    conn = self.pool.getconn()
                    got_from_pool = True
                except Exception as e:
                    if str(e) == 'connection pool exhausted':
                        conn = self.make_connection()
                        temporary = True
                    else:
                        raise e
            else:
                conn = self.conn
                if conn.closed != 0:  # Non zero means closed or problem
                    self.conn = self.make_connection()
                    conn = self.conn
        except Exception as e:
            handle_error('transaction', '<Connection Fail>' + str(e))
        else:
            yield conn
        finally:
            try:
                if got_from_pool:
                    self.pool.putconn(conn)
            except Exception as e:
                handle_error('transaction', '<Pool Return Fail>' + str(e))
            try:
                if temporary:
                    conn.close()
            except Exception as e:
                handle_error('transaction', '<Temporary Connection Close Fail>' + str(e))

    def make_connection(self):
        try:
            return connect(dsn=get_dsn(self.creds))
        except Exception as e:
            handle_error('connect', e)

    def close(self):
        try:
            if self.conn is not None:
                self.conn.close()
                self.conn = None
            if self.pool is not None:
                self.pool.closeall()
                self.pool = None
        except Exception as e:
            handle_error('close', e)

    def disconnect_on_exit(self):
        try:
            self.close()
        except:
            try:
                self.close()
            except BaseException as e:
                handle_error('disconnect_on_exit', e)
        finally:
            raise SystemExit