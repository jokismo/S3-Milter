import logging
from psycopg2 import connect
from psycopg2 import pool

from utils.exceptions import MilterException
from milter_config import postgresql_creds
from utils.log_config import log_error


logger = logging.getLogger(__name__)


def handle_error(function_name, error):
    raise MilterException(500, 'S3 Service Error.', log_error(module_name=__name__, function_name=function_name,
                                                              error=error))


def path_array_to_string(path_array):
    path_string = ''
    for path in path_array:
        path_string += path + '/'
    return path_string


def sql_config_to_string(sql_string, config, bind_vars):
    if config.get('conditions') is not None:
        sql_string += ' WHERE'
        for condition in config['conditions']:
            if condition.get('group_open') is not None:
                sql_string += '('
            sql_string += ' {var_name} {operator} %({var_name})s'.format(**condition)
            if condition.get('group_close') is not None:
                sql_string += ')'
            if condition.get('next') is not None:
                sql_string += ' {var_name}'.format(**condition)
    if config.get('sort') is not None:
        sql_string += 'ORDER BY'
        for sort_clause in config['sort']:
            sql_string += ' {var_name},'
        sql_string = sql_string[:-1]
    if config.get('return_values') is not None:
        sql_string += '  RETURNING'
        for return_value in config['return_values']:
            ' {}'.format(return_value)


class Postgre(object):

    def __init__(self, is_pool=False, minconn=5, maxconn=10):
        self.conn = None
        self.pool = None
        if is_pool:
            self.init_pool(minconn, maxconn)
        else:
            self.connect()

    def connect(self):
        try:
            self.conn = connect(database=postgresql_creds['database'], user=postgresql_creds['user'],
                                password=postgresql_creds['password'], host=postgresql_creds['host'],
                                port=postgresql_creds['port'])
        except Exception as e:
            handle_error('connect', e)

    def init_pool(self, minconn, maxconn):
        try:
            self.pool = pool.ThreadedConnectionPool(minconn=minconn, maxconn=maxconn,
                                                    database=postgresql_creds['database'],
                                                    user=postgresql_creds['user'],
                                                    password=postgresql_creds['password'],
                                                    host=postgresql_creds['host'], port=postgresql_creds['port'])
        except Exception as e:
            handle_error('connect', e)

    def transaction(self, trans_type, trans_config, bind_vars=None):
        if self.pool is not None:
            try:
                conn = self.pool.getconn()
                return getattr(self, trans_type)(trans_config, bind_vars, conn)
            except Exception as e:
                if str(e) == 'connection pool exhausted':
                    return self.non_pool_transaction(trans_type, trans_config, bind_vars)
                handle_error('connect', e)
        else:
            return getattr(self, trans_type)(trans_config, bind_vars, self.conn)

    def non_pool_transaction(self, trans_type, trans_config, bind_vars=None):
        with connect(database=postgresql_creds['database'], user=postgresql_creds['user'],
                     password=postgresql_creds['password'], host=postgresql_creds['host'],
                     port=postgresql_creds['port']) as conn:
            pass

    def insert(self, config, bind_vars, conn):
        sql_string = 'INSERT INTO {table_name} ({variables}) VALUES ({values})'


if __name__ == '__main__':
    postgre = Postgre(is_pool=True, minconn=1, maxconn=2)
    try:
        conn_one = postgre.pool.getconn()
        print len(postgre.pool._used)
        conn_two = postgre.pool.getconn()
        print len(postgre.pool._used)
        conn_three = postgre.pool.getconn()

    except Exception as e:
        print e
        print str(e) == 'connection pool exhausted'
    finally:
        postgre.pool.closeall()
        # postgre.conn.close()