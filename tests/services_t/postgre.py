from services import postgre
from milter_config import postgresql_creds
from utils.exceptions import MilterException
from threading import Thread
from multiprocessing import Queue


def thread_tester(pool, queue):
    while True:
        test_item = queue.get()
        if not test_item:
            pool.putconn(conn)
            break
        conn = pool.getconn()
        assert conn.closed == 0
        assert len(pool._used) == 2


class PostgreTest(object):

    def __init__(self, test_vars):
        self.vars = test_vars

    def run_offline_tests(self):
        test_delete_string()
        test_select_string()
        test_update_string()
        test_insert_string()
        test_add_conditions()
        test_add_sorting()
        test_add_returning()
        test_sql_config_to_string()
        test_list_to_list_of_dicts()

    def run_networked_tests(self):
        self.dbo = postgre.Postgre(self.vars['creds'])
        self.test_connect()
        self.test_pool()
        self.test_close()
        self.dbo = postgre.Postgre(self.vars['creds'])
        self.test_get_connection()
        self.test_transaction()

    def test_connect(self):
        # Test connected
        assert self.dbo.conn.closed == 0
        # Test fake creds
        try:
            fail_dbo = postgre.Postgre(self.vars['wrong_creds'])
        except MilterException:
            assert True
        else:
            fail_dbo.close()
            assert False

    def test_pool(self):
        # Test get connection
        self.dbo.init_pool(minconn=1, maxconn=2)
        conn_one = self.dbo.pool.getconn()
        assert conn_one.closed == 0
        # Test get connection in thread
        test_queue = Queue(maxsize=0)
        conn_user_thread = Thread(target=thread_tester, args=(self.dbo.pool, test_queue))
        conn_user_thread.start()
        test_queue.put(1)
        test_queue.put(None)
        conn_user_thread.join()
        # Test max exceeded
        conn_two = self.dbo.pool.getconn()
        try:
            conn_three = self.dbo.pool.getconn()
        except Exception as e:
            assert str(e) == 'connection pool exhausted'
        else:
            assert False
        self.dbo.pool.putconn(conn_one)
        self.dbo.pool.putconn(conn_two)

    def test_close(self):
        self.dbo.close()
        assert self.dbo.conn is None
        assert self.dbo.pool is None

    def test_get_connection(self):
        # Test with self.conn
        with self.dbo.get_connection() as conn:
            assert conn.closed == 0
        assert self.dbo.conn.closed == 0
        self.dbo.init_pool(minconn=1, maxconn=2)
        self.dbo.conn.close()
        conn_one = self.dbo.pool.getconn()
        # Test with pool
        with self.dbo.get_connection() as conn:
            assert conn.closed == 0
        conn_two = self.dbo.pool.getconn()
        # Pool is full test temporary conn
        assert len(self.dbo.pool._used) == 2
        with self.dbo.get_connection() as conn:
            assert conn.closed == 0
        self.dbo.pool.closeall()
        self.dbo.pool = None
        # Test reopen self.conn
        with self.dbo.get_connection() as conn:
            assert conn.closed == 0
        assert self.dbo.conn.closed == 0

    def test_transaction(self):
        try:
            # Test insert and returning
            with self.dbo.get_connection() as conn:
                byt = 12345
                t = postgre.transaction(conn,
                                        {
                                            'type': 'insert',
                                            'table_name': 'attachments',
                                            'columns': ['file_name', 'folder', 'sender_id', 'receiver_id', 'bytes'],
                                            'return_values': ['bytes']
                                        },
                                        bind_vars={
                                            'file_name': 'a',
                                            'folder': 'b',
                                            'sender_id': 'postgre_test_sender',
                                            'receiver_id': 'postgre_test_receiver',
                                            'bytes': byt
                                        }, return_dicts=True)
                postgre.commit(conn)
                assert t[0]['bytes'] == byt
            # Test select and order
            with self.dbo.get_connection() as conn:
                t = postgre.transaction(conn,
                                        {
                                            'type': 'select',
                                            'table_name': 'attachments',
                                            'columns': ['*'],
                                            'sort': [
                                                {
                                                    'var_name': 'sender_id'
                                                },
                                                {
                                                    'var_name': 'receiver_id',
                                                    'order': 'asc'
                                                }
                                            ]
                                        })
                assert len(t['results_list']) == 1
            # Test update and no return and conditions
            file_name = 'c'
            with self.dbo.get_connection() as conn:
                t = postgre.transaction(conn,
                                        {
                                            'type': 'update',
                                            'table_name': 'attachments',
                                            'columns': ['file_name', 'folder'],
                                            'conditions': [
                                                {
                                                    'group_open': True,
                                                    'var_name': 'folder',
                                                    'operator': '=',
                                                    'next': 'or'
                                                },
                                                {
                                                    'group_close': True,
                                                    'var_name': 'sender_id',
                                                    'operator': '=',
                                                    'next': 'and'
                                                },
                                                {
                                                    'var_name': 'receiver_id',
                                                    'operator': '='
                                                }
                                            ]
                                        },
                                        bind_vars={
                                            'file_name': file_name,
                                            'folder': 'b',
                                            'sender_id': 'postgre_test_sender',
                                            'receiver_id': 'postgre_test_receiver'
                                        })
                assert t == 'No Return Values.'
                postgre.commit(conn)
            with self.dbo.get_connection() as conn:
                t = postgre.transaction(conn,
                                        {
                                            'type': 'select',
                                            'table_name': 'attachments',
                                            'columns': ['file_name']
                                        }, return_dicts=True)
                assert t[0]['file_name'] == file_name
            # Test delete and conditions
            with self.dbo.get_connection() as conn:
                postgre.transaction(conn,
                                    {
                                        'type': 'delete',
                                        'table_name': 'attachments',
                                        'conditions': [
                                            {
                                                'var_name': 'sender_id',
                                                'operator': '='
                                            }
                                        ]
                                    },
                                    bind_vars={
                                        'sender_id': 'postgre_test_sender'
                                    })
                conn.commit()
            with self.dbo.get_connection() as conn:
                t = postgre.transaction(conn,
                                        {
                                            'type': 'select',
                                            'table_name': 'attachments',
                                            'columns': ['file_name']
                                        }, return_dicts=True)
                assert len(t) == 0
            # Test insert and rollback
            with self.dbo.get_connection() as conn:
                t = postgre.transaction(conn,
                                        {
                                            'type': 'insert',
                                            'table_name': 'attachments',
                                            'columns': ['file_name', 'folder', 'sender_id', 'receiver_id', 'bytes'],
                                            'return_values': ['bytes']
                                        },
                                        bind_vars={
                                            'file_name': 'a',
                                            'folder': 'b',
                                            'sender_id': 'postgre_test_sender',
                                            'receiver_id': 'postgre_test_receiver',
                                            'bytes': 12345
                                        }, return_dicts=True)
                postgre.rollback(conn)
                postgre.commit(conn)
            with self.dbo.get_connection() as conn:
                t = postgre.transaction(conn,
                                        {
                                            'type': 'select',
                                            'table_name': 'attachments',
                                            'columns': ['file_name']
                                        }, return_dicts=True)
                assert len(t) == 0
        except AssertionError:
            raise
        except Exception:
            assert False
        finally:
            with self.dbo.get_connection() as conn:
                postgre.transaction(conn,
                                    {
                                        'type': 'delete',
                                        'table_name': 'attachments',
                                        'conditions': [
                                            {
                                                'var_name': 'sender_id',
                                                'operator': '='
                                            }
                                        ]
                                    },
                                    bind_vars={
                                        'sender_id': 'postgre_test_sender'
                                    })
                postgre.commit(conn)


def test_delete_string():
    expected = 'DELETE FROM name'
    assert postgre.delete_string({
        'table_name': 'name'
    }) == expected


def test_select_string():
    expected = 'SELECT a, b, c, d FROM name'
    assert postgre.select_string({
        'table_name': 'name',
        'columns': ['a', 'b', 'c', 'd']
    }) == expected


def test_update_string():
    expected = 'UPDATE name SET a = %(a)s, b = %(b)s'
    assert postgre.update_string({
        'table_name': 'name',
        'columns': ['a', 'b']
    }) == expected


def test_insert_string():
    expected = 'INSERT INTO name (a, b) VALUES (%(a)s, %(b)s)'
    assert postgre.insert_string({
        'table_name': 'name',
        'columns': ['a', 'b']
    }) == expected


def test_add_conditions():
    expected = ' WHERE a = %(a)s and (b >= %(b)s or c = %(c)s) and d = %(d)s'
    assert postgre.add_conditions({
        'conditions': [
            {
                'var_name': 'a',
                'operator': '=',
                'next': 'and'
            },
            {
                'group_open': True,
                'var_name': 'b',
                'operator': '>=',
                'next': 'or'
            },
            {
                'group_close': True,
                'var_name': 'c',
                'operator': '=',
                'next': 'and'
            },
            {
                'var_name': 'd',
                'operator': '='
            }
        ]
    }) == expected


def test_add_sorting():
    expected = ' ORDER BY a, b ASC, c DESC, d'
    assert postgre.add_sorting({
        'sort': [
            {
                'var_name': 'a'
            },
            {
                'var_name': 'b',
                'order': 'asc'
            },
            {
                'var_name': 'c',
                'order': 'desc'
            },
            {
                'var_name': 'd'
            }
        ]
    }) == expected


def test_add_returning():
    expected = ' RETURNING a, b, c, d'
    assert postgre.add_returning({
        'return_values': ['a', 'b', 'c', 'd']
    }) == expected


def test_sql_config_to_string():
    expected = 'SELECT a, b, c, d FROM name' \
               ' WHERE a = %(a)s and (b >= %(b)s or c = %(c)s) and d = %(d)s' \
               ' ORDER BY a, b ASC, c DESC, d' \
               ' RETURNING a, b, c, d;'
    assert postgre.sql_config_to_string({
        'type': 'select',
        'table_name': 'name',
        'columns': ['a', 'b', 'c', 'd'],
        'return_values': ['a', 'b', 'c', 'd'],
        'sort': [
            {
                'var_name': 'a'
            },
            {
                'var_name': 'b',
                'order': 'asc'
            },
            {
                'var_name': 'c',
                'order': 'desc'
            },
            {
                'var_name': 'd'
            }
        ],
        'conditions': [
            {
                'var_name': 'a',
                'operator': '=',
                'next': 'and'
            },
            {
                'group_open': True,
                'var_name': 'b',
                'operator': '>=',
                'next': 'or'
            },
            {
                'group_close': True,
                'var_name': 'c',
                'operator': '=',
                'next': 'and'
            },
            {
                'var_name': 'd',
                'operator': '='
            }
        ]
    }) == expected


def test_list_to_list_of_dicts():
    expected = [
        {
            'a': 0,
            'b': 1
        },
        {
            'a': 2,
            'b': 3
        }
    ]
    assert postgre.list_to_list_of_dicts(['a', 'b'], [(0, 1), (2, 3)]) == expected


if __name__ == '__main__':
    tester_vars = {
        'creds': postgresql_creds,
        'wrong_creds': {
            'database': 'mail',
            'user': 'mail_admin',
            'password': 'wrong_password',
            'host': 'jokismodb.cibj12kfvi4a.us-east-1.rds.amazonaws.com',
            'port': 5432
        }
    }
    tester = PostgreTest(tester_vars)
    test_dbo = postgre.Postgre(tester_vars['creds'])
    try:
        tester.run_offline_tests()
        tester.run_networked_tests()
    finally:
        test_dbo.close()