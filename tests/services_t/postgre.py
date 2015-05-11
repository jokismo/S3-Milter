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
        test_select_string()
        test_update_string()
        test_insert_string()

    def run_networked_tests(self, dbo):
        self.dbo = dbo
        self.test_connect()
        self.test_pool()

    def test_connect(self):
        assert self.dbo.conn.closed == 0

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
    pass


if __name__ == '__main__':
    tester_vars = {
        'creds': postgresql_creds
    }
    tester = PostgreTest(tester_vars)
    test_dbo = postgre.Postgre(tester_vars['creds'])
    try:
        tester.run_offline_tests()
        #tester.run_networked_tests(test_dbo)
    finally:
        test_dbo.close()