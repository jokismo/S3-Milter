from multiprocessing import Queue

from utils.log_config import init
from utils.log_config import set_level
from services.log_manager import LogManager
from services.postgre_manager import PostgreManager
from milter_config import postgresql_creds

from tests.services_t.mime_body_processor import MimeBodyProcessorTest


class Test(object):

    @classmethod
    def setUpClass(klass):
        init()
        set_level(is_dev=True)
        klass.log_queue = Queue(maxsize=0)
        klass.postgre_queue = Queue(maxsize=0)
        klass.log_manager = LogManager(klass.log_queue)
        klass.log_manager.start()
        klass.postgre_manager = PostgreManager(postgresql_creds, klass.postgre_queue, klass.log_queue)
        klass.postgre_manager.start()

    def test_a_resources(self):
        mime_body_processor_test = MimeBodyProcessorTest(self.log_queue, self.postgre_queue)
        mime_body_processor_test.run_offline_tests()
        mime_body_processor_test.run_networked_tests()

    @classmethod
    def tearDownClass(klass):
        klass.postgre_queue.put(None)
        klass.postgre_manager.join()
        klass.log_queue.put(None)
        klass.log_manager.join()