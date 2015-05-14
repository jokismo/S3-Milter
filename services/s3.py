from boto.s3.connection import S3Connection
from boto.s3.key import Key
import logging

from utils.exceptions import MilterException
from utils.log_config import log_error
from utils.log_config import log_success
from utils.log_config import log_called


logger = logging.getLogger(__name__)


def path_array_to_string(path_array):
    path_string = ''
    for path in path_array:
        path_string += path + '/'
    return path_string


class S3(object):

    def __init__(self, config, log_queue=None):
        self.bucket = None
        self.config = config
        self.log_queue = log_queue
        self.connect()
        self.log('debug', log_success(module_name=__name__, function_name='__init__', msg='Connected to S3'))

    def connect(self):
        try:
            self.conn = S3Connection(self.config['id'], self.config['key'])
            if self.config.get('default_bucket') is not None:
                self.set_bucket(self.config['default_bucket'])
        except MilterException as e:
            raise e
        except Exception as e:
            self.handle_error('connect', str(e))

    def set_bucket(self, bucket_name):
        try:
            self.bucket = self.conn.get_bucket(bucket_name)
        except Exception as e:
            self.handle_error('set_bucket', str(e))

    def store(self, path_array, key, data):
        self.log('debug', log_called(module_name=__name__, function_name='upload_file', params={
            'key': key
        }))
        try:
            k = Key(self.bucket)
            k.key = path_array_to_string(path_array) + key
            k.set_contents_from_string(data)
            k.make_public()
            url = k.generate_url(expires_in=0).split('?')[0]
            k.close()
            self.log('info', log_success(module_name=__name__, function_name='upload_file',
                                         msg='Upload Complete', params={'url': url}))
            return url
        except Exception as e:
            self.handle_error('store', str(e))

    def log(self, log_type, message):
        if self.log_queue is not None:
            self.log_queue.put({
                'type': log_type,
                'message': message
            })

    def handle_error(self, function_name, error, params=None):
        error = log_error(module_name=__name__, function_name=function_name,
                          error=error, params=params)
        self.log('error', error)
        raise MilterException(500, 'S3 Service Error.', error)