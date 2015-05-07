from boto.s3.connection import S3Connection
from boto.s3.key import Key
import logging

from utils.exceptions import MilterException
from milter_config import s3_creds
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


class S3(object):

    def __init__(self):
        self.connect()

    def connect(self):
        try:
            self.conn = S3Connection(s3_creds['id'], s3_creds['key'])
        except Exception as e:
            handle_error('connect', e)

    def set_bucket(self, bucket_name):
        try:
            self.bucket = self.conn.get_bucket(bucket_name)
        except Exception as e:
            handle_error('set_bucket', e)

    def store(self, path_array, key, data):
        try:
            k = Key(self.bucket)
            k.key = path_array_to_string(path_array) + key
            k.set_contents_from_string(data)
            k.make_public()
            url = k.generate_url(expires_in=0).split('?')[0]
            k.close()
            return url
        except Exception as e:
            handle_error('store', e)


if __name__ == '__main__':
    s3 = S3()
    s3.set_bucket('persafd')
    s3.store(path_array=['test'], key='test2.txt', data='')