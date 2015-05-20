from services import s3
from milter_config import s3_config
from utils.exceptions import MilterException
from requests import get
import os


class S3Test(object):

    def __init__(self):
        self.test_folder = 'test'

    def run_offline_tests(self):
        test__path_array_to_string()

    def run_networked_tests(self):
        self.test_connect()
        self.test_transactions()

    def test_connect(self):
        self.s3 = s3.S3(s3_config)
        assert len(self.s3.conn.get_all_buckets()) > 0
        if s3_config.get('default_bucket') is not None:
            assert self.s3.bucket is not None

    def test_transactions(self):
        test_data = 'aaa'
        self.s3.store(['test'], 'test.t', test_data)
        a = self.s3.get(['test'], 'test.t')
        assert a == test_data
        self.s3.delete(['test'], 'test.t')
        try:
            self.s3.get(['test'], 'test.t')
        except MilterException:
            assert True
        else:
            assert False
        url = self.s3.store(['test'], 'test.t', test_data, config={
            'is_public': True,
            'return_url': True
        })
        r = get(url)
        assert r.text == test_data
        self.s3.delete(['test'], 'test.t')


def test__path_array_to_string():
    expected = 'asdf/0/'
    path_array = ['asdf', 0]
    assert s3._path_array_to_string(path_array) == expected


if __name__ == '__main__':
    s3_test = S3Test()
    s3_test.run_offline_tests()
    s3_test.run_networked_tests()
    # s3_tester = s3.S3(s3_config)
    # with open('asdf.png', 'rb') as svg:
    #     svg_string = base64.b64encode(svg.read())
    #     print svg_string
    # path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'title.svg')
    # url = s3_tester.store(['static'], 'title.svg', path, config={
    #     'is_public': True,
    #     'return_url': True,
    #     'upload_method': 'file'
    # })
    # print url