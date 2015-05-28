from services import mime_body_processor
from milter_config import body_processor_params


class MimeBodyProcessorTest(object):

    def __init__(self, log_queue=None, postgre_queue=None):
        self.log_queue = log_queue
        self.postgre_queue = postgre_queue

    def run_offline_tests(self):
        test_update_text_url_string()
        test_shrink_to_max_length()
        test_get_url_string_html()
        test_compose_attachments_html()

    def run_networked_tests(self):
        self.test_mime_body_processor()

    def test_mime_body_processor(self):
        pass


def test_update_text_url_string():
    file_name = 'p.pdf'
    file_url = 'http://p.pdf'
    expected = body_processor_params['plain_text_replace_strings']['initial_line'] \
               + body_processor_params['plain_text_replace_strings']['url_line'].format(file_name, file_url)
    text_url_string = mime_body_processor.update_text_url_string('', file_url, file_name)
    assert text_url_string == expected
    expected = text_url_string \
               + body_processor_params['plain_text_replace_strings']['url_line'].format(file_name, file_url)
    text_url_string = mime_body_processor.update_text_url_string(text_url_string, file_url, file_name)
    assert text_url_string == expected


def test_shrink_to_max_length():
    long_string = 'a' * 20
    max_length = 10
    num_dots = body_processor_params['html']['long_string_num_dots']
    expected = 'a' * (max_length / 2 - num_dots) + '.' * num_dots + 'a' * (max_length / 2)
    assert mime_body_processor.shrink_to_max_length(long_string, max_length) == expected
    short_string = 'a' * 10
    assert mime_body_processor.shrink_to_max_length(short_string, max_length) == short_string


def test_get_url_string_html():
    file_name = ('a' * 20) + '.pdf'
    file_url = 'http://' + file_name
    html = mime_body_processor.get_url_string_html(file_url, file_name)
    file_name = ('a' * 20) + '.png'
    file_url = 'http://' + file_name
    html = mime_body_processor.get_url_string_html(file_url, file_name)
    # print html


def test_compose_attachments_html():
    file_name = ('Q' * 20) + '.pyv'
    file_url = 'http://www.test.com/' + file_name
    num_attachments = 5
    url_strings_html = []
    for x in xrange(0, num_attachments):
        url_strings_html.append(mime_body_processor.get_url_string_html(file_url, file_name))
    html = mime_body_processor.compose_attachments_html(url_strings_html)
    with open('test_html.html', 'w+') as f:
        f.write(html)
    # print html


if __name__ == '__main__':
    tester_vars = {}
    tester = MimeBodyProcessorTest(tester_vars)
    tester.run_offline_tests()