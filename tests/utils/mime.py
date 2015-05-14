from email import message_from_file
from email.generator import Generator
import datetime

from utils import mime


class MimeTest(object):

    def __init__(self):
        pass

    def run_offline_tests(self):
        test_get_attachment_parts()
        test_clear_attachment()
        test_add_plain_text_urls()
        test__get_html_insert_position()
        test_add_html_urls()

    def run_networked_tests(self):
        pass


def test_add_html_urls():
    html_string = '<asd>text_string_asdf</asd>'
    with open('test_messages/text_html.txt', 'r') as f:
        html_parts = []
        text_parts = []
        msg = message_from_file(f)
        generator = mime.get_attachment_parts(msg, text_parts, html_parts)
        for attachment_part, file_name, c_id in generator:
            pass
        mime.add_html_urls(html_parts, html_string)
    with open('test_messages/message_write.txt', 'w+') as f:
        g = Generator(f)
        g.flatten(msg)
    with open('test_messages/message_write.txt', 'r') as f:
        html_parts = []
        text_parts = []
        msg = message_from_file(f)
        generator = mime.get_attachment_parts(msg, text_parts, html_parts)
        for attachment_part, file_name, c_id in generator:
            pass
        content = mime._get_decoded_payload(html_parts[0])
        assert html_string in content


def test__get_html_insert_position():
    insert = '<p>'
    html = '<!DOCTYPE><HTML>{}htmlhere'
    expected = html.format(insert)
    html = html.format('')
    position = mime._get_html_insert_position(html)
    html = html[:position] + insert + html[position:]
    assert html == expected
    html = '<!DOCTYPE><asdhTmLasd**>{}htmlhere'
    expected = html.format(insert)
    html = html.format('')
    position = mime._get_html_insert_position(html)
    html = html[:position] + insert + html[position:]
    assert html == expected
    html = '<!DOCTYPE asdasdsadas>{}htmlhere'
    expected = html.format(insert)
    html = html.format('')
    position = mime._get_html_insert_position(html)
    html = html[:position] + insert + html[position:]
    assert html == expected
    html = '<!DOCTYPE><HTML><body>{}htmlhere'
    expected = html.format(insert)
    html = html.format('')
    position = mime._get_html_insert_position(html)
    html = html[:position] + insert + html[position:]
    assert html == expected
    html = '<!DOCTYPE><HTML></head>{}htmlhere'
    expected = html.format(insert)
    html = html.format('')
    position = mime._get_html_insert_position(html)
    html = html[:position] + insert + html[position:]
    assert html == expected
    html = '{}htmlhere'
    expected = html.format(insert)
    html = html.format('')
    position = mime._get_html_insert_position(html)
    html = html[:position] + insert + html[position:]
    assert html == expected


def test_add_plain_text_urls():
    text_string = 'text_string_asdf'
    with open('test_messages/text_only.txt', 'r') as f:
        html_parts = []
        text_parts = []
        msg = message_from_file(f)
        generator = mime.get_attachment_parts(msg, text_parts, html_parts)
        for attachment_part, file_name, c_id in generator:
            pass
        mime.add_plain_text_urls(text_parts, text_string)
    with open('test_messages/message_write.txt', 'w+') as f:
        g = Generator(f)
        g.flatten(msg)
    with open('test_messages/message_write.txt', 'r') as f:
        html_parts = []
        text_parts = []
        msg = message_from_file(f)
        generator = mime.get_attachment_parts(msg, text_parts, html_parts)
        for attachment_part, file_name, c_id in generator:
            pass
        content = mime._get_decoded_payload(text_parts[0])
        assert text_string + '\n' in content


def test_clear_attachment():
    with open('test_messages/text_three_attachments.txt', 'r') as f:
        html_parts = []
        text_parts = []
        msg = message_from_file(f)
        generator = mime.get_attachment_parts(msg, text_parts, html_parts)
        truncated_filename = '1' * 124 + '.png'
        for attachment_part, file_name, c_id in generator:
            assert file_name in [truncated_filename, 'green-_ball.png', 'redball.png']
            assert c_id is None
            mime.clear_attachment(attachment_part)
        assert len(html_parts) == 0
        assert len(text_parts) == 1
    with open('test_messages/message_write.txt', 'w+') as f:
        g = Generator(f)
        g.flatten(msg)
    with open('test_messages/message_write.txt', 'r') as f:
        html_parts = []
        text_parts = []
        msg = message_from_file(f)
        generator = mime.get_attachment_parts(msg, text_parts, html_parts)
        for attachment_part, file_name, c_id in generator:
            assert False
        assert len(html_parts) == 3
        assert len(text_parts) == 1


def test_get_attachment_parts():
    with open('test_messages/text_only.txt', 'r') as f:
        html_parts = []
        text_parts = []
        msg = message_from_file(f)
        generator = mime.get_attachment_parts(msg, text_parts, html_parts)
        for attachment_part, file_name, c_id in generator:
            assert False
        assert len(html_parts) == 0
        assert len(text_parts) == 1
    with open('test_messages/text_three_attachments.txt', 'r') as f:
        html_parts = []
        text_parts = []
        msg = message_from_file(f)
        generator = mime.get_attachment_parts(msg, text_parts, html_parts)
        truncated_filename = '1' * 124 + '.png'
        for attachment_part, file_name, c_id in generator:
            assert file_name in [truncated_filename, 'green-_ball.png', 'redball.png']
            assert c_id is None
        assert len(html_parts) == 0
        assert len(text_parts) == 1
    with open('test_messages/text_three_attachments_no_filename.txt', 'r') as f:
        html_parts = []
        text_parts = []
        attachments = []
        msg = message_from_file(f)
        generator = mime.get_attachment_parts(msg, text_parts, html_parts)
        for attachment_part, file_name, c_id in generator:
            attachments.append(file_name)
            assert datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') in file_name
            assert c_id is None
        assert len(attachments) == 3
        assert len(html_parts) == 0
        assert len(text_parts) == 1
    with open('test_messages/text_html.txt', 'r') as f:
        html_parts = []
        text_parts = []
        msg = message_from_file(f)
        generator = mime.get_attachment_parts(msg, text_parts, html_parts)
        for attachment_part, file_name, c_id in generator:
            assert False
        assert len(html_parts) == 1
        assert len(text_parts) == 1
    with open('test_messages/text_html_two_attachments.txt', 'r') as f:
        html_parts = []
        text_parts = []
        msg = message_from_file(f)
        generator = mime.get_attachment_parts(msg, text_parts, html_parts)
        for attachment_part, file_name, c_id in generator:
            assert file_name in ['greenball.png', 'redball.png']
            assert c_id is None
        assert len(html_parts) == 1
        assert len(text_parts) == 1
    with open('test_messages/text_html_three_attachments_one_cid.txt', 'r') as f:
        html_parts = []
        text_parts = []
        cids = []
        msg = message_from_file(f)
        generator = mime.get_attachment_parts(msg, text_parts, html_parts)
        for attachment_part, file_name, c_id in generator:
            assert file_name in ['blueball.png', 'greenball.png', 'redball.png']
            if c_id is not None:
                cids.append(c_id)
        assert '938014623@17052000-0f9b' in cids
        assert len(html_parts) == 1
        assert len(text_parts) == 1

if __name__ == '__main__':
    mime_test = MimeTest()
    mime_test.run_offline_tests()