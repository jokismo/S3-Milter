from utils import mime
from email import message_from_file

if __name__ == '__main__':
    with open('sample_message', 'r') as f:
        html_parts = []
        text_parts = []
        msg = message_from_file(f)
        generator = mime.get_attachment_parts(msg, text_parts, html_parts)
        for attachment_part, file_name, c_id in generator:
            pass
        print mime._get_decoded_payload(html_parts[0])