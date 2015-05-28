from utils.log_config import log_error
from utils.log_config import log_database_entry
from utils.log_config import log_success
from utils.log_config import log_status
from utils.log_config import log_called


class LoggingSetupTest(object):

    def __init__(self):
        pass

    def run_offline_tests(self):
        test_log_strings()

    def run_networked_tests(self):
        pass


def test_log_strings():
    expected = '<ERROR><log_config.test_log_strings><an_error>'
    logs = log_error(module_name='log_config', function_name='test_log_strings',
                     error='an_error',
                     params={
                         'param_one': 'one',
                         'param_two': 'two'
                     })
    assert expected in logs
    assert 'param_one=one' in logs
    assert 'param_two=two' in logs
    expected = '<ERROR><log_config.test_log_strings><FIELD NOT SUPPLIED>'
    logs = log_error(module_name='log_config', function_name='test_log_strings',
                     params={
                         'param_one': 'one',
                         'param_two': 'two'
                     })
    assert expected in logs
    expected = '<ERROR><log_config.test_log_strings><an_error>'
    logs = log_error(module_name='log_config', function_name='test_log_strings',
                     error='an_error')
    assert expected in logs
    expected = '<DB Update><log_config.test_log_strings><Table:a_table>'
    logs = log_database_entry(module_name='log_config', function_name='test_log_strings',
                              table_name='a_table')
    assert expected in logs
    expected = '<SUCCESS><log_config.test_log_strings><a_msg>'
    logs = log_success(module_name='log_config', function_name='test_log_strings',
                       msg='a_msg')
    assert expected in logs
    expected = '<STATUS><log_config.test_log_strings><a_msg>'
    logs = log_status(module_name='log_config', function_name='test_log_strings',
                      msg='a_msg')
    assert expected in logs
    expected = '<CALLED><log_config.test_log_strings>'
    logs = log_called(module_name='log_config', function_name='test_log_strings',
                      msg='a_msg')
    assert expected in logs

if __name__ == "__main__":
    tester = LoggingSetupTest()
    tester.run_offline_tests()