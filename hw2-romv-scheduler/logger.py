from utils import Singleton


class Logger(Singleton):
    def __init__(self):
        self._turned_off_log_type_names = set()
        self._prefix = ''

    def turn_on(self, log_type_name):
        if log_type_name in self._turned_off_log_type_names:
            self._turned_off_log_type_names.remove(log_type_name)

    def turn_off(self, log_type_name):
        self._turned_off_log_type_names.add(log_type_name)

    @property
    def prefix(self):
        return self._prefix

    @prefix.setter
    def prefix(self, new_prefix):
        self._prefix = new_prefix

    def log(self, log_str='', log_type_name=None):
        if log_type_name is None or log_type_name not in self._turned_off_log_type_names:
            print(self._prefix + log_str)
