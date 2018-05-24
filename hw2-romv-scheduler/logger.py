
class _Singleton(type):
    """ A metaclass that creates a Singleton base class when called. """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(_Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Singleton(_Singleton('SingletonMeta', (object,), {})):
    pass


class Logger(Singleton):
    def __init__(self):
        self._turned_off_log_type_names = set()

    def turn_on(self, log_type_name):
        if log_type_name in self._turned_off_log_type_names:
            self._turned_off_log_type_names.remove(log_type_name)

    def turn_off(self, log_type_name):
        self._turned_off_log_type_names.add(log_type_name)

    def log(self, log_str, log_type_name=None):
        if log_type_name is None or log_type_name not in self._turned_off_log_type_names:
            print(log_str)
