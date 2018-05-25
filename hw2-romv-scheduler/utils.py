import argparse


class _Singleton(type):
    """ A metaclass that creates a Singleton base class when called. """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(_Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Singleton(_Singleton('SingletonMeta', (object,), {})):
    pass


def add_feature_to_parser(parser: argparse.ArgumentParser, feature_names: list, help=None,
                          feature_prefix=None, on_text='ON', off_text='OFF', default=False, dest=None):
    if feature_prefix is None:
        feature_prefix = 'Turn {on_or_off} feature'
    assert '{on_or_off}' in feature_prefix

    if not isinstance(feature_names, list):
        assert isinstance(feature_names, str)
        feature_names = [feature_names]
    assert isinstance(feature_names, list)

    help_desc = help
    if help_desc is None:
        help_desc = ''

    dest_not_none = dest
    if dest_not_none is None:
        dest_not_none = feature_names[0]
        assert dest_not_none[0] == '-'
        if dest_not_none[1] == '-':  # long string name
            dest_not_none = dest_not_none[2:]
        else:  # short string name
            dest_not_none = dest_not_none[1:]
        dest_not_none = dest_not_none.replace('-', '_')

    default_text = on_text if default else off_text

    def add_no_to_feature_name(feature_name):
        assert len(feature_name) > 1
        assert feature_name[0] == '-'
        if feature_name[1] == '-':  # long string name
            return '--no-' + feature_name[2:]
        return '-n' + feature_name

    on_feature_names = feature_names
    off_feature_names = [add_no_to_feature_name(feature_name) for feature_name in feature_names]

    feature_parser = parser.add_mutually_exclusive_group(required=False)
    feature_parser.add_argument(*on_feature_names,
                                action='store_true', dest=dest_not_none,
                                help=feature_prefix.format(on_or_off=on_text) + ': ' + help_desc +
                                     ' By default turned {}.'.format(default_text) +
                                     ' To{} turn feature off use{}: '.format(
                                         ' explicitly' if not default else '',
                                         ' one of' if len(off_feature_names) > 1 else '') +
                                     (', '.join(off_feature_names)))
    feature_parser.add_argument(*off_feature_names,
                                action='store_false', dest=dest_not_none,
                                help=argparse.SUPPRESS)
    parser.set_defaults(**{dest_not_none: default})
    return feature_parser
