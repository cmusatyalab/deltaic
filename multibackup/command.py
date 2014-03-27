import argparse
import os

def _get_default_config_path():
    config_dir = os.environ.get('XDG_CONFIG_HOME')
    if config_dir is None:
        config_dir = os.path.join(os.environ['HOME'], '.config')
    return os.path.join(config_dir, 'multibackup.conf')
_default_config_path = _get_default_config_path()


def make_subcommand_group(name, help):
    subparser = subparsers.add_parser(name, help=help)
    return subparser.add_subparsers(metavar='COMMAND')


parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config-file', metavar='PATH',
        default=_default_config_path,
        help='path to config file (default: %s)' % _default_config_path)

subparsers = parser.add_subparsers(metavar='COMMAND')
