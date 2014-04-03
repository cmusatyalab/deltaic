import argparse
import os
import sys

def _get_default_config_path():
    config_dir = os.environ.get('XDG_CONFIG_HOME')
    if config_dir is None:
        config_dir = os.path.join(os.environ['HOME'], '.config')
    return os.path.join(config_dir, 'multibackup.conf')
_default_config_path = _get_default_config_path()


def make_subcommand_group(name, help):
    subparser = subparsers.add_parser(name, help=help)
    return subparser.add_subparsers(metavar='COMMAND')


def get_cmdline_for_subcommand(subcommand):
    cmd = [sys.executable, sys.argv[0]]
    # Append global command-line arguments that must be passed to all
    # sub-invocations.  Reparse the command line so the entire call chain
    # doesn't have to pass the Namespace around.
    args = parser.parse_args()
    if args.config_file != parser.get_default('config_file'):
        cmd.extend(['-c', args.config_file])
    cmd.extend(subcommand)
    return cmd


parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config-file', metavar='PATH',
        default=_default_config_path,
        help='path to config file (default: %s)' % _default_config_path)

subparsers = parser.add_subparsers(metavar='COMMAND')
