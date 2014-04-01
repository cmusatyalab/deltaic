from getpass import getuser
import os
import sys

from .command import subparsers

TEMPLATES = {
    'crontab': '''
30 21 * * * %(prog)s prune
0 22 * * * %(prog)s run
''',

    'sudoers': '''
# Allow backup script to query and create snapshot volumes
%(user)s ALL=NOPASSWD: /sbin/lvs, /sbin/lvcreate, /sbin/lvremove
''',
}


def cmd_mkconf(config, args):
    print TEMPLATES[args.file].strip() % {
        'user': getuser(),
        'prog': os.path.abspath(sys.argv[0]),
    }


def _setup():
    parser = subparsers.add_parser('mkconf',
            help='generate a config file template')
    parser.set_defaults(func=cmd_mkconf)
    parser.add_argument('file', choices=TEMPLATES,
            help='config file to generate')

_setup()
