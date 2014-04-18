from getpass import getuser
import os
import sys

from .command import subparsers

TEMPLATES = {
    'crontab': '''
MAILTO = %(email)s

0 21 * * * %(prog)s prune
55 21 * * * %(prog)s df -c
0 22 * * * %(prog)s run >/dev/null && echo "OK"
''',

    'sudoers': '''
# Allow backup script to query and create snapshot volumes
%(user)s ALL=NOPASSWD: /sbin/lvs, /sbin/lvcreate, /sbin/lvremove, /sbin/lvchange, /bin/mount, /bin/umount
''',
}


def cmd_mkconf(config, args):
    print TEMPLATES[args.file].strip() % {
        'user': getuser(),
        'prog': os.path.abspath(sys.argv[0]),
        'email': args.email,
    }


def _setup():
    parser = subparsers.add_parser('mkconf',
            help='generate a config file template')
    parser.set_defaults(func=cmd_mkconf)
    parser.add_argument('file', choices=TEMPLATES,
            help='config file to generate')
    parser.add_argument('--email', default='root', metavar='ADDRESS',
            help='email address to send status reports')

_setup()
