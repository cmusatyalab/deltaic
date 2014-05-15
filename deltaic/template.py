from getpass import getuser
import os
import sys

from .command import subparsers

TEMPLATES = {
    'crontab': '''
MAILTO = %(email)s

0 23 * * * %(prog)s prune
55 23 * * * %(prog)s df -c
0 0 * * * %(prog)s run >/dev/null && echo "OK"
''',

    'sudoers': '''
# Allow Deltaic to query, create, delete, mount, and unmount snapshot volumes
%(user)s ALL=NOPASSWD: /sbin/lvs, /sbin/lvcreate, /sbin/lvremove, /sbin/lvchange, /bin/mount, /bin/umount
# Allow running sudo from cron
Defaults:%(user)s !requiretty
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
