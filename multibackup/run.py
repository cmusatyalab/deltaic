import errno
import fcntl
import os
import sys

from .command import subparsers
from .source import Source
from .storage import Snapshot
from . import sources as _  # Load all sources

def run_tasks(config):
    sources = []
    for source_type in Source.get_sources().values():
        source = source_type(config)
        source.start()
        sources.append(source)
    for source in sources:
        source.wait()


def cmd_run(config, args):
    settings = config['settings']
    lockfile = os.path.join(settings['root'], '.lock')
    with open(lockfile, 'w') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError, e:
            if e.errno in (errno.EACCES, errno.EAGAIN):
                print >>sys.stderr, 'Another backup is already running.'
                return 1
            else:
                raise

        run_tasks(config)
        if args.snapshot:
            vg, lv = settings['backup-lv'].split('/')
            Snapshot.create(vg, lv, verbose=True)


def _setup():
    parser = subparsers.add_parser('run',
            help='run a backup')
    parser.set_defaults(func=cmd_run)
    parser.add_argument('-S', '--no-snapshot', dest='snapshot',
            action='store_false', default=True,
            help='skip snapshot of backup volume')

_setup()
