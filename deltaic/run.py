#
# Deltaic - an efficient backup system supporting multiple data sources
#
# Copyright (c) 2014 Carnegie Mellon University
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of version 2 of the GNU General Public License as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#

import errno
import fcntl
import os
import sys

from .command import subparsers
from .sources import Source
from .storage import Snapshot
from . import sources as _  # Load all sources

def run_tasks(config):
    sources = []
    for source_type in Source.get_sources().values():
        source = source_type(config)
        source.start()
        sources.append(source)
    success = True
    for source in sources:
        if not source.wait():
            success = False
    return success


def cmd_run(config, args):
    settings = config['settings']
    root_dir = settings['root']
    root_parent = os.path.dirname(root_dir)
    lockfile = os.path.join(root_dir, '.lock')
    if os.stat(root_dir).st_dev == os.stat(root_parent).st_dev:
        raise IOError('Backup filesystem is not mounted')
    with open(lockfile, 'w') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError, e:
            if e.errno in (errno.EACCES, errno.EAGAIN):
                print >>sys.stderr, 'Another backup is already running.'
                return 1
            else:
                raise

        success = run_tasks(config)
        if args.snapshot:
            vg, lv = settings['backup-lv'].split('/')
            Snapshot.create(vg, lv, verbose=True)

    if not success:
        return 1


def _setup():
    parser = subparsers.add_parser('run',
            help='run a backup')
    parser.set_defaults(func=cmd_run)
    parser.add_argument('-S', '--no-snapshot', dest='snapshot',
            action='store_false', default=True,
            help='skip snapshot of backup volume')

_setup()
