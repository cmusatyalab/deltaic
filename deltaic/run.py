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

from .command import subparsers
from .sources import Source
from .storage import PhysicalSnapshot
from .util import lockfile
from . import sources as _  # Load all sources

def back_up_units(config):
    tasks = []
    for source_type in Source.get_sources().values():
        source = source_type(config)
        task = source.get_backup_task()
        task.start()
        tasks.append(task)
    success = True
    for task in tasks:
        if not task.wait():
            success = False
    return success


def cmd_run(config, args):
    settings = config['settings']
    with lockfile(settings, 'backup'):
        success = back_up_units(config)
        if args.snapshot:
            vg, lv = settings['backup-lv'].split('/')
            PhysicalSnapshot.create(vg, lv, verbose=True)
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
