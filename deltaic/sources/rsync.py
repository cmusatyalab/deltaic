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

import os
import re
import subprocess
import sys

from ..command import make_subcommand_group
from ..source import Task, Source
from ..util import random_do_work

def remote_command(host, command):
    args = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=no',
            'root@%s' % host, command]
    print ' '.join(args)
    subprocess.check_call(args)


def run_rsync(cmd):
    print ' '.join(cmd)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    # Filter out spurious log output from
    # https://bugzilla.samba.org/show_bug.cgi?id=10496
    spurious = re.compile(r'[.h][dfL]\.{8}x ')
    for line in proc.stdout:
        if not spurious.match(line):
            print line.strip()

    return proc.wait()


def sync_host(host, root_dir, mounts, exclude=(), scrub=False, rsync=None):
    if rsync is None:
        rsync = 'rsync'
    args = [rsync, '-aHRxi', '--acls', '--xattrs', '--fake-super', '--delete',
            '--delete-excluded', '--numeric-ids', '--stats', '--partial',
            '--rsh=ssh -o BatchMode=yes -o StrictHostKeyChecking=no']
    args.extend(['--exclude=' + r for r in exclude])
    args.extend(['root@%s:%s' % (host, mount.rstrip('/') or '/')
            for mount in mounts])
    args.append(root_dir.rstrip('/'))
    if scrub:
        args.append('--checksum')

    ret = run_rsync(args)
    if ret in (2, 10):
        # Try falling back to protocol 30 to work around failures when one
        # rsync is 3.1.0 and the other is >= 3.1.1
        ret = run_rsync(args + ['--protocol=30'])
    if ret == 2:
        # Drop features that require protocol >= 30
        args.remove('--acls')
        args.remove('--xattrs')
        ret = run_rsync(args)

    if ret not in (0, 24):
        raise IOError('rsync failed with code %d' % ret)


def get_relroot(hostname, info):
    alias = info.get('alias', hostname.split('.')[0])
    return os.path.join('rsync', alias)


def cmd_rsync_backup(config, args):
    settings = config['settings']
    info = config['rsync'][args.host]
    root_dir = os.path.join(settings['root'], get_relroot(args.host, info))
    exclude = settings.get('rsync-exclude', [])
    exclude.extend(info.get('exclude', []))
    rsync = settings.get('rsync-local-binary')

    if 'pre' in info:
        remote_command(args.host, info['pre'])
    sync_host(args.host, root_dir, info['mounts'], exclude, scrub=args.scrub,
            rsync=rsync)
    if 'post' in info:
        remote_command(args.host, info['post'])


def _setup():
    group = make_subcommand_group('rsync',
            help='host filesystem support via rsync')

    parser = group.add_parser('backup',
            help='back up host filesystem')
    parser.set_defaults(func=cmd_rsync_backup)
    parser.add_argument('host',
            help='host to back up')
    parser.add_argument('-c', '--scrub', action='store_true',
            help='check backup data against original')

_setup()


class RsyncTask(Task):
    def __init__(self, settings, hostname, info):
        Task.__init__(self, settings)
        self.root = get_relroot(hostname, info)
        self.args = ['rsync', 'backup', hostname]
        if random_do_work(settings, 'rsync-scrub-probability', 0.0166):
            self.args.append('-c')


class RsyncSource(Source):
    LABEL = 'rsync'

    def __init__(self, config):
        Source.__init__(self, config)
        for hostname, info in sorted(self._manifest.items(),
                key=lambda e: e[1].get('alias', e[0])):
            self._queue.put(RsyncTask(self._settings, hostname, info))
