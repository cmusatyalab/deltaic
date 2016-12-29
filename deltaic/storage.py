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

from __future__ import division
from datetime import date, datetime
import os
import subprocess

from .command import subparsers
from .util import make_dir_path, humanize_size

class Snapshot(object):
    DATE_FMT = '%Y%m%d'

    def __init__(self, name):
        self.name = name
        datecode, revision = name.split('-')
        self.date = datetime.strptime(datecode, self.DATE_FMT).date()
        self.revision = int(revision)
        self.year, self.week, self.day = self.date.isocalendar()
        # 4, 7-day weeks/month => 13 months/year, 14 on long years
        self.month = ((self.week - 1) // 4) + 1

    def __str__(self):
        return str(self.name)

    def __repr__(self):
        return 'Snapshot(%s)' % repr(self.name)

    def __cmp__(self, other):
        if not isinstance(other, self.__class__):
            return hash(other) - hash(self)
        if self.date < other.date:
            return -1
        elif self.date > other.date:
            return 1
        else:
            return self.revision - other.revision

    def get_physical(self, settings):
        vg, _ = self._get_backup_vg_lv(settings)
        return PhysicalSnapshot(vg, self.name)

    @classmethod
    def _get_backup_vg_lv(cls, settings):
        return settings['backup-lv'].split('/')


class PhysicalSnapshot(Snapshot):
    TAG = 'backup-snapshot'

    def __init__(self, vg, name):
        if '/' in name:
            raise ValueError('Invalid snapshot name: %s' % name)
        Snapshot.__init__(self, name)
        self.vg = vg

    def __repr__(self):
        return 'PhysicalSnapshot(%s, %s)' % (repr(self.vg), repr(self.name))

    def get_physical(self, settings):
        return self

    @classmethod
    def list(cls):
        proc = subprocess.Popen(['sudo', 'lvs', '--noheadings',
                '-o', 'vg_name,lv_name', '@' + cls.TAG],
                stdout=subprocess.PIPE)
        out, _ = proc.communicate()
        if proc.returncode:
            raise IOError("Couldn't list snapshot LVs")
        ret = []
        for line in out.split('\n'):
            if not line:
                continue
            vg, lv = line.split()
            ret.append(cls(vg, lv))
        return sorted(ret)

    @classmethod
    def create(cls, settings, verbose=False):
        vg, lv = cls._get_backup_vg_lv(settings)
        today = date.today().strftime(cls.DATE_FMT)
        with open('/dev/null', 'r+') as null:
            # Give up eventually in case test keeps failing
            for n in range(1, 100):
                snapshot_lv = '%s-%d' % (today, n)
                ret = subprocess.call(['sudo', 'lvs',
                        '%s/%s' % (vg, snapshot_lv)], stdout=null, stderr=null)
                if ret:
                    break
            else:
                raise IOError("Couldn't locate unused snapshot LV")
            subprocess.check_call(['sudo', 'lvcreate',
                    '-s', '%s/%s' % (vg, lv), '-p', 'r', '-n', snapshot_lv,
                    '--addtag', cls.TAG], stdin=null,
                    stdout=None if verbose else null)
        return cls(vg, snapshot_lv)

    def remove(self, verbose=False):
        with open('/dev/null', 'r+') as null:
            subprocess.check_call(['sudo', 'lvremove', '--force',
                    '%s/%s' % (self.vg, self.name)], stdin=null,
                    stdout=None if verbose else null)

    def mount(self, mountpoint):
        subprocess.check_call(['sudo', 'lvchange', '-a', 'y', '-K',
                '%s/%s' % (self.vg, self.name)])
        subprocess.check_call(['sudo', 'mount', '-o', 'ro',
                '/dev/%s/%s' % (self.vg, self.name), mountpoint])

    def umount(self, mountpoint):
        subprocess.check_call(['sudo', 'umount', mountpoint])
        # May fail if double-mounted
        subprocess.call(['sudo', 'lvchange', '-a', 'n',
                '%s/%s' % (self.vg, self.name)])


class StorageStatus(object):
    def __init__(self, vg, lv, mountpoint):
        # Get filesystem stats
        st = os.statvfs(mountpoint)
        self.fs_free = st.f_frsize * st.f_bavail
        self.fs_free_pct = 100 * st.f_bavail / st.f_blocks
        self.ino_free = st.f_favail
        self.ino_free_pct = 100 * st.f_favail / st.f_files

        # Find pool LV
        proc = subprocess.Popen(['sudo', 'lvs', '--noheadings',
                '-o', 'pool_lv', '%s/%s' % (vg, lv)], stdout=subprocess.PIPE)
        out, _ = proc.communicate()
        if proc.returncode:
            raise IOError("Couldn't retrieve pool LV: lvs returned %d" %
                    proc.returncode)
        pool_lv = out.strip()
        if not pool_lv:
            raise IOError("Couldn't retrieve pool LV")

        # Get pool LV stats
        proc = subprocess.Popen(['sudo', 'lvs', '--noheadings', '--nosuffix',
                '--units', 'b',
                '-o', 'lv_size,data_percent,lv_metadata_size,metadata_percent',
                '%s/%s' % (vg, pool_lv)], stdout=subprocess.PIPE)
        out, _ = proc.communicate()
        if proc.returncode:
            raise IOError("Couldn't examine pool LV: lvs returned %d" %
                    proc.returncode)
        vals = [float(v) for v in out.split()]
        data_size, data_pct, meta_size, meta_pct = vals
        self.lv_free_data = data_size * (100 - data_pct) / 100
        self.lv_free_data_pct = 100 - data_pct
        self.lv_free_metadata = meta_size * (100 - meta_pct) / 100
        self.lv_free_metadata_pct = 100 - meta_pct

    def report(self, pct_threshold=100):
        data = (
            ('Free filesystem space', True, self.fs_free, self.fs_free_pct),
            ('Free inodes', False, self.ino_free, self.ino_free_pct),
            ('Free LVM data space', True, self.lv_free_data,
                    self.lv_free_data_pct),
            ('Free LVM metadata space', True, self.lv_free_metadata,
                    self.lv_free_metadata_pct),
        )
        printed = False
        for label, humanize, value, pct in data:
            if humanize:
                value = humanize_size(value)
            else:
                value = str(value) + 4 * " "
            if pct < pct_threshold:
                print '%-25s %14s (%4.1f%%)' % (label + ':', value, pct)
                printed = True
        return printed


def cmd_df(config, args):
    settings = config['settings']
    vg, lv = settings['backup-lv'].split('/')
    status = StorageStatus(vg, lv, settings['root'])
    if args.check:
        threshold = settings.get('df-warning', 5)
    else:
        threshold = 100
    printed = status.report(threshold)
    if args.check and printed:
        return 1


def cmd_ls(config, args):
    for snapshot in PhysicalSnapshot.list():
        print snapshot


def cmd_mount(config, args):
    settings = config['settings']
    snapshots = [Snapshot(s).get_physical(settings) for s in args.snapshot]
    for snapshot in snapshots:
        mountpoint = make_dir_path(settings['root'], 'Snapshots',
                snapshot.name)
        try:
            snapshot.mount(mountpoint)
        except:
            os.rmdir(mountpoint)
            raise
        print mountpoint


def cmd_umount(config, args):
    settings = config['settings']
    root_dir = settings['root']
    snapshot_dir = os.path.join(root_dir, 'Snapshots')
    if args.all:
        root_dev = os.stat(root_dir).st_dev
        snapshots = [name for name in sorted(os.listdir(snapshot_dir))
                if os.stat(os.path.join(snapshot_dir, name)).st_dev !=
                root_dev]
    else:
        snapshots = args.snapshot
        if not snapshots:
            raise ValueError('At least one snapshot must be specified')
    snapshots = [Snapshot(s).get_physical(settings) for s in snapshots]
    for snapshot in snapshots:
        mountpoint = os.path.join(snapshot_dir, snapshot.name)
        snapshot.umount(mountpoint)
        os.rmdir(mountpoint)


def _setup():
    parser = subparsers.add_parser('df',
            help='report available disk space')
    parser.set_defaults(func=cmd_df)
    parser.add_argument('-c', '--check', action='store_true',
            help='only report problems')

    parser = subparsers.add_parser('ls',
            help='list snapshots')
    parser.set_defaults(func=cmd_ls)

    parser = subparsers.add_parser('mount',
            help='mount one or more snapshots')
    parser.set_defaults(func=cmd_mount)
    parser.add_argument('snapshot', nargs='+',
            help='snapshot name')

    parser = subparsers.add_parser('umount',
            help='unmount one or more snapshots')
    parser.set_defaults(func=cmd_umount)
    parser.add_argument('-a', '--all', action='store_true',
            help='unmount all mounted snapshots')
    parser.add_argument('snapshot', nargs='*',
            help='snapshot name')

_setup()
