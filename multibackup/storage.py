from __future__ import division
from datetime import date, datetime, timedelta
import os
import subprocess
import sys

from .command import subparsers

class Snapshot(object):
    DATE_FMT = '%Y%m%d'
    TAG = 'backup-snapshot'

    def __init__(self, vg, name):
        self.vg = vg
        self.name = name
        datecode, revision = name.split('-')
        self.date = datetime.strptime(datecode, self.DATE_FMT).date()
        self.revision = int(revision)
        self.year, self.week, self.day = self.date.isocalendar()
        # 4, 7-day weeks/month => 13 months/year, 14 on long years
        self.month = ((self.week - 1) // 4) + 1

    def __str__(self):
        return '%s/%s' % (self.vg, self.name)

    def __repr__(self):
        return 'Snapshot(%s, %s)' % (repr(self.vg), repr(self.name))

    def __cmp__(self, other):
        if not isinstance(other, self.__class__):
            return hash(other) - hash(self)
        if self.date < other.date:
            return -1
        elif self.date > other.date:
            return 1
        else:
            return self.revision - other.revision

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
            ret.append(Snapshot(vg, lv))
        return ret

    @classmethod
    def create(cls, vg, lv, verbose=False):
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
            subprocess.check_call(['sudo', 'lvremove',
                    '%s/%s' % (self.vg, self.name)], stdin=null,
                    stdout=None if verbose else null)


def select_snapshots_to_remove(settings, snapshots):
    conf_duplicate_days = settings.get('gc-duplicate-days', 14)
    conf_daily_weeks = settings.get('gc-daily-weeks', 8)
    conf_weekly_months = settings.get('gc-weekly-months', 12)

    duplicate_thresh = date.today() - timedelta(conf_duplicate_days)
    daily_thresh = date.today() - timedelta(conf_daily_weeks * 7)
    weekly_thresh = date.today() - timedelta(conf_weekly_months * 28)

    # First filter out all but the last backup per day, except for recent
    # backups
    filtered = []
    prev = None
    for cur in sorted(snapshots, reverse=True):
        if (prev is None or prev.date != cur.date or
                cur.date > duplicate_thresh):
            filtered.insert(0, cur)
        prev = cur

    # Do the rest of the filtering
    def keep(prev, cur):
        # Always keep oldest backup
        if prev is None:
            return True
        # Keep multiple backups per day if not already filtered out
        if prev.date == cur.date:
            return True
        # Keep daily backups for gc-daily-weeks weeks
        if cur.date > daily_thresh and prev.date != cur.date:
            return True
        # Keep weekly backups for gc-weekly-months 28-day months
        if cur.date > weekly_thresh and (prev.year != cur.year or
                prev.week != cur.week):
            return True
        # Keep monthly backups forever
        if prev.month != cur.month:
            return True
        return False
    remove = set(snapshots)
    prev = None
    for cur in filtered:
        if keep(prev, cur):
            remove.remove(cur)
        prev = cur
    return remove


class StorageStatus(object):
    def __init__(self, vg, lv, mountpoint):
        # Get filesystem stats
        st = os.statvfs(mountpoint)
        self.fs_free_gb = st.f_frsize * st.f_bavail / (1 << 30)
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
                '--units', 'g',
                '-o', 'lv_size,data_percent,lv_metadata_size,metadata_percent',
                '%s/%s' % (vg, pool_lv)], stdout=subprocess.PIPE)
        out, _ = proc.communicate()
        if proc.returncode:
            raise IOError("Couldn't examine pool LV: lvs returned %d" %
                    proc.returncode)
        vals = [float(v) for v in out.split()]
        data_size, data_pct, meta_size, meta_pct = vals
        self.lv_free_data_gb = data_size * (100 - data_pct) / 100
        self.lv_free_data_pct = 100 - data_pct
        self.lv_free_metadata_gb = meta_size * (100 - meta_pct) / 100
        self.lv_free_metadata_pct = 100 - meta_pct

    def report(self, pct_threshold=100):
        data = (
            ('Free filesystem space', 'GB', self.fs_free_gb,
                    self.fs_free_pct),
            ('Free inodes', '', self.ino_free, self.ino_free_pct),
            ('Free LVM data space', 'GB', self.lv_free_data_gb,
                    self.lv_free_data_pct),
            ('Free LVM metadata space', 'GB', self.lv_free_metadata_gb,
                    self.lv_free_metadata_pct),
        )
        printed = False
        for label, units, value, pct in data:
            if pct < pct_threshold:
                print '%-25s %11d %2s (%4.1f%%)' % (label + ':', value,
                        units, pct)
                printed = True
        return printed


def cmd_prune(config, args):
    settings = config['settings']
    snapshots = Snapshot.list()
    remove = select_snapshots_to_remove(settings, snapshots)
    for cur in sorted(remove):
        if args.dry_run:
            print cur
        else:
            cur.remove(verbose=args.verbose)


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
        sys.exit(1)


def _setup():
    parser = subparsers.add_parser('prune',
            help='delete old LVM snapshots')
    parser.set_defaults(func=cmd_prune)
    parser.add_argument('-n', '--dry-run', action='store_true',
            help='just print the snapshots that would be removed')
    parser.add_argument('-v', '--verbose', action='store_true',
            help='report actions taken')

    parser = subparsers.add_parser('df',
            help='report available disk space')
    parser.set_defaults(func=cmd_df)
    parser.add_argument('-c', '--check', action='store_true',
            help='only report problems')

_setup()