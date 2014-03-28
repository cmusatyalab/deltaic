from __future__ import division
from datetime import date, datetime
import subprocess

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
