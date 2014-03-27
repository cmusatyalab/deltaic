from datetime import date
import os
import Queue
import random
import subprocess
import sys
from threading import Thread

from .command import subparsers

class Task(object):
    def run(self):
        command = [sys.executable, sys.argv[0]] + list(self.args)
        print ' '.join(command)
        subprocess.check_call(command, close_fds=True)


class Source(object):
    def __init__(self, config):
        self._settings = config.get('settings', {})
        self._manifest = config.get(self.LABEL, {})
        self._queue = Queue.Queue()
        self._thread_count = self._settings.get('%s-workers' % self.LABEL, 1)
        self._threads = []

    @classmethod
    def get_sources(cls):
        sources = {}
        for subclass in cls.__subclasses__():
            if hasattr(subclass, 'LABEL'):
                sources[subclass.LABEL] = subclass
        return sources

    def start(self):
        for n in range(self._thread_count):
            thread = Thread(target=self._worker)
            thread.start()
            self._threads.append(thread)

    def _worker(self):
        while True:
            try:
                task = self._queue.get_nowait()
            except Queue.Empty:
                return
            task.run()

    def wait(self):
        for thread in self._threads:
            thread.join()
        self._threads = []


class BucketTask(Task):
    def __init__(self, name, force_s3_acls=False):
        Task.__init__(self)
        self.args = ['rgw', 'backup', name]
        if force_s3_acls:
            self.args.append('-A')


class RGWSource(Source):
    LABEL = 'rgw'

    def __init__(self, config, options):
        Source.__init__(self, config)
        for name in self._manifest:
            self._queue.put(BucketTask(name,
                    force_s3_acls='rgw-force-acls' in options))


class ImageTask(Task):
    def __init__(self, pool, friendly_name):
        Task.__init__(self)
        self.args = ['rbd', 'backup', pool, friendly_name]


class SnapshotTask(ImageTask):
    def __init__(self, pool, friendly_name):
        ImageTask.__init__(self, pool, friendly_name)
        self.args.append('-s')


class RBDSource(Source):
    LABEL = 'rbd'

    def __init__(self, config, options):
        Source.__init__(self, config)
        for pool, info in self._manifest.items():
            for friendly_name in info.get('images', {}):
                self._queue.put(ImageTask(pool, friendly_name))
            for friendly_name in info.get('snapshots', {}):
                self._queue.put(SnapshotTask(pool, friendly_name))


class RsyncTask(Task):
    def __init__(self, hostname):
        Task.__init__(self)
        self.args = ['rsync', 'backup', hostname]


class RsyncSource(Source):
    LABEL = 'rsync'

    def __init__(self, config, options):
        Source.__init__(self, config)
        for hostname in sorted(self._manifest):
            self._queue.put(RsyncTask(hostname))


class CodaTask(Task):
    def __init__(self, settings, server, volume):
        Task.__init__(self)
        self.args = ['coda', 'backup', server, volume]
        if random.random() >= settings.get('coda-full-probability', 0.143):
            self.args.append('-i')


class CodaSource(Source):
    LABEL = 'coda'

    def __init__(self, config, options):
        Source.__init__(self, config)
        for group in self._manifest:
            for volume in group['volumes']:
                for server in group['servers']:
                    self._queue.put(CodaTask(self._settings, server, volume))


def run_tasks(config, options, source_names):
    source_map = Source.get_sources()
    sources = []
    for name in source_names:
        source = source_map[name](config, options)
        source.start()
        sources.append(source)
    for source in sources:
        source.wait()


def create_snapshot(settings):
    today = date.today().strftime('%Y%m%d')
    backup_lv = settings['backup-lv']
    vg = backup_lv.split('/')[0]
    with open('/dev/null', 'r+') as null:
        # Give up eventually in case test keeps failing
        for n in range(1, 100):
            snapshot_lv = '%s-%d' % (today, n)
            ret = subprocess.call(['sudo', 'lvs',
                    '%s/%s' % (vg, snapshot_lv)], stdout=null, stderr=null)
            if ret:
                break
        else:
            raise Exception("Couldn't locate unused snapshot LV")
    subprocess.check_call(['sudo', 'lvcreate', '-s', backup_lv, '-p', 'r',
            '-n', snapshot_lv, '--addtag', 'backup-snapshot'])


def cmd_run(config, args):
    run_tasks(config, args.options, args.sources)
    if args.snapshot:
        create_snapshot(config['settings'])


def _setup():
    parser = subparsers.add_parser('run',
            help='run a backup')
    parser.set_defaults(func=cmd_run)
    parser.add_argument('-o', '--opt', dest='options', action='append',
            default=[], metavar='OPTION',
            help='option string for source drivers')
    source_list = ', '.join(sorted(Source.get_sources()))
    parser.add_argument('-s', '--source', dest='sources', action='append',
            default=[], metavar='SOURCE',
            help='backup source to enable (%s)' % source_list)
    parser.add_argument('-S', '--no-snapshot', dest='snapshot',
            action='store_false', default=True,
            help='skip snapshot of backup volume')

_setup()
