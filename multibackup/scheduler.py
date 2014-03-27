from datetime import date
from itertools import chain
import os
import Queue
import random
import subprocess
import sys
from threading import Thread
import yaml

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
    def __init__(self, settings, name, force_s3_acls=False):
        Task.__init__(self)
        out_dir = os.path.join(settings['root'], 'rgw', name)
        self.args = ['rgwbackup', '-v', settings['rgw-server'], name, out_dir]
        if settings.get('rgw-secure', False):
            self.args.append('-s')
        if force_s3_acls:
            self.args.append('-A')


class RGWSource(Source):
    LABEL = 'rgw'

    def __init__(self, config, options):
        Source.__init__(self, config)
        for name in self._manifest:
            self._queue.put(BucketTask(self._settings, name,
                    force_s3_acls='rgw-force-acls' in options))


class ImageTask(Task):
    SECTION = 'images'

    def __init__(self, settings, pool, name, friendly_name):
        Task.__init__(self)
        out_path = os.path.join(settings['root'], 'rbd', pool,
                self.SECTION, friendly_name)
        self.args = ['rbdbackup', pool, name, out_path]


class SnapshotTask(ImageTask):
    SECTION = 'snapshots'

    def __init__(self, settings, pool, name, friendly_name):
        ImageTask.__init__(self, settings, pool, name, friendly_name)
        self.args.append('-s')


class RBDSource(Source):
    LABEL = 'rbd'

    def __init__(self, config, options):
        Source.__init__(self, config)
        for pool, info in self._manifest.get('images', {}).items():
            for friendly_name, name in info.items():
                self._queue.put(ImageTask(self._settings, pool, name,
                        friendly_name))
        for pool, info in self._manifest.get('snapshots', {}).items():
            for friendly_name, name in info.items():
                self._queue.put(SnapshotTask(self._settings, pool, name,
                        friendly_name))


class RsyncTask(Task):
    def __init__(self, settings, hostname, info):
        Task.__init__(self)
        out_path = os.path.join(settings['root'], 'rsync',
                hostname.split('.')[0])
        self.args = ['rsyncbackup', '-v', hostname, out_path]
        self.args.extend(info['mounts'])
        for rule in chain(settings.get('rsync-exclude', []),
                info.get('exclude', [])):
            self.args.extend(['-x', rule])
        if 'pre' in info:
            self.args.extend(['--pre', info['pre']])
        if 'post' in info:
            self.args.extend(['--post', info['post']])


class RsyncSource(Source):
    LABEL = 'rsync'

    def __init__(self, config, options):
        Source.__init__(self, config)
        for hostname, info in self._manifest.items():
            self._queue.put(RsyncTask(self._settings, hostname, info))


class CodaTask(Task):
    def __init__(self, settings, server, volume):
        Task.__init__(self)
        out_path = os.path.join(settings['root'], 'coda',
                server.split('.')[0], volume)
        self.args = ['codabackup', '-v', server, volume, out_path]
        if random.random() >= settings.get('coda-full-probability', 0.143):
            self.args.append('-i')
        for prog in 'volutil', 'codadump2tar':
            path = settings.get('coda-%s-path' % prog)
            if path is not None:
                self.args.extend(['--%s' % prog, path])


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


def cmd_run(args):
    with open(args.config_path) as fh:
        config = yaml.safe_load(fh)

    run_tasks(config, args.options, args.sources)
    if args.snapshot:
        create_snapshot(config['settings'])


def _setup():
    parser = subparsers.add_parser('run',
            help='run a backup')
    parser.set_defaults(func=cmd_run)
    parser.add_argument('config_path', metavar='config-path',
            help='path to config file')
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
