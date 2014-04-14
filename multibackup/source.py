from datetime import datetime, date
import os
import Queue
import subprocess
import sys
from threading import Thread

from .command import get_cmdline_for_subcommand

class Task(object):
    DATE_FMT = '%Y%m%d'

    def __init__(self, settings):
        self._settings = settings

    def __str__(self):
        return self.root

    def run(self):
        log_dir = os.path.join(self._settings['root'], 'Logs', self.root)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_base = os.path.join(log_dir, date.today().strftime(self.DATE_FMT))
        timestamp = lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        sys.stdout.write('Starting %s\n' % self)
        command = get_cmdline_for_subcommand(self.args)
        with open('/dev/null', 'r+') as null:
            with open(log_base + '.err', 'a') as err:
                with open(log_base + '.out', 'a') as out:
                    for fh in out, err:
                        fh.write('# Starting task at %s\n' % timestamp())
                        fh.write('# %s\n' % ' '.join(command))
                        fh.flush()
                    ret = subprocess.call(command, stdin=null, stdout=out,
                            stderr=err, close_fds=True)
                    for fh in out, err:
                        if ret < 0:
                            fh.write('# Task died on signal %d\n' % -ret)
                        else:
                            fh.write('# Task exited with status %d\n' % ret)
                        fh.write('# Ending task at %s\n\n' % timestamp())
        if ret:
            sys.stderr.write('%s failed\n' % self)
        sys.stdout.write('Ending %s\n' % self)


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
