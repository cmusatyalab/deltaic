import Queue
import subprocess
import sys
from threading import Thread

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
