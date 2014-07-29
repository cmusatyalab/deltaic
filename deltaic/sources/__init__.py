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

from datetime import datetime, date
import os
import Queue
import subprocess
import sys
from threading import Thread

from ..command import get_cmdline_for_subcommand
from ..util import make_dir_path

class Unit(object):
    def __init__(self):
        self.root = None
        self.backup_args = None

    def __str__(self):
        return self.root


class Task(object):
    DATE_FMT = '%Y%m%d'
    LOG_EXCERPT_INPUT_BYTES = 8192
    LOG_EXCERPT_MAX_BYTES = 4096
    LOG_EXCERPT_MAX_LINES = 10

    def __init__(self, thread_count, units):
        self._queue = Queue.Queue()
        for unit in units:
            self._queue.put(unit)
        self._success = True
        self._threads = [Thread(target=self._worker)
                for i in range(thread_count)]

    def start(self):
        for thread in self._threads:
            thread.start()

    def _worker(self):
        while True:
            try:
                unit = self._queue.get_nowait()
            except Queue.Empty:
                return
            if not self._execute(unit):
                self._success = False

    def _execute(self, unit):
        raise NotImplementedError

    def wait(self):
        for thread in self._threads:
            thread.join()
        return self._success

    def _run_subcommand(self, name, args, log_dir):
        log_base = os.path.join(log_dir, date.today().strftime(self.DATE_FMT))
        timestamp = lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        sys.stdout.write('Starting %s\n' % name)
        command = get_cmdline_for_subcommand(args)
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
            with open(log_base + '.err') as err:
                # Read LOG_EXCERPT_INPUT_BYTES
                err.seek(0, 2)
                start = max(0, err.tell() - self.LOG_EXCERPT_INPUT_BYTES)
                err.seek(start)
                excerpt = err.read(self.LOG_EXCERPT_INPUT_BYTES).strip()
                truncated = start > 0
                # Drop exception backtraces
                accept = True
                excerpt_lines = []
                for line in excerpt.split('\n'):
                    if accept:
                        if line == 'Traceback (most recent call last):':
                            accept = False
                        else:
                            excerpt_lines.append(line)
                    elif not line.startswith(' '):
                        accept = True
                        excerpt_lines.append(line)
                # Reduce to LOG_EXCERPT_MAX_BYTES
                excerpt = '\n'.join(excerpt_lines)
                if len(excerpt) > self.LOG_EXCERPT_MAX_BYTES:
                    excerpt = excerpt[-self.LOG_EXCERPT_MAX_BYTES:]
                    truncated = True
                # Reduce to LOG_EXCERPT_MAX_LINES
                excerpt_lines = excerpt.split('\n')
                if len(excerpt_lines) > self.LOG_EXCERPT_MAX_LINES:
                    excerpt_lines = excerpt_lines[-self.LOG_EXCERPT_MAX_LINES:]
                    truncated = True
                # Add truncation indicator
                if truncated:
                    excerpt_lines.insert(0, '[...]')
                # Serialize
                excerpt = '\n'.join(' ' * 3 + l for l in excerpt_lines)
            sys.stderr.write('Failed:  %s\n   %s\n%s\n' % (name,
                    ' '.join(command), excerpt))

        sys.stdout.write('Ending   %s\n' % name)
        return ret == 0


class _SourceBackupTask(Task):
    def __init__(self, settings, thread_count, units):
        Task.__init__(self, thread_count, units)
        self._settings = settings

    def _execute(self, unit):
        log_dir = make_dir_path(self._settings['root'], 'Logs', unit.root)
        return self._run_subcommand(unit.root, unit.backup_args, log_dir)


class Source(object):
    def __init__(self, config):
        self._settings = config.get('settings', {})
        self._manifest = config.get(self.LABEL, {})

    @classmethod
    def get_sources(cls):
        sources = {}
        for subclass in cls.__subclasses__():
            if hasattr(subclass, 'LABEL'):
                sources[subclass.LABEL] = subclass
        return sources

    def get_units(self):
        raise NotImplementedError

    def get_backup_task(self):
        thread_count = self._settings.get('%s-workers' % self.LABEL, 1)
        return _SourceBackupTask(self._settings, thread_count,
                self.get_units())


# Now import submodules that need these definitions
from . import coda, github, rbd, rgw, rsync
