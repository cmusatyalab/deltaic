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

import calendar
from contextlib import contextmanager
from cStringIO import StringIO
import errno
import fcntl
import os
from pybloom import ScalableBloomFilter
import random
import subprocess
from tempfile import mkdtemp, mkstemp
import xattr

TEMPFILE_PREFIX = '.backup-tmp'

class LockConflict(Exception):
    pass


class BloomSet(object):
    def __init__(self, initial_capacity=1000, error_rate=0.0001):
        self._set = ScalableBloomFilter(initial_capacity=initial_capacity,
                error_rate=error_rate,
                mode=ScalableBloomFilter.LARGE_SET_GROWTH)
        # False positives in the Bloom filter will cause us to fail to
        # garbage-collect an object.  Salt the Bloom filter to ensure
        # that we get a different set of false positives on every run.
        self._bloom_salt = os.urandom(2)

    def add(self, name):
        self._set.add(self._bloom_key(name))

    def __contains__(self, name):
        # May return false positives.
        return self._bloom_key(name) in self._set

    def _bloom_key(self, name):
        if isinstance(name, unicode):
            name = name.encode('utf-8')
        return self._bloom_salt + name


def gc_directory_tree(root_dir, valid_paths, report_callback=None):
    if report_callback is None:
        report_callback = lambda path, is_dir: None
    def handle_err(err):
        raise err
    for dirpath, _, filenames in os.walk(root_dir, topdown=False,
            onerror=handle_err):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if filepath not in valid_paths:
                report_callback(filepath, False)
                os.unlink(filepath)
        if dirpath not in valid_paths:
            try:
                os.rmdir(dirpath)
                report_callback(dirpath, True)
            except OSError:
                # Directory not empty
                pass


@contextmanager
def write_atomic(path, prefix=TEMPFILE_PREFIX, suffix=''):
    # Open a temporary file for writing.  On successfully exiting the
    # context, close the file and rename it to the specified path.
    # On exiting due to exception, close and delete the temporary file.
    #
    # Any source using this function must eventually garbage-collect
    # temporary files, and must ignore them during restores.
    fd, tempfile = mkstemp(prefix=prefix, suffix=suffix,
            dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, 'wb') as fh:
            yield fh
        os.chmod(tempfile, 0644)
        os.rename(tempfile, path)
    except:
        os.unlink(tempfile)
        raise


class UpdateFile(object):
    '''File-like object, only for writing, which atomically overwrites
    the specified file only if the new data is different from the old.
    Avoids unnecessary LVM COW.
    
    Any source using this class must eventually garbage-collect temporary
    files, and must ignore them during restores.'''

    def __init__(self, path, prefix=TEMPFILE_PREFIX, suffix='',
            block_size=256 << 10):
        self.modified = None
        self._coroutine = self._start_coroutine(path, prefix, suffix,
                block_size)
        self._buf = ''
        self._desired_size = self._coroutine.next()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type is None:
            self.close()
        else:
            self.abort()

    def __del__(self):
        self.close()

    def write(self, buf):
        self._buf += buf
        while len(self._buf) >= self._desired_size:
            self._send(self._desired_size)

    def close(self):
        # First send remaining buffered data, then send '' until the
        # coroutine exits
        while self._coroutine:
            self._send(len(self._buf))

    def abort(self):
        # Terminate coroutine, abort new file
        if self._coroutine:
            self._coroutine.close()
            self._coroutine = None

    def _send(self, len):
        assert self._coroutine
        buf = self._buf[0:self._desired_size]
        self._buf = self._buf[self._desired_size:]
        try:
            self._desired_size = self._coroutine.send(buf)
        except StopIteration:
            self._coroutine = None

    def _start_coroutine(self, path, prefix, suffix, block_size):
        # "buf = input_data.read(count)" is spelled "buf = yield count".

        # Open old file if it exists
        try:
            oldfh = open(path, 'rb')
        except IOError:
            oldfh = None

        try:
            # Find length of common prefix
            prefix_len = 0
            databuf = ''
            while oldfh:
                oldbuf = oldfh.read(block_size)
                if oldbuf == '':
                    databuf = yield block_size
                    if databuf == '':
                        # Files are identical
                        self.modified = False
                        return
                    break
                databuf = yield len(oldbuf)
                if oldbuf != databuf:
                    break
                prefix_len += len(databuf)

            # Write new file
            with write_atomic(path, prefix=prefix, suffix=suffix) as newfh:
                # Copy common prefix
                if oldfh:
                    oldfh.seek(0)
                    while prefix_len:
                        buf = oldfh.read(min(block_size, prefix_len))
                        newfh.write(buf)
                        prefix_len -= len(buf)

                # Copy leftover data from common-prefix search
                newfh.write(databuf)

                # Copy remaining data
                while True:
                    databuf = yield block_size
                    if databuf == '':
                        break
                    newfh.write(databuf)

            self.modified = True
        finally:
            if oldfh:
                oldfh.close()


def update_file(path, data, prefix=TEMPFILE_PREFIX, suffix='',
        block_size=256 << 10):
    # Avoid unnecessary LVM COW by only updating the file if its data has
    # changed.  data can be a string or a file-like object which does
    # not need to be seekable.
    #
    # Any source using this function must eventually garbage-collect
    # temporary files, and must ignore them during restores.

    with UpdateFile(path, prefix=prefix, suffix=suffix,
            block_size=block_size) as fh:
        if hasattr(data, 'read'):
            while True:
                buf = data.read(block_size)
                if buf == '':
                    break
                fh.write(buf)
        else:
            fh.write(data)
    return fh.modified


def _test_update_file():
    data = ''.join(chr(random.getrandbits(8)) for i in range((2 << 20) + 30))
    dirpath = mkdtemp(prefix='update-file-')
    path = os.path.join(dirpath, 'file')

    def init(data):
        with open(path, 'wb') as fh:
            fh.write(data)
    def change_byte(data, index):
        char = ord(data[index])
        char += 1
        if char > 255:
            char = 0
        return data[0:index] + chr(char) + data[index + 1:]
    def check(data):
        with open(path, 'rb') as fh:
            assert fh.read() == data

    try:
        # Write new file
        assert not os.path.exists(path)
        assert update_file(path, data)
        check(data)

        # Update file, nothing changed
        mtime = os.stat(path).st_mtime
        assert not update_file(path, data)
        check(data)
        assert os.stat(path).st_mtime == mtime

        # Update empty file
        open(path, 'w').close()
        assert os.stat(path).st_size == 0
        assert update_file(path, data)
        check(data)

        # Update file starting from various byte offsets
        for offset in 0, 1000, 512 << 10, 520 << 10:
            init(data)
            ndata = change_byte(data, offset)
            ndata = change_byte(ndata, 1 << 20)
            assert update_file(path, ndata)
            check(ndata)

        # Extend file
        init(data)
        ndata = data + 'asdfghjkl'
        assert update_file(path, ndata)
        check(ndata)

        # Truncate file
        init(data)
        ndata = data[:300000]
        assert update_file(path, ndata)
        check(ndata)

        # File handle
        init(data)
        ndata = change_byte(data, 1234567)
        assert update_file(path, StringIO(ndata))
        check(ndata)

        # Streaming writes
        init(data)
        ndata = data + 'asdfghjkl'
        with UpdateFile(path) as fh:
            for i in range(0, len(ndata), 384 << 10):
                fh.write(ndata[i:i + (384 << 10)])
        assert fh.modified
        check(ndata)

        # Streaming writes with failure
        init(data)
        ndata = 'q' + data[:300000]
        try:
            with UpdateFile(path) as fh:
                fh.write(ndata)
                raise ValueError
        except ValueError:
            pass
        assert fh.modified is None
        check(data)

        # Streaming writes, no change
        init(data)
        with UpdateFile(path) as fh:
            fh.write(data)
        assert fh.modified is False
        check(data)

    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
        os.rmdir(dirpath)


@contextmanager
def lockfile(settings, name):
    root_dir = settings['root']
    root_parent = os.path.dirname(root_dir)
    if os.stat(root_dir).st_dev == os.stat(root_parent).st_dev:
        raise IOError('Backup filesystem is not mounted')

    lock_dir = make_dir_path(root_dir, '.lock')
    lock_file = os.path.join(lock_dir, name)
    with open(lock_file, 'w') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError, e:
            if e.errno in (errno.EACCES, errno.EAGAIN):
                raise LockConflict('Another action is already running.')
            else:
                raise
        yield


class XAttrs(object):
    def __init__(self, path):
        self._attrs = xattr.xattr(path, xattr.XATTR_NOFOLLOW)

    def __contains__(self, key):
        return key in self._attrs

    def __getitem__(self, key):
        return self._attrs[key]

    def get(self, key, default=None):
        try:
            return self._attrs[key]
        except KeyError:
            return default

    def update(self, key, value):
        if self.get(key) != value:
            self._attrs[key] = value

    def delete(self, key):
        try:
            del self._attrs[key]
        except KeyError:
            pass


def random_do_work(settings, option, default_probability):
    # Decide probabilistically whether to do some extra work.
    return random.random() < settings.get(option, default_probability)


def datetime_to_time_t(dt):
    return calendar.timegm(dt.utctimetuple())


def make_dir_path(*args):
    path = os.path.join(*args)
    try:
        os.makedirs(path)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise
    return path


def humanize_size(size):
    units = ('  B', 'KiB', 'MiB', 'GiB', 'TiB')
    index = 0
    while size >= 1024 and index < len(units):
        size /= 1024
        index += 1
    return '%.1f %s' % (size, units[index])


class Pipeline(object):
    def __init__(self, cmds, in_fh=None, out_fh=None, env=None):
        self._procs = []

        fin = []
        fout = []
        try:
            fin.append(in_fh)
            for _ in range(len(cmds) - 1):
                pipe_r, pipe_w = os.pipe()
                fout.append(pipe_w)
                fin.append(pipe_r)
            fout.append(out_fh)

            for i, cmd in enumerate(cmds):
                proc = subprocess.Popen(cmd, stdin=fin[i], stdout=fout[i],
                        env=env, close_fds=True)
                self._procs.append(proc)
        finally:
            for fh in fout + fin:
                if fh is not in_fh and fh is not out_fh:
                    os.close(fh)

    def close(self, terminate=False):
        if terminate:
            for proc in self._procs:
                proc.terminate()
        failed = False
        while self._procs:
            proc = self._procs.pop()
            proc.wait()
            failed = failed or proc.returncode
        if failed and not terminate:
            raise IOError('Pipeline process returned non-zero exit status')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        # On exception, terminate processes to avoid deadlock
        self.close(exc_type is not None)

    def __del__(self):
        self.close()


if __name__ == '__main__':
    _test_update_file()
