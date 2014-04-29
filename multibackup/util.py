import calendar
from contextlib import contextmanager
from cStringIO import StringIO
import errno
import os
from pybloom import ScalableBloomFilter
import random
from tempfile import mkdtemp, mkstemp

TEMPFILE_PREFIX = '.backup-tmp'

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


def update_file(path, data, prefix=TEMPFILE_PREFIX, suffix='',
        block_size=256 << 10):
    # Avoid unnecessary LVM COW by only updating the file if its data has
    # changed.  data can be a string or a file-like object which does
    # not need to be seekable.
    #
    # Any source using this function must eventually garbage-collect
    # temporary files, and must ignore them during restores.

    if not hasattr(data, 'read'):
        data = StringIO(data)

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
                databuf = data.read(block_size)
                if databuf == '':
                    # Files are identical
                    return False
                break
            databuf = data.read(len(oldbuf))
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
                databuf = data.read(block_size)
                if databuf == '':
                    break
                newfh.write(databuf)

        return True
    finally:
        if oldfh:
            oldfh.close()


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
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
        os.rmdir(dirpath)


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


if __name__ == '__main__':
    _test_update_file()
