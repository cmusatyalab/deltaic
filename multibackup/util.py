from contextlib import contextmanager
import os
from pybloom import ScalableBloomFilter
import random
from tempfile import mkstemp

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


def update_file(path, data, prefix=TEMPFILE_PREFIX, suffix=''):
    # Avoid unnecessary LVM COW by only updating the file if its data has
    # changed.
    #
    # Any source using this function must eventually garbage-collect
    # temporary files, and must ignore them during restores.
    try:
        with open(path, 'rb') as fh:
            if fh.read() == data:
                return False
    except IOError:
        pass
    with write_atomic(path, prefix=prefix, suffix=suffix) as fh:
        fh.write(data)
    return True


def random_do_work(settings, option, default_probability):
    # Decide probabilistically whether to do some extra work.
    return random.random() < settings.get(option, default_probability)
