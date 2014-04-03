import os
from pybloom import ScalableBloomFilter

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


def update_file(path, data):
    # Avoid unnecessary LVM COW by only updating the file if its data has
    # changed
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0666)
    with os.fdopen(fd, 'r+b') as fh:
        if fh.read() != data:
            fh.seek(0)
            fh.write(data)
            fh.truncate()
