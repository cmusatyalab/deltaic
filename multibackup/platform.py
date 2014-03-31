from ctypes import *
import os

_libc = CDLL('libc.so.6', use_errno=True)


class Timeval(Structure):
    _fields_ = [
        ('tv_sec', c_long),
        ('tv_usec', c_long),
    ]


TwoTimevals = Timeval * 2
_lutimes = _libc.lutimes
_lutimes.argtypes = [c_char_p, TwoTimevals]
_lutimes.restype = c_int


def lutime(path, time):
    time = Timeval(int(time), int((time - int(time)) * 1e6))
    if _lutimes(path, TwoTimevals(time, time)):
        raise OSError('lutimes() failed: %s' % os.strerror(get_errno()))
