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


_fallocate = _libc.fallocate64
_fallocate.argtypes = [c_int, c_int, c_longlong, c_longlong]
_fallocate.restype = c_int
_FALLOC_FL_KEEP_SIZE = 1
_FALLOC_FL_PUNCH_HOLE = 2


def punch(fh, offset, length):
    if hasattr(fh, 'punch'):
        fh.punch(offset, length)
    elif _fallocate(fh.fileno(), _FALLOC_FL_PUNCH_HOLE | _FALLOC_FL_KEEP_SIZE,
            offset, length):
        raise OSError('fallocate() failed: %s' % os.strerror(get_errno()))
