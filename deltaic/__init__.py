# SPDX-FileCopyrightText: 2001-2021 Python Software Foundation
# SPDX-License-Identifier: 0BSD
#
# example from https://docs.python.org/3/library/pkgutil.html
#

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)
