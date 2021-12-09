# SPDX-FileCopyrightText: 2001-2021 Python Software Foundation
# SPDX-License-Identifier: PSF-2.0 OR 0BSD
#
# example from https://docs.python.org/3/library/pkgutil.html

from pkgutil import extend_path
from typing import List

__path__: List[str] = extend_path(__path__, __name__)
