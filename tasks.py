#
# Deltaic - an efficient backup system supporting multiple data sources
#
# Copyright (c) 2021 Carnegie Mellon University
#
# SPDX-License-Identifier: GPL-2.0-only
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

# pyinvoke file with common maintenance tasks

from invoke import task


@task
def update_dependencies(c):
    c.run("poetry update")
    c.run("poetry run pre-commit autoupdate")
    c.run("poetry run pre-commit run -a")
    # run tests....
    c.run("git commit -m'Updated dependencies' poetry.lock .pre-commit-config.yaml")
