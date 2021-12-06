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

import os
from datetime import date, timedelta

from .command import subparsers
from .storage import PhysicalSnapshot


def select_snapshots_to_remove(settings, snapshots):
    conf_duplicate_days = settings.get("gc-duplicate-days", 14)
    conf_daily_weeks = settings.get("gc-daily-weeks", 8)
    conf_weekly_months = settings.get("gc-weekly-months", 12)

    duplicate_thresh = date.today() - timedelta(conf_duplicate_days)
    daily_thresh = date.today() - timedelta(conf_daily_weeks * 7)
    weekly_thresh = date.today() - timedelta(conf_weekly_months * 28)

    # First filter out all but the last backup per day, except for recent
    # backups
    filtered = []
    prev = None
    for cur in sorted(snapshots, reverse=True):
        if prev is None or prev.date != cur.date or cur.date > duplicate_thresh:
            filtered.insert(0, cur)
        prev = cur

    # Do the rest of the filtering
    def keep(prev, cur):
        # Always keep oldest backup
        if prev is None:
            return True
        # Keep multiple backups per day if not already filtered out
        if prev.date == cur.date:
            return True
        # Keep daily backups for gc-daily-weeks weeks
        if cur.date > daily_thresh and prev.date != cur.date:
            return True
        # Keep weekly backups for gc-weekly-months 28-day months
        if cur.date > weekly_thresh and (
            prev.year != cur.year or prev.week != cur.week
        ):
            return True
        # Keep monthly backups forever
        if prev.month != cur.month:
            return True
        return False

    remove = set(snapshots)
    prev = None
    for cur in filtered:
        if keep(prev, cur):
            remove.remove(cur)
        prev = cur
    return remove


def prune_logs(root_dir, distinct_days):
    def handle_error(err):
        raise err

    for dirpath, _, filenames in os.walk(root_dir, onerror=handle_error):
        days = set()
        for filename in sorted(filenames, reverse=True):
            day, _ = os.path.splitext(filename)
            if day in days:
                continue
            elif len(days) < distinct_days:
                days.add(day)
                continue
            else:
                os.unlink(os.path.join(dirpath, filename))


def cmd_prune(config, args):
    settings = config["settings"]

    snapshots = PhysicalSnapshot.list()
    remove = select_snapshots_to_remove(settings, snapshots)
    for cur in sorted(remove):
        if args.dry_run:
            print cur
        else:
            cur.remove(verbose=args.verbose)

    if not args.dry_run:
        log_dir = os.path.join(settings["root"], "Logs")
        distinct_days = settings.get("gc-log-distinct-days", 60)
        prune_logs(log_dir, distinct_days)


def _setup():
    parser = subparsers.add_parser(
        "prune", help="delete old LVM snapshots and backup logs"
    )
    parser.set_defaults(func=cmd_prune)
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="just print the snapshots that would be removed",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="report snapshots removed"
    )


_setup()
