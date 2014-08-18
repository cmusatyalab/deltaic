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

from __future__ import division
from boto.exception import BotoServerError
import boto.glacier
import boto.sdb
import calendar
from contextlib import contextmanager
from datetime import datetime, timedelta
import dateutil.parser
from dateutil.tz import tzutc
import math
import os
import Queue
import sys
import threading
import time

from ..util import humanize_size
from . import Archiver, DownloadState

DEBUG = True

class _ConditionalCheckFailed(Exception):
    pass


@contextmanager
def _conditional_check():
    try:
        yield
    except BotoServerError, e:
        if e.error_code == 'ConditionalCheckFailed':
            raise _ConditionalCheckFailed()
        else:
            raise


def _debug(*args):
    if DEBUG:
        print >>sys.stderr, ' '.join(str(a) for a in args)


class AWSArchiver(Archiver):
    LABEL = 'aws'
    CHUNK_SIZE = 64 << 20
    RETRIEVAL_DELAY = 10
    JOB_CHECK_INTERVAL = 60
    DEFAULT_RETRIEVAL_RATE = 1 << 30
    BANDWIDTH_ITEM = '/bandwidth'
    TIMESLOT_SLOP = timedelta(minutes=2)

    # Glacier billing parameters
    MONTHLY_FREE_RETRIEVAL_FRACTION = .05
    BILLING_RETRIEVAL_HOURS = 4
    DOWNLOAD_QUANTUM = 1 << 20
    PROTECTED_PERIOD = timedelta(days=90, hours=1)  # 1-hour slop for paranoia

    def __init__(self, profile_name, profile):
        Archiver.__init__(self, profile_name, profile)

        sdb = boto.sdb.connect_to_region(
                profile['aws-region'],
                aws_access_key_id=profile['aws-access-key-id'],
                aws_secret_access_key=profile['aws-secret-access-key'])
        self._domain = sdb.create_domain(profile['aws-namespace'])

        glacier = boto.glacier.connect_to_region(
                profile['aws-region'],
                aws_access_key_id=profile['aws-access-key-id'],
                aws_secret_access_key=profile['aws-secret-access-key'])
        self._vault = glacier.create_vault(profile['aws-namespace'])
        self._storage_cost = profile.get('aws-storage-cost', 0.01)

    @classmethod
    def _make_set_item_name(cls, set_name):
        return set_name + '//'

    @classmethod
    def _make_archive_item_name(cls, set_name, archive_name):
        return '/'.join([set_name, archive_name])

    @classmethod
    def _quote_value(cls, value):
        return value.replace('"', '""')

    @classmethod
    def _now(cls, hour=False, month=False):
        now = datetime.now(tz=tzutc())
        if hour or month:
            now = now.replace(minute=0, second=0, microsecond=0)
        if month:
            now = now.replace(day=1, hour=0)
        return now

    def _wait_for_job(self, job_id):
        while True:
            job = self._vault.get_job(job_id)
            if job.completed:
                return job
            time.sleep(self.JOB_CHECK_INTERVAL)

    def _get_bandwidth_item(self):
        # Return expected serial number, and attributes including updated
        # serial number.  Delete obsolete data.
        item_name = self.BANDWIDTH_ITEM
        data = self._domain.get_item(item_name, consistent_read=True) or {}

        # Increment record serial number
        last_serial = data.get('serial', False)
        data['serial'] = str(int(data.get('serial', 0)) + 1)

        # Delete stale timeslot records.
        hourly_attrs = [k for k in data if k.startswith('bandwidth-')]
        delete_attrs = []
        now_hour = self._now(hour=True)
        for attr in hourly_attrs:
            timestamp_str = attr.replace('bandwidth-', '', 1)
            if dateutil.parser.parse(timestamp_str) < now_hour:
                del data[attr]
                delete_attrs.append(attr)
        if delete_attrs:
            # These attrs are permanently stale, so don't bother with the
            # serial number.
            self._domain.delete_attributes(item_name, delete_attrs)

        # Update current month attributes.  If the caller updates the item,
        # this will clear out the old values.
        now_month_str = self._now(month=True).date().isoformat()
        if data.get('current-month') != now_month_str:
            data.update({
                'current-month': now_month_str,
                'max-bandwidth-month': '0',
            })

        return (last_serial, data)

    def _request_download_quota(self, max_rate, download_size):
        # Calculate hourly figures
        size_per_hour = int(math.ceil(download_size /
                self.BILLING_RETRIEVAL_HOURS))
        hourly_quantum = self.DOWNLOAD_QUANTUM // self.BILLING_RETRIEVAL_HOURS

        # Get item
        last_serial, data = self._get_bandwidth_item()

        # Avoid allocating during the TIMESLOT_SLOP window around the
        # timeslot boundary, to avoid double-allocation within a slot
        # due to clock skew.
        cur_slot = self._now(hour=True)
        now = self._now()
        if now - cur_slot < self.TIMESLOT_SLOP:
            _debug('Refusing to allocate near beginning of timeslot')
            return (cur_slot - timedelta(hours=1), 0)
        if cur_slot + timedelta(hours=1) - now < self.TIMESLOT_SLOP:
            _debug('Refusing to allocate near end of timeslot')
            return (cur_slot, 0)

        # Parse item into timeslots
        timeslot_attrs = ['bandwidth-%s' %
                (cur_slot + timedelta(hours=n)).isoformat()
                for n in range(self.BILLING_RETRIEVAL_HOURS)]
        timeslot_bw = [int(data.get(k, 0)) for k in timeslot_attrs]
        # Later timeslots cannot have larger reservations than earlier ones
        for i in range(1, len(timeslot_bw)):
            assert timeslot_bw[i] <= timeslot_bw[i - 1]

        # Calculate available allocation
        allocation = max_rate - timeslot_bw[0]
        if allocation < size_per_hour:
            # We can't process the entire request.  Round down to next
            # quantum.
            allocation = (allocation // hourly_quantum) * hourly_quantum
        else:
            # Take only what we need
            allocation = size_per_hour
        # Reject if empty
        if allocation <= 0:
            return (cur_slot, 0)

        # Allocate
        for attr, bw in zip(timeslot_attrs, timeslot_bw):
            data[attr] = str(bw + allocation)

        # Update max bandwidth
        data['max-bandwidth-month'] = str(max(timeslot_bw[0] + allocation,
                int(data['max-bandwidth-month'])))

        # Update item
        try:
            with _conditional_check():
                self._domain.put_attributes(self.BANDWIDTH_ITEM, data,
                        expected_value=('serial', last_serial))
        except _ConditionalCheckFailed:
            return self._request_download_quota(max_rate, download_size)

        # Return timeslot and maximum request size
        return (cur_slot, min(allocation * self.BILLING_RETRIEVAL_HOURS,
                download_size))

    @classmethod
    def _seconds_to_next_timeslot(cls, timeslot):
        resume_time = timeslot + timedelta(hours=1) + cls.TIMESLOT_SLOP
        wait_time = resume_time - cls._now()
        if wait_time > timedelta(0):
            return wait_time.seconds + .000001 * wait_time.microseconds
        else:
            return 0

    def list_sets(self):
        now = self._now()
        sets = {}
        qstr = ('select `aws-set`, `aws-complete`, `aws-creation-time`, ' +
                '`size` from `%s` where `aws-set` is not null') % (
                self._domain.name)
        for metadata in self._domain.select(qstr, consistent_read=True):
            set_name = metadata['aws-set']
            set = sets.setdefault(set_name, {
                'count': 0,
                'size': 0,
                'complete': False,
                'protected': False,
            })
            if 'aws-complete' in metadata:
                set['complete'] = True
            creation_time = metadata.get('aws-creation-time')
            if creation_time and (now - dateutil.parser.parse(creation_time) <
                    self.PROTECTED_PERIOD):
                set['protected'] = True
            size = metadata.get('size')
            if size is not None:
                set['count'] += 1
                set['size'] += int(size)
        return sets

    def complete_set(self, set_name):
        item_name = self._make_set_item_name(set_name)
        self._domain.put_attributes(item_name, {
            'aws-set': set_name,
            'aws-complete': 'true',
        })

    def delete_set(self, set_name):
        items = {
            self._make_set_item_name(set_name): None,
        }
        aids = []
        qstr = ('select `aws-archive`, `aws-aid` from `%s` where ' +
                '`aws-set` = "%s" and `aws-archive` is not null') % (
                self._domain.name, self._quote_value(set_name))
        for res in self._domain.select(qstr, consistent_read=True):
            item_name = self._make_archive_item_name(set_name,
                    res['aws-archive'])
            items[item_name] = None
            aids.append(res['aws-aid'])
        self._domain.batch_delete_attributes(items)
        for aid in aids:
            self._vault.delete_archive(aid)

    def list_set_archives(self, set_name):
        qstr = ('select * from `%s` where `aws-set` = "%s" and ' +
                '`aws-archive` is not null') % (self._domain.name,
                self._quote_value(set_name))
        return dict((res['aws-archive'], dict(res))
                for res in self._domain.select(qstr, consistent_read=True))

    def upload_archive(self, set_name, archive_name, metadata, local_path):
        # We don't want to upload concurrently because the chunk size is
        # automatically scaled to fit the archive within 10k parts, and we
        # don't want to consume all of RAM.  The concurrent uploader includes
        # retry logic that we want to reuse, so run it with only 1 thread.
        aid = self._vault.concurrent_create_archive_from_file(local_path,
                ' '.join([set_name, archive_name]), part_size=self.CHUNK_SIZE,
                num_threads=1)
        try:
            metadata = dict(metadata)
            metadata.update({
                'aws-set': set_name,
                'aws-archive': archive_name,
                'aws-aid': aid,
                'aws-creation-time': self._now().isoformat(),
            })
            item_name = self._make_archive_item_name(set_name, archive_name)
            with _conditional_check():
                self._domain.put_attributes(item_name, metadata,
                        expected_value=('aws-aid', False))
        except _ConditionalCheckFailed:
            # Lost the race.  Pay the early-deletion penalty rather than
            # supporting multiple Glacier archives per Deltaic archive.
            self._vault.delete_archive(aid)
        except:
            # Don't leak the archive
            self._vault.delete_archive(aid)
            raise

    def download_archives(self, set_name, archive_list, max_rate=None):
        if not max_rate:
            max_rate = self.DEFAULT_RETRIEVAL_RATE

        # Collect metadata
        archive_names = []
        archive_sizes = []
        archive_paths = {}	# archive_name -> archive_path
        archive_metadata = {}	# archive_name -> metadata
        total_size = 0
        for archive_name, archive_path in archive_list:
            item_name = self._make_archive_item_name(set_name, archive_name)
            metadata = self._domain.get_item(item_name, consistent_read=True)
            if metadata:
                archive_size = int(metadata['size'])
                archive_names.append(archive_name)
                archive_sizes.append(archive_size)
                archive_paths[archive_name] = archive_path
                archive_metadata[archive_name] = metadata
                total_size += archive_size
            else:
                yield (archive_name, ValueError('No such archive'))
        if not archive_names:
            return

        # Give the user a chance to cancel before they are charged.
        print >>sys.stderr, 'Going to retrieve %s of data at %s/hour.' % (
                humanize_size(total_size), humanize_size(max_rate))
        for remaining in range(self.RETRIEVAL_DELAY, -1, -1):
            print >>sys.stderr, ('\rStarting in %d seconds. ' % remaining),
            sys.stdout.flush()
            if remaining:
                time.sleep(1)
        print >>sys.stderr, '\nRetrieving.'

        # Retrieve.
        state = DownloadState(zip(archive_names, archive_sizes))
        finished_queue = Queue.Queue()
        while not state.done:
            # Launch retrievals.
            timeslot = self._now(hour=True)
            _debug('New timeslot', timeslot)
            while not state.requests_done:
                # Figure out what to do next.
                metadata = archive_metadata[state.name]
                timeslot, request_size = self._request_download_quota(
                        max_rate, state.remaining)
                if request_size == 0:
                    # Nothing left to do in this timeslot
                    break

                # Start the job.
                _debug('Requesting', request_size, 'bytes of', state.name,
                        'at', state.offset)
                byte_range = '%d-%d' % (state.offset,
                        state.offset + request_size - 1)
                response = self._vault.layer1.initiate_job(self._vault.name, {
                    'Type': 'archive-retrieval',
                    'ArchiveId': metadata['aws-aid'],
                    'Description': '%s %s %s' % (set_name, state.name,
                            byte_range),
                    'RetrievalByteRange': byte_range,
                })
                args = (finished_queue, state.name, response.get('JobId'),
                        archive_paths[state.name], state.offset)
                threading.Thread(target=self._finish_download,
                        args=args).start()
                state.requested(request_size)

            # Wait for completions or next timeslot.
            while not state.done:
                max_wait = self._seconds_to_next_timeslot(timeslot)
                try:
                    archive_name, exception = finished_queue.get(
                            timeout=max_wait)
                except Queue.Empty:
                    # We've hit the next timeslot.
                    break

                if exception:
                    if state.response_failed(archive_name):
                        yield (archive_name, exception)
                else:
                    if state.response_processed(archive_name):
                        yield (archive_name, archive_metadata[archive_name])

    def _finish_download(self, queue, archive_name, job_id, local_path,
            offset):
        # Thread function
        try:
            job = self._wait_for_job(job_id)
            fd = os.open(local_path, os.O_WRONLY | os.O_CREAT, 0666)
            with os.fdopen(fd, 'wb') as fh:
                fh.seek(offset)
                # Workaround for:
                # https://github.com/boto/boto/issues/2509
                # https://github.com/boto/boto/issues/2510
                output = job.get_output(validate_checksum=False)
                while True:
                    buf = output.read(self.CHUNK_SIZE)
                    if not buf:
                        break
                    fh.write(buf)
            _debug('Finished request of', archive_name, 'at', offset)
            queue.put((archive_name, None))
        except Exception, e:
            _debug('Failed request of', archive_name, 'at', offset,
                    '-->', e)
            queue.put((archive_name, e))

    def resync(self):
        # Delete archives without associated metadata.

        # Get archive IDs in vault
        job_id = self._vault.retrieve_inventory()
        job = self._wait_for_job(job_id)
        inventory = job.get_output()
        vault_archives = dict((archive['ArchiveId'], archive['Size'])
                for archive in inventory['ArchiveList'])

        # Walk archives in domain
        qstr = 'select `aws-aid` from `%s` where `aws-aid` is not null' % (
                self._domain.name)
        for res in self._domain.select(qstr, consistent_read=True):
            # Ignore archives missing from vault inventory, since the
            # inventory can be up to 24 hours stale.
            vault_archives.pop(res['aws-aid'], None)

        # Delete archives not in domain
        for aid in vault_archives:
            self._vault.delete_archive(aid)
        if vault_archives:
            print 'Deleted %d leaked archives, %d bytes' % (
                    len(vault_archives), sum(vault_archives.values()))

    def report_cost(self):
        msg = '''
The %(total_size)s currently in storage costs $%(storage_cost).2f/month.

You can retrieve around %(free_transfer)s for free today (in UTC), provided that your
retrievals occur at a constant rate during all hours in which you are
retrieving.  If you exceed this amount, your bill will be determined by your
maximum hourly retrieval rate for the month.  Each additional GB/hour of
retrieval bandwidth costs $%(transfer_cost_gb).2f, but is then available for the whole month at
no additional charge.  The maximum retrieval rate this month has been
%(max_transfer_rate)s/hour.
'''.strip()

        total_size = sum(metadata['size']
                for metadata in self.list_sets().values())
        _, bandwidth_item = self._get_bandwidth_item()
        now = self._now()
        days_in_month = calendar.monthrange(now.year, now.month)[1]
        print msg % {
            'total_size': humanize_size(total_size),
            'storage_cost': (total_size / (1 << 30)) * self._storage_cost,
            'free_transfer' : humanize_size(total_size *
                    self.MONTHLY_FREE_RETRIEVAL_FRACTION / days_in_month),
            'transfer_cost_gb': self._storage_cost * days_in_month * 24,
            'max_transfer_rate': humanize_size(int(
                    bandwidth_item['max-bandwidth-month'])),
        }
