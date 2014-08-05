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
from contextlib import contextmanager
from datetime import datetime, timedelta
import dateutil.parser
from dateutil.tz import tzutc
import os
import sys
import time

from ..util import humanize_size
from . import Archiver

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


class AWSArchiver(Archiver):
    LABEL = 'aws'
    UPLOAD_PART_SIZE = 64 << 20
    RETRIEVAL_DELAY = 10
    JOB_CHECK_INTERVAL = 60
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

    @classmethod
    def _make_set_item_name(cls, set_name):
        return set_name + '//'

    @classmethod
    def _make_archive_item_name(cls, set_name, archive_name):
        return '/'.join([set_name, archive_name])

    @classmethod
    def _quote_value(cls, value):
        return value.replace('"', '""')

    def _wait_for_job(self, job):
        print 'Wait 3-5 hours...'
        while True:
            if self._vault.get_job(job.id).completed:
                return
            time.sleep(self.JOB_CHECK_INTERVAL)

    def list_sets(self):
        now = datetime.now(tz=tzutc())
        sets = {}
        qstr = ('select `aws-set`, `aws-complete`, `aws-creation-time`, ' +
                '`size` from `%s`') % self._domain.name
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
        aid = self._vault.concurrent_create_archive_from_file(local_path,
                None, part_size=self.UPLOAD_PART_SIZE)
        try:
            metadata = dict(metadata)
            metadata.update({
                'aws-set': set_name,
                'aws-archive': archive_name,
                'aws-aid': aid,
                'aws-creation-time': datetime.now(tz=tzutc()).isoformat(),
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

    def download_archive(self, set_name, archive_name, fh):
        item_name = self._make_archive_item_name(set_name, archive_name)
        metadata = self._domain.get_item(item_name, consistent_read=True)
        if not metadata:
            return None
        # Give the user a chance to cancel before they are charged.
        size = humanize_size(int(metadata['size']))
        print 'Going to retrieve %s of data.' % size
        for remaining in range(self.RETRIEVAL_DELAY, -1, -1):
            print ('\rStarting in %d seconds. ' % remaining),
            sys.stdout.flush()
            if remaining:
                time.sleep(1)
        print '\nRetrieving.'
        job = self._vault.retrieve_archive(metadata['aws-aid'])
        self._wait_for_job(job)
        job.download_to_fileobj(fh)
        return dict(metadata)

    def resync(self):
        # Delete archives without associated metadata.

        # Get archive IDs in vault
        job = self._vault.retrieve_inventory_job()
        self._wait_for_job(job)
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
