#
# Deltaic - an efficient backup system supporting multiple data sources
#
# Copyright (c) 2015-2021 Carnegie Mellon University
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

import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

import click
import httplib2
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from oauth2client import client
from oauth2client.file import Storage

from ..command import pass_config
from ..util import humanize_size
from . import Archiver

GOOGLE_DRIVE_PRICING = [
    # total storage, monthly rate
    (15 << 30, 0.0),
    (100 << 30, 1.99),
    (1 << 40, 9.99),
    (10 << 40, 99.99),
    (20 << 40, 199.99),
    (30 << 40, 299.99),
    (0, 999.99),
]


OAUTH_SCOPE = (
    "https://www.googleapis.com/auth/drive.appfolder",
    "https://www.googleapis.com/auth/drive.file",
)
REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"
DEFAULT_CREDENTIALS_FILE = "deltaic-googledrive-credentials.json"


def foreverholdyourpeace(message, delay=10, file=sys.stdout):
    for remaining in range(delay, -1, -1):
        print(f"\r{message} in {remaining} seconds. ", end="", file=file, flush=True)
        if remaining:
            time.sleep(1)
    print(f"\r{message}.                 ", file=file)


def _default_drive_credentials_path(settings):
    cache_dir = os.environ.get("XDG_CACHE_HOME")
    if cache_dir is None:
        cache_dir = os.path.join(os.environ["HOME"], ".cache")
    default_path = os.path.join(cache_dir, DEFAULT_CREDENTIALS_FILE)
    return settings.get("googledrive-credentials-file", default_path)


def _get_drive_service(settings):
    credentials_file = _default_drive_credentials_path(settings)
    storage = Storage(credentials_file)
    credentials = storage.get()
    auth_http = credentials.authorize(httplib2.Http())
    return build("drive", "v2", http=auth_http)


@click.group()
def googledrive():
    """low-level Google Drive support"""
    pass


@googledrive.command()
@pass_config
def auth(config):
    """obtain authorization token"""
    settings = config["settings"]

    client_id = settings.get("googledrive-client-id")
    client_secret = settings.get("googledrive-client-secret")

    if not client_id or not client_secret:
        print("Missing client id or secret, check setup instructions")
        return -1

    flow = client.OAuth2WebServerFlow(
        client_id, client_secret, scope=" ".join(OAUTH_SCOPE), redirect_uri=REDIRECT_URI
    )
    flow.user_agent = "Deltaic"

    authorize_url = flow.step1_get_authorize_url()
    print("Go to the following link in your browser:", authorize_url)
    code = input("Enter verification code: ").strip()
    credentials = flow.step2_exchange(code)

    # write credentials to file
    credentials_file = _default_drive_credentials_path(settings)
    foreverholdyourpeace(f"writing credentials to {credentials_file}", file=sys.stderr)

    os.makedirs(os.path.dirname(credentials_file))
    storage = Storage(credentials_file)
    storage.put(credentials)


@googledrive.command()
@pass_config
def test(config):
    """test Google Drive connection"""
    settings = config["settings"]
    service = _get_drive_service(settings)

    # get metadata for appfolder
    result = service.files().get(fileId="appfolder").execute()
    print(json.dumps(result, indent=1))


@googledrive.command()
@pass_config
def ls(config):
    """(debug) dump metadata for all files we have access to"""
    settings = config["settings"]
    service = _get_drive_service(settings)

    # list all files that we have access to
    result = service.files().list().execute()
    print(json.dumps(result["items"], indent=1))


@googledrive.command()
@click.argument("fileid")
@pass_config
def rm(config, fileid):
    """(debug) remove drive object by fileid"""
    settings = config["settings"]
    service = _get_drive_service(settings)

    result = service.files().get(fileId=fileid).execute()
    foreverholdyourpeace(f"Deleting {result['title']} ({fileid})")
    service.files().delete(fileId=fileid).execute()


class _PartialFile:
    def __init__(self, f, start, length):
        self._file = f

        f.seek(0, os.SEEK_END)
        self._size = f.tell()
        self._length = min(length, self._size - start)

        f.seek(start, os.SEEK_SET)
        self._start = start
        self._offset = 0

    def read(self, n=-1):
        if n == -1:
            n = self._length - self._offset
        if n < 0:
            n = 0
        buf = self._file.read(n)
        self._offset += len(buf)
        return buf

    def tell(self):
        return self._offset

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_SET:
            self._offset = offset
        elif whence == os.SEEK_CUR:
            self._offset += offset
        elif whence == os.SEEK_END:
            self._offset = self._length + offset

        if self._offset < 0:
            self._offset = 0
        if self._offset > self._length:
            self._offset = self._length
        self._file.seek(self._start + self._offset)


class DriveArchiver(Archiver):
    LABEL = "googledrive"
    MAX_FILESIZE = 5120 << 30  # Drive has 5TB file size limit
    MAX_FILESIZE = 2048 << 30  # safer to avoid possible off-by-one errors
    UPLOAD_CHUNKSIZE = 4 << 30  # GAE has 5MB limit on request size
    DOWNLOAD_CHUNKSIZE = 4 << 30  # Response content is stored in memory

    def __init__(self, profile_name, profile):
        Archiver.__init__(self, profile_name, profile)
        self._service = _get_drive_service(profile)

    def _list_folder(self, folder, q=None):
        """Get list of all files stored in a folder"""
        query = [f"'{folder}' in parents"]
        if q is not None:
            query.extend(q)
        param = {"q": " and ".join(query)}

        while True:
            result = self._service.files().list(**param).execute()

            yield from result["items"]

            page_token = result.get("nextPageToken")
            if not page_token:
                break
            param["pageToken"] = page_token

    def _find_set_id(self, set_name):
        results = self._list_folder(
            "appfolder",
            q=[
                "mimeType = 'application/vnd.google-apps.folder'",
                f"title = '{set_name}'",
            ],
        )
        try:
            # warn if we get more than one result?
            return results.next()["id"]
        except StopIteration:
            return None

    def _file_attrs(self, file_id):
        return self._service.files().get(fileId=file_id).execute()

    def list_sets(self):
        sets = {}
        results = self._list_folder(
            "appfolder", q=["mimeType = 'application/vnd.google-apps.folder'"]
        )
        for archiveset in list(results):
            set_name = archiveset["title"]
            set_id = archiveset["id"]

            properties = {
                p["key"]: p["value"] for p in archiveset.get("properties", [])
            }
            archive_sizes = [
                int(item["fileSize"]) for item in self._list_folder(set_id)
            ]

            sets[set_name] = {
                "count": len(archive_sizes),
                "size": sum(archive_sizes),
                "complete": properties.get("complete") == "true",
                "protected": False,
            }
        return sets

    def complete_set(self, set_name):
        set_id = self._find_set_id(set_name)
        if not set_id:
            return

        self._service.properties().insert(
            fileId=set_id,
            body={
                "key": "complete",
                "value": "true",
                "visibility": "PRIVATE",
            },
        ).execute()

    def delete_set(self, set_name):
        set_id = self._find_set_id(set_name)
        if not set_id:
            return

        self._service.properties().delete(
            fileId=set_id, propertyKey="complete", visibility="PRIVATE"
        ).execute()
        self._service.files().delete(fileId=set_id).execute()

    def list_set_archives(self, set_name):
        set_id = self._find_set_id(set_name)
        if not set_id:
            # create_set_if_not_exists
            body = {
                "title": set_name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [{"id": "appfolder"}],
            }
            folder = self._service.files().insert(body=body).execute()
            set_id = folder["id"]
            return {}  # we know it is empty because we just created it

        archives = {}
        for archive in self._list_folder(set_id):
            properties = {p["key"]: p["value"] for p in archive.get("properties", [])}
            if properties.get("googledrive-part", "1") != "1":
                continue

            # size property is the size of a reassembled multipart object
            size = properties.get("size", archive["fileSize"])
            parts = properties.get("googledrive-parts", "1")

            name = archive["title"]
            archives[name] = {
                "size": size,
                "googledrive-set": set_name,
                "googledrive-archive": name,
                "googledrive-aid": archive["id"],
                "googledrive-creation-time": archive["createdDate"],
                "googledrive-parts": parts,
            }
        return archives

    def upload_archive(self, set_name, archive_name, metadata, local_path):
        set_id = self._find_set_id(set_name)
        if not set_id:
            raise ValueError(f"Set folder for '{set_name}' missing")

        size = os.path.getsize(local_path)
        parts = ((size - 1) // self.MAX_FILESIZE) + 1 or 1

        with open(local_path, "rb") as source_file:
            props = dict(metadata)
            props["googledrive-parts"] = str(parts)

            for part in range(parts):
                props["googledrive-part"] = str(part + 1)

                offset = part * self.MAX_FILESIZE
                file_part = _PartialFile(source_file, offset, self.MAX_FILESIZE)

                # create archive file
                mimetype = "application/octet-stream"
                body = {
                    "title": archive_name,
                    "mimeType": mimetype,
                    "parents": [{"id": set_id}],
                    "properties": [
                        {"key": key, "value": value, "visibility": "PRIVATE"}
                        for key, value in props.items()
                    ],
                }
                data = MediaIoBaseUpload(
                    file_part,
                    mimetype=mimetype,
                    chunksize=self.UPLOAD_CHUNKSIZE,
                    resumable=True,
                )
                self._service.files().insert(body=body, media_body=data).execute()

    def _download_archive(self, archive, path):
        with open(path, "wb") as archive_file:
            M = len(archive)
            for N, part in enumerate(archive, 1):
                part_id = part["id"]
                request = self._service.files().get_media(fileId=part_id)
                media_req = MediaIoBaseDownload(
                    archive_file, request, chunksize=self.DOWNLOAD_CHUNKSIZE
                )

                try:
                    while True:
                        progress, done = media_req.next_chunk(num_retries=5)
                        if progress:
                            pct = int(progress.progress() * 100)
                            print(
                                "\r(%d/%d) progress: %d%%   "
                                % (
                                    N,
                                    M,
                                    pct,
                                ),
                                end="",
                                file=sys.stderr,
                            )
                        if done:
                            break
                finally:
                    print("", file=sys.stderr)

    def download_archives(self, set_name, archive_list, max_rate=None):
        set_id = self._find_set_id(set_name)
        if not set_id:
            return

        idmap: Dict[str, List[Optional[Dict[str, Any]]]] = {}
        for archive_part in self._list_folder(set_id):
            properties = {
                p["key"]: p["value"] for p in archive_part.get("properties", [])
            }
            N = int(properties.get("googledrive-part", "1"))
            M = int(properties.get("googledrive-parts", "1"))

            archive = idmap.setdefault(
                archive_part["title"],
                [
                    None,
                ]
                * M,
            )
            # part numbers are 1-indexed, python arrays are 0-indexed
            archive[N - 1] = archive_part

        archives = []
        for archive_name, archive_path in archive_list:
            try:
                archive = idmap[archive_name]
                if None not in archive:
                    archives.append((archive_name, archive_path, archive))
                else:
                    yield (archive_name, ValueError("Missing one or more parts"))
            except KeyError:
                yield (archive_name, ValueError("No such archive"))
        if not archives:
            return

        total_size = sum(
            int(part["fileSize"])
            for _, _, archive in archives
            for part in archive
            if part is not None
        )
        foreverholdyourpeace(
            f"Going to retrieve {humanize_size(total_size)} of data", file=sys.stderr
        )

        for archive_name, archive_path, archive in archives:
            archive_size = sum(
                int(part["fileSize"]) for part in archive if part is not None
            )
            print(
                f"Retrieving {archive_name} "
                f"({archive_size} bytes/{len(archive)} part(s))"
            )

            # pull archive metadata from first part
            assert archive[0] is not None
            archive_metadata = {
                prop["key"]: prop["value"] for prop in archive[0]["properties"]
            }

            try:
                self._download_archive(archive, archive_path)
            except HttpError as exception:
                os.unlink(archive_path)
                yield (archive_name, exception)
                continue

            if archive_size != os.path.getsize(archive_path):
                os.unlink(archive_path)
                yield (archive_name, ValueError("Size mismatch"))
            else:
                yield (archive_name, archive_metadata)

    def resync(self):
        return

    def report_cost(self):
        msg = """
The %(total_size)s currently in storage requires the %(storage_size)s tier
which costs $%(storage_cost).2f/month.
""".strip()

        total_size = sum(metadata["size"] for metadata in self.list_sets().values())
        for tier, cost in GOOGLE_DRIVE_PRICING:
            if total_size <= tier:
                break

            print(
                msg
                % {
                    "total_size": humanize_size(total_size),
                    "storage_size": humanize_size(tier),
                    "storage_cost": cost,
                }
            )
