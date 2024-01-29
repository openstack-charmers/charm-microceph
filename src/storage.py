#!/usr/bin/env python3

# Copyright 2024 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Handle Juju Storage Events."""

import os
import json
import logging
import itertools
from subprocess import CalledProcessError, run

from ops.charm import (
    CharmBase,
    EventBase,
    EventSource,
    ObjectEvents,
    StorageAttachedEvent,
    StorageDetachingEvent,
)
from ops.framework import Object, StoredState

import microceph

logger = logging.getLogger(__name__)


class StorageBlockedEvent(EventBase):
    """Storage Event for the charm to observe."""

    # msg used by charm to set the status.
    msg = ""

    # snapshot/restore methods are not being used since
    # event data is not needed to persist deferrals.
    def __init__(self, handle, msg: str):
        super().__init__(handle)
        self.msg = msg


class MicroCephStorageEvents(ObjectEvents):
    """Events related to MicroCluster apps."""

    storage_blocked = EventSource(StorageBlockedEvent)


class StorageHandler(Object):
    """The Storage class manages the Juju storage events.

    Observes the following events:
    1) *_storage_attached
    2) *_storage_detaching
    """

    osd_data_path = '/var/snap/microceph/common/data/osd'

    on = MicroCephStorageEvents()
    charm = None
    _stored = StoredState()

    def __init__(self, charm: CharmBase, name="storage"):
        super().__init__(charm, name)
        self._stored.set_default(disk=set(), wal=set(), db=set())
        self.charm = charm

        # Attach handlers
        self.framework.observe(
            getattr(charm.on, "osd_devices_storage_attached"), self._on_osd_devices_attached
        )
        for key in ["disk", "wal", "db"]:
            self.framework.observe(getattr(charm.on, f"{key}_storage_attached"), self._on_attached)

        # OSD Detaching handlers.
        for key in ["osd_devices", "disk"]:
            self.framework.observe(getattr(charm.on, f"{key}_storage_detaching"), self._on_osd_detaching)

        for key in ["wal", "db"]:
            self.framework.observe(getattr(charm.on, f"{key}_storage_detaching"), self._on_wal_db_detaching)

    """handlers"""
    def _on_attached(self, event: StorageAttachedEvent):
        """Updates the attached storage devices in state."""
        enroll = {
            'disk': [],
            'wal': [],
            'db': [],
        }

        for storage in self.juju_storage_list():
            # split storage names of the form disk/0
            directive = "".join(itertools.takewhile(str.isalpha, storage))
            storage_path = self.juju_storage_get(storage_id=storage, attribute='location')
            if storage_path not in getattr(self._stored, directive):
                enroll[directive].append(storage_path)

        logger.info(enroll)

        # enrolls available disks with WAL/DB.
        self._enroll_with_wal_db(disk=enroll['disk'], wal=enroll['wal'], db=enroll['db'])

    def _on_osd_devices_attached(self, event: StorageAttachedEvent):
        """Event handler for storage attach event."""
        if not microceph._is_ready():
            logger.warning("MicroCeph not ready yet, deferring storage event.")
            event.defer()
            return

        microceph.enroll_disks_as_osds([event.storage.location.as_posix()])
        logger.debug(f"Added {event.storage.location} as disk.")

    def _on_osd_detaching(self, event: StorageDetachingEvent):
        """Event handler for storage detaching event."""
        detaching_disk = event.storage.location.as_posix()
        try:
            microceph.remove_disk(detaching_disk)
        except CalledProcessError as e:
            logger.error(e.stderr)
            err_str = f"Storage {detaching_disk} used as OSD is detaching, provide replacement."
            logger.warning(err_str)
            self._trigger_storage_blocked(error_msg=err_str)
            return

    def _on_wal_db_detaching(self, event: StorageDetachingEvent):
        """Updates the attached storage devices in state."""
        # check wal/db symlinks in all osds to check if device is being used.
        directive = "".join(itertools.takewhile(str.isalpha, event.storage.name))
        device = None
        osd_path = None
        for osd in [path[0] for path in os.walk(self.osd_data_path)]:
            try:
                # osd/block.wal or osd/block.db
                device = os.readlink(f"{osd}/block.{directive}")
                if device == event.storage.location.as_posix():
                    osd_path = osd  # save using osd for reporting.
                    break
            except FileNotFoundError as e:
                # osd has no wal/db configured.
                continue

        if osd_path:
            warn_str = f"{directive} storage {device} used for {osd_path} is detaching."
            logger.warning(warn_str)
            self._trigger_storage_blocked(error_msg=warn_str)
            return

    """helpers"""
    def _run(self, cmd: list) -> str:
        """Wrapper around subprocess run for storage commands."""
        process = run(cmd, capture_output=True, text=True, check=True, timeout=180)
        logger.debug(f"Command {' '.join(cmd)} finished; Output: {process.stdout}")
        return process.stdout

    def _trigger_storage_blocked(self, error_msg: str) -> None:
        """Triggers the storage updated event with provided dict as arguments."""
        self.on.storage_blocked.emit(msg=error_msg)

    def _enroll_with_wal_db(self, disk: list, wal: list, db: list):
        """Checks if sufficient devices are available to be enrolled into OSDs."""
        enrollment_count = min(len(disk), len(wal), len(db))
        for i in range(enrollment_count):
            try:
                microceph.add_osd_cmd(disk[i], wal[i], db[i])
                # store configured devices.
                self._stored.disk.add(disk[i])
                self._stored.wal.add(wal[i])
                self._stored.db.add(db[i])
            except CalledProcessError as e:
                logger.error(e.stderr)

    def juju_storage_get(self, storage_id=None, attribute=None):
        """Get storage attributes"""
        _args = ['storage-get', '--format=json']
        if storage_id:
            _args.extend(('-s', storage_id))
        if attribute:
            _args.append(attribute)
        try:
            return json.loads(self._run(_args))
        except ValueError:
            return None


    def juju_storage_list(self, storage_name=None):
        """List the storage IDs for the unit"""
        _args = ['storage-list', '--format=json']
        if storage_name:
            _args.append(storage_name)
        try:
            return json.loads(self._run(_args))
        except ValueError:
            return None
        except OSError as e:
            import errno
            if e.errno == errno.ENOENT:
                # storage-list does not exist
                return []
            raise

