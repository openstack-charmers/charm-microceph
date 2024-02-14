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
from tenacity import retry, stop_after_attempt, wait_fixed

from ops_sunbeam.guard import guard, BlockedExceptionError
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

    name = "storage"
    osd_data_path = "/var/snap/microceph/common/data/osd"

    on = MicroCephStorageEvents()
    charm = None
    """
        osd_data: type dict"
            disk_by_id: OSD disk by id
            disk: OSD disk path
            wal: wal disk path
            db: db disk path
    """
    _stored = StoredState()

    def __init__(self, charm: CharmBase, name="storage"):
        super().__init__(charm, name)
        self._stored.set_default(osd_data={})
        self.charm = charm
        self.name = name

        logger.info("TEST: INITIALISED STORAGE INTERFACE")

        # Attach handlers
        self.framework.observe(
            getattr(charm.on, "osd_devices_storage_attached"), self._on_osd_devices_attached
        )
        for key in ["disk", "wal", "db"]:
            self.framework.observe(getattr(charm.on, f"{key}_storage_attached"), self._on_attached)

        # OSD Detaching handlers.
        for key in ["osd_devices", "disk"]:
            self.framework.observe(getattr(charm.on, f"{key}_storage_detaching"), self._on_storage_detaching)

        for key in ["wal", "db"]:
            self.framework.observe(getattr(charm.on, f"{key}_storage_detaching"), self._on_storage_detaching)

    """handlers"""
    def _on_attached(self, event: StorageAttachedEvent):
        """Updates the attached storage devices in state."""
        if not microceph._is_ready():
            logger.warning("MicroCeph not ready yet, deferring storage event.")
            event.defer()
            return

        self._clean_stale_osd_data()

        enroll = {
            'disk': [],
            'wal': [],
            'db': [],
        }

        logger.info(enroll)
        # filter only storage directives for wal/db and disk.
        accepts = {"disk", "wal", "db"}
        for storage in [x for x in self.juju_storage_list() if any(x.find(id) >= 0 for id in accepts)]:
            # split storage names of the form disk/0
            directive = "".join(itertools.takewhile(str.isalpha, storage))
            storage_path = self.juju_storage_get(storage_id=storage, attribute='location')
            if not self._get_osd_num(storage_path, directive):
                enroll[directive].append(storage_path)

        logger.info(enroll)

        # enrolls available disks with WAL/DB and save osd data.
        with guard(self.charm, self.name):
            self._enroll_with_wal_db(
                disk=enroll['disk'], wal=enroll['wal'], db=enroll['db']
            )

    def _on_osd_devices_attached(self, event: StorageAttachedEvent):
        """Event handler for storage attach event."""
        if not microceph._is_ready():
            logger.warning("MicroCeph not ready yet, deferring storage event.")
            event.defer()
            return

        self._clean_stale_osd_data()

        enroll = []
        for storage in [device for device in self.juju_storage_list() if 'osd-devices' in device]:
            path = self.juju_storage_get(storage_id=storage, attribute='location')
            if not self._get_osd_num(path, 'osd-devices'):
                enroll.append(path)

        with guard(self.charm, self.name):
            microceph.enroll_disks_as_osds(enroll)
        for device in enroll:
            self._save_osd_data(device)

        logger.info(f"Added {enroll} as disk.")

    # def _on_osd_detaching(self, event: StorageDetachingEvent):
    #     """Event handler for storage detaching event."""
    #     detaching_disk = event.storage.location.as_posix()
    #     with guard(self.charm, self.name):
    #         try:
    #             # try removing the disk
    #             microceph.remove_disk(detaching_disk)
    #             self._remove_osd_data(detaching_disk)
    #         except CalledProcessError as e:
    #             # TODO: Clean records of disk from microcluster db.
    #             osd_num = self._get_osd_num(event.storage.location.as_posix(), event.storage.name)
    #             err_str = f"Storage {event.storage.full_id} detached, provide replacement for osd.{osd_num}."
    #             logger.warning(err_str)
    #             raise BlockedExceptionError("Storage device lost, data loss is possible.")

    def _on_storage_detaching(self, event: StorageDetachingEvent):
        """Updates the attached storage devices in state."""
        # check if the wal/db device is being used.
        osd_num = self._get_osd_num(event.storage.location.as_posix(), event.storage.name)

        # If detaching wal/db disk is used with an OSD.
        if osd_num:
            with guard(self.charm, self.name):
                try:
                    self.remove_osd(osd_num)
                except CalledProcessError as e:
                    if self._is_safety_failure(e.stderr):
                        warning = f"Storage {event.storage.full_id} detached, provide replacement for osd.{osd_num}."
                        logger.warning(warning)
                        # clean records from microceph db, since juju will deprovision storage device.
                        self.remove_osd(osd_num, force=True)
                        raise BlockedExceptionError(warning)

    """helpers"""
    def _is_safety_failure(err: str) -> bool:
        """checks if the subprocess error is caused by safety check."""
        return "need at least 3 OSDs" in err

    def _check_ceph_osds(self, path: str, directive: str = None) -> str:
        """Returns osd num if certain disk is being used for OSDs or wal/db directive."""
        device = None
        osd_path = None
        for osd in [path[0] for path in os.walk(self.osd_data_path)]:
            try:
                # osd/block.wal or osd/block.db
                osd_path = f"{osd}/block"
                if directive:
                    osd_path += f".{directive}"
                device = os.readlink(osd_path)
                if device == path:
                    return osd.split('-')[1]
            except FileNotFoundError:
                # osd has no wal/db configured.
                continue

        # impossible osd number
        return -1

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
                self._save_osd_data(disk=disk[i], wal=wal[i], db=db[i])
            except CalledProcessError as e:
                logger.error(e.stderr)

    def remove_osd(self, osd: int, force: bool):
        """Removes OSD from MicroCeph and from stored state."""
        try:
            microceph.remove_disk_cmd(osd, force)
            self._remove_osd_data(self._stored.osd_data[osd])
        except CalledProcessError as e:
            if force:
                # clean stored state.
                self._remove_osd_data(self._stored.osd_data[osd])
            raise e

    def _save_osd_data(self, disk: str, wal: str = None, db: str = None):
        """Save OSD data."""
        for osd in microceph.list_disk_cmd()["ConfiguredDisks"]:
            # e.g. check 'vdd' in '/dev/vdd'
            if str(microceph._get_disk_info(osd["path"], "name")) in disk:
                self._stored.osd_data[osd["osd"]] = {
                    "disk_by_id": osd["path"],
                    "disk": disk,
                    "wal": wal,
                    "db": db,
                }

    def _get_osd_num(self, disk, directive):
        """Fetch the OSD number of consuming OSD, None is not used as OSD."""
        # both osd-devices and disks are used as OSD disks.
        if directive in ["osd-devices", "disk"]:
            directive = "disk"

        logger.info(self._stored.osd_data)
        logger.info(f"Incoming Disk {disk}, directive {directive}.")

        for k,v in dict(self._stored.osd_data).items():
            if v and v[directive] == disk:
                return k  # key is the stored osd number.
        return None

    def _clean_stale_osd_data(self):
        """Compare with disk list and remove stale entries."""
        osds = [osd["osd"] for osd in microceph.list_disk_cmd()["ConfiguredDisks"]]

        for key in dict(self._stored.osd_data).keys():
            if key not in osds:
                val = self._stored.osd_data[key]
                self._stored.osd_data[key] = None
                logger.info(f"Popped {val}")

    def _remove_osd_data(self, disk: str):
        """Remove data for removed OSD."""
        num = -1  # impossible osd number.
        for osd_num, data in dict(self._stored.osd_data).items():
            if data and data['disk'] == disk:
                num = osd_num
        if num > 0:
            val = self._stored.osd_data[num]
            self._stored.osd_data[num] = None
            logger.info(f"Popped {val}")

        logger.info(self._stored.osd_data)

    # NOTE(utkarshbhatthere): 'storage-get' sometimes fires before
    # requested information is available.
    @retry(wait=wait_fixed(5), stop=stop_after_attempt(10))
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

