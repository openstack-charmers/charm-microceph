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

import json
import logging
from subprocess import CalledProcessError, run

import ops_sunbeam.guard as sunbeam_guard
from ops.charm import CharmBase, StorageAttachedEvent, StorageDetachingEvent
from ops.framework import Object, StoredState
from ops.model import ActiveStatus, MaintenanceStatus
from tenacity import retry, stop_after_attempt, wait_fixed

import microceph

logger = logging.getLogger(__name__)


class StorageHandler(Object):
    """The Storage class manages the Juju storage events.

    Observes the following events:
    1) *_storage_attached
    2) *_storage_detaching
    """

    name = "storage"
    osd_data_path = "/var/snap/microceph/common/data/osd"

    charm = None
    """
..        osd_data: dict of dicts with int (osd num) key
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

        # Attach handlers
        self.framework.observe(
            charm.on["osd_devices"].storage_attached, self._on_osd_devices_attached
        )
        for directive in ["disk", "wal", "db"]:
            self.framework.observe(charm.on[directive].storage_attached, self._on_attached)

        # OSD Detaching handlers.
        for directive in ["osd_devices", "disk", "wal", "db"]:
            self.framework.observe(
                charm.on[directive].storage_detaching, self._on_storage_detaching
            )

    # storage event handlers

    def _on_attached(self, event: StorageAttachedEvent):
        """Storage attached handler for disk/wal/db devices."""
        if not self.charm.ready_for_service():
            logger.warning("MicroCeph not ready yet, deferring storage event.")
            event.defer()
            return

        self._clean_stale_osd_data()

        enroll = {
            "disk": [],
            "wal": [],
            "db": [],
        }

        # filter storage for wal/db and disk directives only.
        for storage in self._fetch_filtered_storages(["disk", "wal", "db"]):
            # split storage names of the form disk/n, wal/n or db/n
            directive = storage.split("/")[0]
            storage_path = self.juju_storage_get(storage_id=storage, attribute="location")
            if not self._get_osd_num(storage_path, directive):
                enroll[directive].append(storage_path)

        # enrolls available disks with WAL/DB and save osd data.
        with sunbeam_guard.guard(self.charm, self.name):
            self.charm.status.set(MaintenanceStatus("Enrolling OSDs"))
            self._enroll_with_wal_db(disk=enroll["disk"], wal=enroll["wal"], db=enroll["db"])
            self.charm.status.set(ActiveStatus("charm is ready"))

    def _on_osd_devices_attached(self, event: StorageAttachedEvent):
        """Storage attached handler for osd-devices."""
        if not self.charm.ready_for_service():
            logger.warning("MicroCeph not ready yet, deferring storage event.")
            event.defer()
            return

        self._clean_stale_osd_data()

        enroll = []
        for storage in self._fetch_filtered_storages(["osd-devices"]):
            path = self.juju_storage_get(storage_id=storage, attribute="location")
            if not self._get_osd_num(path, "osd-devices"):
                enroll.append(path)

        with sunbeam_guard.guard(self.charm, self.name):
            self.charm.status.set(MaintenanceStatus("Enrolling OSDs"))
            self._enroll_disks_in_batch(enroll)
            self.charm.status.set(ActiveStatus("charm is ready"))

    def _on_storage_detaching(self, event: StorageDetachingEvent):
        """Unified storage detaching handler."""
        # check if the detaching device is being used as or with an OSD.
        osd_num = self._get_osd_num(event.storage.location.as_posix(), event.storage.name)

        if osd_num:
            with sunbeam_guard.guard(self.charm, self.name):
                try:
                    self.remove_osd(osd_num)
                except CalledProcessError as e:
                    if self._is_safety_failure(e.stderr):
                        warning = f"Storage {event.storage.full_id} detached, provide replacement for osd.{osd_num}."
                        logger.warning(warning)
                        # clean records since juju will deprovision device.
                        self.remove_osd(osd_num, force=True)
                        raise sunbeam_guard.BlockedExceptionError(warning)

    # helper functions

    def _fetch_filtered_storages(self, directives: list) -> list:
        """Provides a filtered list of attached storage devices."""
        filtered = []
        for device in self.juju_storage_list():
            if device.split("/")[0] in directives:
                filtered.append(device)

        return filtered

    def _is_safety_failure(self, err: str) -> bool:
        """Checks if the subprocess error is caused by safety check."""
        return "need at least 3 OSDs" in err

    def _run(self, cmd: list) -> str:
        """Wrapper around subprocess run for storage commands."""
        process = run(cmd, capture_output=True, text=True, check=True, timeout=180)
        logger.debug(f"Command {' '.join(cmd)} finished; Output: {process.stdout}")
        return process.stdout

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

    def _enroll_disks_in_batch(self, disks: list):
        """Adds requested Disks to Microceph and stored state."""
        microceph.enroll_disks_as_osds(disks)
        for disk in disks:
            self._save_osd_data(disk)
        logger.debug(f"Added {disks} as OSDs.")

    def remove_osd(self, osd: int, force: bool = False):
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
            # get block device info using /dev/disk-by-id and lsblk.
            local_device = microceph._get_disk_info(osd["path"])

            # OSD not configured on current unit.
            if not local_device:
                continue

            # e.g. check 'vdd' in '/dev/vdd'
            if local_device["name"] in disk:
                self._stored.osd_data[osd["osd"]] = {
                    "disk_by_id": osd["path"],
                    "disk": disk,
                    "wal": wal,
                    "db": db,
                }

    def _get_osd_num(self, disk, directive):
        """Fetch the OSD number of consuming OSD, None is not used as OSD."""
        # both osd-devices and disks are used as OSD disks.
        if directive == "osd-devices":
            directive = "disk"

        logger.debug(self._stored.osd_data)
        logger.debug(f"Incoming Disk {disk}, directive {directive}.")

        for k, v in dict(self._stored.osd_data).items():
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
                logger.debug(f"Popped {val}")

    def _remove_osd_data(self, disk: str):
        """Remove data for removed OSD."""
        num = -1  # impossible osd number.
        for osd_num, data in dict(self._stored.osd_data).items():
            if data and data["disk"] == disk:
                num = osd_num
        if num > 0:
            val = self._stored.osd_data[num]
            self._stored.osd_data[num] = None
            logger.debug(f"Popped {val}")

        logger.debug(self._stored.osd_data)

    # NOTE(utkarshbhatthere): 'storage-get' sometimes fires before
    # requested information is available.
    @retry(wait=wait_fixed(5), stop=stop_after_attempt(10))
    def juju_storage_get(self, storage_id=None, attribute=None):
        """Get storage attributes."""
        _args = ["storage-get", "--format=json"]
        if storage_id:
            _args.extend(("-s", storage_id))
        if attribute:
            _args.append(attribute)
        try:
            return json.loads(self._run(_args))
        except ValueError:
            return None

    def juju_storage_list(self, storage_name=None):
        """List the storage IDs for the unit."""
        _args = ["storage-list", "--format=json"]
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
