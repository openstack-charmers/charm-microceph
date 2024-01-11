#!/usr/bin/env python3

# Copyright 2023 Canonical Ltd.
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


"""Microceph Operator Charm.

This charm deploys and manages microceph.
"""
import json
import logging
import re
import subprocess
from socket import gethostname
from typing import List

import ops.framework
import ops_sunbeam.charm as sunbeam_charm
import ops_sunbeam.relation_handlers as sunbeam_rhandlers
from netifaces import AF_INET, gateways, ifaddresses
from ops.charm import ActionEvent
from ops.main import main

from ceph_broker import get_named_key
from relation_handlers import (
    CephClientProviderHandler,
    MicroClusterNewNodeEvent,
    MicroClusterNodeAddedEvent,
    MicroClusterPeerHandler,
    MicroClusterRemoveNodeEvent,
)

logger = logging.getLogger(__name__)


def _get_local_ip_by_default_route() -> str:
    """Get IP address of host associated with default gateway."""
    interface = "lo"
    ip = "127.0.0.1"

    # TOCHK: Gathering only IPv4
    if "default" in gateways():
        interface = gateways()["default"][AF_INET][1]

    ip_list = ifaddresses(interface)[AF_INET]
    if len(ip_list) > 0 and "addr" in ip_list[0]:
        ip = ip_list[0]["addr"]

    return ip


class MicroCephCharm(sunbeam_charm.OSBaseOperatorCharm):
    """Charm the service."""

    _state = ops.framework.StoredState()
    service_name = "microceph"

    def __init__(self, framework: ops.framework.Framework) -> None:
        """Run constructor."""
        super().__init__(framework)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.list_disks_action, self._list_disks_action)
        self.framework.observe(self.on.add_osd_action, self._add_osd_action)

    def _on_install(self, event: ops.framework.EventBase) -> None:
        config = self.model.config.get
        cmd = [
            "snap",
            "install",
            "microceph",
            "--channel",
            config("snap-channel"),
        ]

        logger.debug(f'Running command {" ".join(cmd)}')
        process = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=180)
        logger.debug(f"Command finished. stdout={process.stdout}, " f"stderr={process.stderr}")

        cmd = ["sudo", "snap", "alias", "microceph.ceph", "ceph"]
        logger.debug(f'Running command {" ".join(cmd)}')
        process = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=180)
        logger.debug(f"Command finished. stdout={process.stdout}, " f"stderr={process.stderr}")

    def _on_config_changed(self, event: ops.framework.EventBase) -> None:
        self.configure_charm(event)

    def _list_disks_action(self, event: ActionEvent):
        """Run list-disks action."""
        if not self.peers.interface.state.joined:
            event.fail("Node not yet joined in microceph cluster")
            return

        # TOCHK: Replace microceph commands with microceph daemon API calls
        cmd = ["sudo", "microceph", "disk", "list"]
        try:
            logger.debug(f'Running command {" ".join(cmd)}')
            process = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=180)
            logger.debug(f"Command finished. stdout={process.stdout}, " f"stderr={process.stderr}")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.warning(e.stderr)
            event.fail(e.stderr)
            return

        disks = self._handle_disk_list_output(process.stdout)
        print(disks)
        event.set_results(disks)

    def _add_osd_action(self, event: ActionEvent):
        """Add OSD disk to microceph."""
        if not self.peers.interface.state.joined:
            event.fail("Node not yet joined in microceph cluster")
            return

        device_ids = event.params.get("device-id").split(",")
        for device_id in device_ids:
            cmd = ["sudo", "microceph", "disk", "add", device_id]
            try:
                logger.debug(f'Running command {" ".join(cmd)}')
                process = subprocess.run(
                    cmd, capture_output=True, text=True, check=True, timeout=180
                )
                logger.debug(
                    f"Command finished. stdout={process.stdout}, " f"stderr={process.stderr}"
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
                logger.warning(e.stderr)
                error_disk_already_exists = (
                    "Failed adding new disk: Failed to record disk: Failed to create "
                    '"disks" entry: UNIQUE constraint failed: disks.member_id, disks.path'
                )
                if error_disk_already_exists not in e.stderr:
                    event.fail(e.stderr)
                    return

        event.set_results({"status": "success"})

    def _handle_disk_list_output(self, output: str) -> dict:
        # Do not use _ for keys that need to set in action result, instead use -.
        disks = {"osds": [], "unpartitioned-disks": []}

        # Used in each matched regex: \w, space, -, backslash
        osds_re = r"\n\|([\w -]+)\|([\w -]+)\|([\w -\/]+)\|\n"
        osds = re.findall(osds_re, output)

        # Used in each matched regex: \w, space, -, backslash, dot
        unpartitioned_disks_re = r"\n\|([\w -]+)\|([\w -\.]+)\|([\w -]+)\|([\w -\/]+)\|\n"
        unpartitioned_disks = re.findall(unpartitioned_disks_re, output)

        for osd in osds:
            # Skip the header
            if "OSD" in osd[0] and "LOCATION" in osd[1] and "PATH" in osd[2]:
                continue

            disks["osds"].append(
                {"osd": osd[0].strip(), "location": osd[1].strip(), "path": osd[2].strip()}
            )

        for disk in unpartitioned_disks:
            # Skip the header
            if (
                "MODEL" in disk[0]
                and "CAPACITY" in disk[1]
                and "TYPE" in disk[2]
                and "PATH" in disk[3]
            ):
                continue

            # keys are in sync with what is returned by microceph daemon API call /1.0/resources
            disks["unpartitioned-disks"].append(
                {
                    "model": disk[0].strip(),
                    "size": disk[1].strip(),
                    "type": disk[2].strip(),
                    "path": disk[3].strip(),
                }
            )

        return disks

    def get_relation_handlers(self, handlers=None) -> List[sunbeam_rhandlers.RelationHandler]:
        """Relation handlers for the service."""
        handlers = handlers or []
        if self.can_add_handler("peers", handlers):
            self.peers = MicroClusterPeerHandler(
                self,
                "peers",
                self.configure_charm,
                "peers" in self.mandatory_relations,
            )
            handlers.append(self.peers)
        if self.can_add_handler("ceph", handlers):
            self.ceph = CephClientProviderHandler(
                self,
                "ceph",
                self.handle_ceph,
            )
            handlers.append(self.ceph)

        handlers = super().get_relation_handlers(handlers)
        return handlers

    def ready_for_service(self) -> bool:
        """Check if service is ready or not."""
        # TODO(hemanth): check ceph quorum
        if self.bootstrapped:
            return True

        return False

    def get_ceph_info_from_configs(self, service_name) -> dict:
        """Update ceph info from configuration."""
        # public address should be updated once config public-network is supported
        public_addr = _get_local_ip_by_default_route()
        return {
            "auth": "cephx",
            "ceph-public-address": public_addr,
            "key": get_named_key(service_name),
        }

    def handle_ceph(self, event) -> None:
        """Callback for interface ceph."""
        logger.info("Callback for ceph interface, ignore")

    def configure_app_leader(self, event: ops.framework.EventBase) -> None:
        """Configure the leader unit."""
        if not self.is_leader_ready():
            self.bootstrap_cluster(event)
            # mark bootstrap node also as joined
            self.peers.interface.state.joined = True

        self.set_leader_ready()

        if isinstance(event, MicroClusterNewNodeEvent):
            self.add_node_to_cluster(event)
        elif isinstance(event, MicroClusterRemoveNodeEvent):
            self.remove_node_from_cluster(event)

    def configure_app_non_leader(self, event: ops.framework.EventBase) -> None:
        """Configure the non leader unit."""
        super().configure_app_non_leader(event)
        if isinstance(event, MicroClusterNodeAddedEvent):
            self.join_node_to_cluster(event)

    def bootstrap_cluster(self, event: ops.framework.EventBase) -> None:
        """Bootstrap microceph cluster."""
        cmd = ["sudo", "microceph", "cluster", "bootstrap"]
        try:
            logger.debug(f'Running command {" ".join(cmd)}')
            process = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=180)
            logger.debug(f"Command finished. stdout={process.stdout}, " f"stderr={process.stderr}")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.warning(e.stderr)
            hostname = gethostname()
            error_already_exists = f'Failed to initialize local remote entry: A remote with name "{hostname}" already exists'
            error_socket_not_exists = "dial unix /var/snap/microceph/common/state/control.socket: connect: no such file or directory"

            if error_socket_not_exists in e.stderr:
                event.defer()
                return

            if error_already_exists not in e.stderr:
                raise e

    def add_node_to_cluster(self, event: ops.framework.EventBase) -> None:
        """Add node to microceph cluster."""
        cmd = ["sudo", "microceph", "cluster", "add", event.unit.name]
        try:
            logger.debug(f'Running command {" ".join(cmd)}')
            process = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=180)
            logger.debug(f"Command finished. stdout={process.stdout}, " f"stderr={process.stderr}")
            token = process.stdout.strip()
            self.peers.set_app_data({f"{event.unit.name}.join_token": token})
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.warning(e.stderr)
            error_node_already_exists = (
                'Failed to create "internal_token_records" entry: UNIQUE '
                "constraint failed: internal_token_records.name"
            )
            if error_node_already_exists not in e.stderr:
                raise e

    def join_node_to_cluster(self, event: ops.framework.EventBase) -> None:
        """Join node to microceph cluster."""
        token = self.peers.get_app_data(f"{event.unit.name}.join_token")
        cmd = ["sudo", "microceph", "cluster", "join", token]
        try:
            logger.debug(f'Running command {" ".join(cmd)}')
            process = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=180)
            logger.debug(f"Command finished. stdout={process.stdout}, " f"stderr={process.stderr}")
            self.peers.interface.state.joined = True
            self.peers.set_unit_data({"joined": json.dumps(True)})
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.warning(e.stderr)
            raise e

    def remove_node_from_cluster(self, event: ops.framework.EventBase) -> None:
        """Remove node from microceph cluster."""
        cmd = ["sudo", "microceph", "cluster", "remove", event.unit.name]
        try:
            logger.debug(f'Running command {" ".join(cmd)}')
            process = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=180)
            logger.debug(f"Command finished. stdout={process.stdout}, " f"stderr={process.stderr}")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.warning(e.stderr)
            # Bugs to be fixed in microceph
            # https://github.com/canonical/microceph/issues/125
            # https://github.com/canonical/microceph/issues/49
            error_remove_node = 'Delete "internal_cluster_members": FOREIGN KEY constraint failed'
            error_no_node_exists = "No remote exists with the given name"
            if error_remove_node not in e.stderr and error_no_node_exists not in e.stderr:
                raise e


if __name__ == "__main__":  # pragma: no cover
    main(MicroCephCharm)
