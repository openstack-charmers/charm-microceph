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
import logging
import subprocess
from socket import gethostname
from typing import List

import charms.operator_libs_linux.v2.snap as snap
import netifaces
import ops.framework
import ops_sunbeam.charm as sunbeam_charm
import ops_sunbeam.guard as sunbeam_guard
import ops_sunbeam.relation_handlers as sunbeam_rhandlers
from ops.main import main

import cluster
import microceph
from ceph_broker import get_named_key
from relation_handlers import (
    CephClientProviderHandler,
    CephRadosGWProviderHandler,
    MicroClusterNewNodeEvent,
    MicroClusterNodeAddedEvent,
    MicroClusterPeerHandler,
    UpgradeNodeDoneEvent,
    UpgradeNodeRequestEvent,
)
from storage import StorageHandler

logger = logging.getLogger(__name__)


class MicroCephCharm(sunbeam_charm.OSBaseOperatorCharm):
    """Charm the service."""

    _state = ops.framework.StoredState()
    service_name = "microceph"
    storage = None  # StorageHandler

    def __init__(self, framework: ops.framework.Framework) -> None:
        """Run constructor."""
        super().__init__(framework)

        # Initialise Modules.
        self.storage = StorageHandler(self)
        self.cluster_nodes = cluster.ClusterNodes(self)
        self.cluster_upgrades = cluster.ClusterUpgrades(self)

        # Initialise handlers for events.
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.stop, self._on_stop)

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

        try:
            snap.SnapCache()["microceph"].hold()
            # Temporary fix while the auto connect request for the interface is accepted.
            snap.SnapCache()["microceph"].connect("mount-observe")
        except Exception:
            logger.exception("Failed to hold microceph refresh: ")

        self.channel = self.model.config.get("snap-channel")

    def _on_stop(self, event: ops.StopEvent):
        """Removes departing unit from the MicroCeph cluster forcefully."""
        try:
            microceph.remove_cluster_member(gethostname(), is_force=True)
        except subprocess.CalledProcessError as e:
            # NOTE: Depending upon the state of the cluster, forcefully removing
            # a host may result in errors even if the request was successful.
            if microceph.is_cluster_member(gethostname()):
                raise e

    def _on_config_changed(self, event: ops.framework.EventBase) -> None:
        self.configure_charm(event)

    @property
    def channel(self) -> str:
        """Get the saved snap channel."""
        c = self.peers.get_app_data("channel")
        if c:
            return c
        # return default channel if not set.
        return self.model.config["snap-channel"]

    @channel.setter
    def channel(self, value: str) -> None:
        if self.unit.is_leader():
            logger.debug(f"Setting channel on peers rel: {value}")
            self.peers.set_app_data({"channel": value})

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
            self.peers.upgrade_callback = self.upgrade_dispatch
            handlers.append(self.peers)
        if self.can_add_handler("ceph", handlers):
            self.ceph = CephClientProviderHandler(
                self,
                "ceph",
                self.handle_ceph,
            )
            handlers.append(self.ceph)
        if self.can_add_handler("radosgw", handlers):
            self.radosgw = CephRadosGWProviderHandler(self, self.handle_ceph)

        handlers = super().get_relation_handlers(handlers)
        logger.debug(f"Relation handlers: {handlers}")
        return handlers

    def ready_for_service(self) -> bool:
        """Check if service is ready or not."""
        # TODO(hemanth): check ceph quorum
        if not snap.SnapCache()["microceph"].present:
            logger.warning("Snap microceph not installed yet.")
            return False

        if not microceph.is_cluster_member(gethostname()):
            logger.warning("Microceph not bootstrapped yet.")
            return False

        # ready for service if leader has been announced.
        return self.is_leader_ready()

    def _lookup_system_interfaces(self, mon_hosts: list) -> str:
        """Looks up available addresses on the machine and returns addr if found in mon_hosts."""
        if not mon_hosts:
            return ""

        for intf in netifaces.interfaces():
            addrs = netifaces.ifaddresses(intf)

            # check ipv4 addresses
            for addr in addrs.get(netifaces.AF_INET, []):
                if addr["addr"] in mon_hosts:
                    return addr["addr"]

            # check ipv6 addresses.
            for addr in addrs.get(netifaces.AF_INET6, []):
                if addr["addr"] in mon_hosts:
                    return addr["addr"]

        # return empty string if none found.
        return ""

    def get_ceph_info_from_configs(self, service_name, caps=None) -> dict:
        """Update ceph info from configuration."""
        # public address should be updated once config public-network is supported
        public_addrs = microceph.get_mon_public_addresses()
        return {
            "auth": "cephx",
            "ceph-public-address": self._lookup_system_interfaces(public_addrs),
            "key": get_named_key(name=service_name, caps=caps),
        }

    def handle_ceph(self, event) -> None:
        """Callback for interface ceph."""
        logger.info("Callback for ceph interface, ignore")

    def upgrade_dispatch(self, event: ops.framework.EventBase) -> None:
        """Dispatch upgrade events."""
        logger.debug(f"Dispatch upgrade: {self.unit.name}, {event}")
        dispatch = {
            UpgradeNodeRequestEvent: self.cluster_upgrades.upgrade_node_request,
            UpgradeNodeDoneEvent: self.cluster_upgrades.upgrade_node_done,
        }
        hdlr = dispatch.get(type(event))
        if not hdlr:
            logger.debug(f"Unhandled event: {event}")
            return
        with sunbeam_guard.guard(self, "Upgrading"):
            hdlr(event)

    def configure_app_leader(self, event: ops.framework.EventBase) -> None:
        """Configure the leader unit."""
        if not self.is_leader_ready():
            self.bootstrap_cluster(event)
            # mark bootstrap node also as joined
            self.peers.interface.state.joined = True
            self.configure_ceph()

        self.set_leader_ready()
        snap_chan = self.model.config.get("snap-channel")

        if self.cluster_upgrades.upgrade_requested(snap_chan):
            ok, msg = self.cluster_upgrades.can_upgrade_charm_payload(snap_chan)
            if not ok:
                raise sunbeam_guard.BlockedExceptionError(msg)
            self.cluster_upgrades.init_upgrade(snap_chan)

        if isinstance(event, MicroClusterNewNodeEvent):
            self.cluster_nodes.add_node_to_cluster(event)

    def configure_app_non_leader(self, event: ops.framework.EventBase) -> None:
        """Configure the non leader unit."""
        super().configure_app_non_leader(event)
        if isinstance(event, MicroClusterNodeAddedEvent):
            self.cluster_nodes.join_node_to_cluster(event)

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

    def configure_ceph(self) -> None:
        """Configure Ceph.

        Set mon_allow_pool_size_one to true
        """
        cmds = [
            ["ceph", "config", "set", "global", "mon_allow_pool_size_one", "true"],
            ["ceph", "config", "set", "global", "osd_pool_default_size", "1"],
        ]
        try:
            for cmd in cmds:
                logger.debug(f'Running command {" ".join(cmd)}')
                process = subprocess.run(
                    cmd, capture_output=True, text=True, check=True, timeout=60
                )
                logger.debug(
                    f"Command finished. stdout={process.stdout}, " f"stderr={process.stderr}"
                )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.warning(e.stderr)
            raise e


if __name__ == "__main__":  # pragma: no cover
    main(MicroCephCharm)
