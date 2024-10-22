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


"""The cluster module manages cluster-wide operations."""

import json
import logging
import subprocess
import uuid
from typing import Tuple

import ops.charm
import ops_sunbeam.guard as sunbeam_guard
import tenacity
from charms.operator_libs_linux.v2 import snap

import charm
import microceph
import relation_handlers
from microceph import CephHealth, CephStatus

logger = logging.getLogger(__name__)


class ClusterNodes(ops.framework.Object):
    """ClusterNodes manages adding and joining nodes to the microceph cluster."""

    def __init__(self, charm: "charm.MicroCephCharm"):
        super().__init__(charm, "cluster-nodes")
        self.charm = charm

    def add_node_to_cluster(self, event: ops.framework.EventBase) -> None:
        """Add node to microceph cluster."""
        if not event.unit:
            return
        # get hostname using unit name.
        hostnames = self.charm.peers.get_all_unit_values(
            key=event.unit.name, include_local_unit=True
        )
        if not hostnames:
            event.defer()

        cmd = ["microceph", "cluster", "add", hostnames[0]]
        try:
            out = microceph._run_cmd(cmd)
            token = out.strip()
            self.charm.peers.set_app_data({f"{event.unit.name}.join_token": token})
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
        if not event.unit:
            return

        token = self.charm.peers.get_app_data(f"{event.unit.name}.join_token")
        if not token:
            logger.info("Token not available, deferring join event.")
            event.defer()
            return

        if self.charm.peers.interface.state.joined is True:
            logger.info("Unit has already joined the cluster")
            return

        try:
            microceph.join_cluster(token=token, **self.charm._get_bootstrap_params())
            self.charm.peers.interface.state.joined = True
            self.charm.peers.set_unit_data({"joined": json.dumps(True)})
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.warning(e.stderr)
            raise e


class ClusterUpgrades(ops.framework.Object):
    """ClusterUpgrades manages snap upgrades across the cluster."""

    charm = None
    _stored = ops.framework.StoredState()

    def __init__(self, charm: "charm.MicroCephCharm"):
        super().__init__(charm, "cluster-upgrade")
        self.charm = charm

    @property
    def peer_int(self):
        """Get the peer interface."""
        return self.charm.peers.interface

    @property
    def channel(self) -> str:
        """Get the snap channel."""
        return self.charm.channel

    @channel.setter
    def channel(self, value: str) -> None:
        self.charm.channel = value

    @property
    def track(self) -> str:
        """Get the snap track."""
        c = self.channel
        if not self.channel:
            c = self.model.config["snap-channel"]
        return c.split("/")[0]

    def upgrade_requested(self, chan: str) -> bool:
        """Check if a snap upgrade was requested."""
        logger.debug(f"Requested, current channel: {chan}, {self.channel}")
        return self.channel != chan

    def can_upgrade_charm_payload(self, snap_chan: str) -> Tuple[bool, str]:
        """Check if snap can be upgraded."""
        if not microceph.can_upgrade_snap(self.track, snap_chan.split("/")[0]):
            msg = f"Cannot upgrade from {self.channel} to {snap_chan}"
            logger.warning(msg)
            return False, msg
        health, det = CephStatus().ceph_health()
        if health != CephHealth.Ok:
            msg = f"Cannot upgrade, ceph health not ok: {health}, {det}"
            logger.warning(msg)
            return False, msg
        return True, ""

    def perform_upgrade(self, channel: str) -> None:
        """Perform the snap upgrade on this node."""
        node = self.model.unit.name
        logger.debug(f"Upgrading {node} to {channel}")

        # Check if any of the non-service commands are running
        # Upgrading while a non-service command is running fails; checking
        # this here so we can return a meaningful error message
        cmds_pat = r"/snap/microceph/.*/(microceph|ceph|rados|rbd)"
        try:
            # As some of these are Python scripts we need to check against the
            # full command line
            subprocess.run(["pgrep", "-f", cmds_pat], check=True)
            msg = "Cannot upgrade, one of microceph|ceph|rados|rbd commands is running"
            logger.warning(msg)
            raise sunbeam_guard.BlockedExceptionError(msg)
        except subprocess.CalledProcessError:
            # pgrep didn't find the command and returned non-zero
            logger.debug("check running programs: none running")

        # TODO(peter) possibly set noout, noin for cases where upgrades take longer

        # let loose the dogs of upgrade
        mc_snap = snap.SnapCache()["microceph"]
        mc_snap.ensure(snap.SnapState.Present, channel=channel)

        @tenacity.retry(
            wait=tenacity.wait_fixed(5),
            stop=tenacity.stop_after_delay(600),
            retry=tenacity.retry_if_result(lambda b: not b),
        )
        def poll_ok():
            health, _ = CephStatus().ceph_health()
            return health == CephHealth.Ok

        poll_ok()  # wait for ceph to be healthy

        health, det = CephStatus().ceph_health()  # check again, get details
        if health != CephHealth.Ok:
            msg = f"Upgrade on {node} to {channel} failed: {health}, {det}"
            logger.error(msg)
            # don't continue on a failed upgrade
            raise sunbeam_guard.BlockedExceptionError(msg)

        logger.debug(f"Upgrade on {node} to {channel} done")

    def init_upgrade(self, snap_chan: str):
        """Kick off the snap upgrade."""
        logger.debug(f"Preparing upgrade from {self.channel} to {snap_chan}")
        self.channel = snap_chan

        # first upgrade this node. upgrade synchronously as we're still in
        # the config handler
        self.perform_upgrade(snap_chan)

        # then initialise peer upgrade upgrade by setting upgrade info on the
        # peer relation
        nonce = str(uuid.uuid4())
        peers = self.peer_int.all_joined_units()
        upgrade_nodes = sorted([u.name for u in peers])
        logger.debug(f"Upgrade init: {upgrade_nodes}, {snap_chan}, {nonce}")
        self.peer_int.set_upgrade_info(
            nonce,
            snap_chan,
            upgrade_nodes,
        )

    def upgrade_node_request(self, event: relation_handlers.UpgradeNodeRequestEvent):
        """Handle upgrade request for this node."""
        node = event.node
        channel = event.channel
        nonce = event.nonce
        logger.debug(f"Upgrading node {node}, {channel}, {nonce}")

        if node == self.model.unit.name:
            self.perform_upgrade(channel)  # raise exception on failure
            self.peer_int.on.upgrade_done.emit(node=node, channel=channel, nonce=nonce)

    def upgrade_node_done(self, event: relation_handlers.UpgradeNodeDoneEvent):
        """Signal upgrade done for this node."""
        logger.debug(f"Handle upgrade done {event.nonce}")
        self.peer_int.set_unit_data({"upgrade-done": event.nonce})
