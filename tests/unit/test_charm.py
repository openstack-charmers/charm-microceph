# Copyright 2023 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for Microceph charm."""

from unittest.mock import MagicMock, PropertyMock, patch

import ops_sunbeam.test_utils as test_utils

import charm
import microceph


class _MicroCephCharm(charm.MicroCephCharm):
    """MicroCeph test charm."""

    def __init__(self, framework):
        """Setup event logging."""
        self.seen_events = []
        super().__init__(framework)


class TestCharm(test_utils.CharmTestCase):
    PATCHES = ["subprocess"]

    def setUp(self):
        """Setup MicroCeph Charm tests."""
        super().setUp(charm, self.PATCHES)
        with open("config.yaml", "r") as f:
            config_data = f.read()
        self.harness = test_utils.get_harness(
            _MicroCephCharm, container_calls=self.container_calls, charm_config=config_data
        )
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    @patch.object(microceph, "subprocess")
    def test_all_relations(self, subprocess):
        """Test all the charms relations."""
        self.harness.set_leader()
        self.harness.update_config({"snap-channel": "1.0/stable"})
        test_utils.add_complete_peer_relation(self.harness)
        subprocess.run.assert_any_call(
            [
                "microceph",
                "cluster",
                "bootstrap",
                "--public-network",
                "10.0.0.0/24",
                "--cluster-network",
                "10.0.0.0/24",
                "--microceph-ip",
                "10.0.0.10",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )

    @patch.object(microceph, "subprocess")
    @patch("ceph.check_output")
    def test_add_osds_action_with_device_id(self, _chk, subprocess):
        """Test action add_osds."""
        test_utils.add_complete_peer_relation(self.harness)
        self.harness._charm.peers.interface.state.joined = True

        action_event = MagicMock()
        action_event.params = {"device-id": "/dev/sdb"}
        self.harness.charm.storage._add_osd_action(action_event)

        action_event.set_results.assert_called()
        action_event.fail.assert_not_called()
        subprocess.run.assert_called_with(
            ["microceph", "disk", "add", "/dev/sdb"],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )

    @patch.object(microceph, "subprocess")
    @patch("ceph.check_output")
    def test_add_osds_action_with_loop_spec(self, _chk, subprocess):
        """Test action add_osds with loop file spec."""
        test_utils.add_complete_peer_relation(self.harness)
        self.harness._charm.peers.interface.state.joined = True

        action_event = MagicMock()
        action_event.params = {"loop-spec": "4G,3"}
        self.harness.charm.storage._add_osd_action(action_event)

        action_event.set_results.assert_called()
        action_event.fail.assert_not_called()
        subprocess.run.assert_called_with(
            ["microceph", "disk", "add", "loop,4G,3"],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )

    def test_add_osds_action_node_not_bootstrapped(self):
        """Test action add_osds when node not bootstrapped."""
        test_utils.add_complete_peer_relation(self.harness)

        action_event = MagicMock()
        action_event.params = {"device-id": "/dev/sdb"}
        self.harness.charm.storage._add_osd_action(action_event)

        action_event.set_results.assert_not_called()
        action_event.fail.assert_called()

    def _create_subprocess_output_mock(self, stdout):
        _mock = MagicMock()
        self.subprocess.run.return_value = _mock
        type(_mock).stdout = PropertyMock(return_value=stdout)
        return _mock

    def _test_list_disks_action(self, microceph_cmd_output, expected_disks):
        """Test action list_disks."""
        test_utils.add_complete_peer_relation(self.harness)
        self.harness._charm.peers.interface.state.joined = True

        action_event = MagicMock()
        self._create_subprocess_output_mock(microceph_cmd_output)

        self.harness.charm.storage._list_disks_action(action_event)
        action_event.set_results.assert_called_with(expected_disks)

    def test_list_disks_action_node_not_bootstrapped(self):
        """Test action list_disks when node not bootstrapped."""
        test_utils.add_complete_peer_relation(self.harness)

        action_event = MagicMock()
        self.harness.charm.storage._list_disks_action(action_event)
        action_event.set_results.assert_not_called()
        action_event.fail.assert_called()

    @patch.object(microceph, "subprocess")
    def test_list_disks_action_no_osds_no_disks(self, subprocess):
        self.subprocess = subprocess
        microceph_cmd_output = '{"ConfiguredDisks":[],"AvailableDisks":[]}'

        expected_disks = {"osds": [], "unpartitioned-disks": []}
        self._test_list_disks_action(microceph_cmd_output, expected_disks)

    @patch.object(microceph, "subprocess")
    def test_list_disks_action_no_osds_1_disk(self, subprocess):
        self.subprocess = subprocess
        microceph_cmd_output = """{
            "ConfiguredDisks":[],
            "AvailableDisks":[{
                    "model": "QEMU HARDDISK",
                    "size": "1.00GiB",
                    "type": "scsi",
                    "path": "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--1"
            }]
        }"""

        expected_disks = {
            "osds": [],
            "unpartitioned-disks": [
                {
                    "model": "QEMU HARDDISK",
                    "size": "1.00GiB",
                    "type": "scsi",
                    "path": "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--1",
                }
            ],
        }
        self._test_list_disks_action(microceph_cmd_output, expected_disks)

    @patch.object(microceph, "subprocess")
    def test_list_disks_action_1_osd_no_disks(self, subprocess):
        self.subprocess = subprocess
        microceph_cmd_output = """{
            "ConfiguredDisks":[{
                "osd":0,
                "location":"microceph-1",
                "path":"/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--1"
            }],
            "AvailableDisks":[]
        }"""

        expected_disks = {
            "osds": [
                {
                    "osd": 0,
                    "location": "microceph-1",
                    "path": "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--1",
                }
            ],
            "unpartitioned-disks": [],
        }
        self._test_list_disks_action(microceph_cmd_output, expected_disks)

    @patch.object(microceph, "subprocess")
    def test_list_disks_action_1_osd_1_disk(self, subprocess):
        self.subprocess = subprocess
        microceph_cmd_output = """{
            "ConfiguredDisks":[{
                "osd":0,
                "location":"microceph-1",
                "path":"/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--1"
            }],
            "AvailableDisks":[{
                    "model": "QEMU HARDDISK",
                    "size": "1.00GiB",
                    "type": "scsi",
                    "path": "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--2"
            }]
        }"""

        expected_disks = {
            "osds": [
                {
                    "osd": 0,
                    "location": "microceph-1",
                    "path": "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--1",
                }
            ],
            "unpartitioned-disks": [
                {
                    "model": "QEMU HARDDISK",
                    "size": "1.00GiB",
                    "type": "scsi",
                    "path": "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--2",
                }
            ],
        }
        self._test_list_disks_action(microceph_cmd_output, expected_disks)

    @patch.object(microceph, "subprocess")
    def test_list_disks_action_1_osd_no_disks_fqdn(self, subprocess):
        self.subprocess = subprocess
        microceph_cmd_output = """{
            "ConfiguredDisks":[{
                "osd":0,
                "location":"microceph-1.lxd",
                "path":"/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--1"
            }],
            "AvailableDisks":[]
        }"""

        expected_disks = {
            "osds": [
                {
                    "osd": 0,
                    "location": "microceph-1.lxd",
                    "path": "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--1",
                }
            ],
            "unpartitioned-disks": [],
        }
        self._test_list_disks_action(microceph_cmd_output, expected_disks)

    @patch("requests.get")
    def test_get_snap_info(self, mock_get):
        # Sample mocked response data
        mock_response_data = {
            "name": "test-snap",
            "summary": "A test snap",
            # ... add more fields as needed
        }
        mock_response = MagicMock()
        # mock_response.raise_for_status.return_value = None  # Avoid raising exceptions
        mock_response.json.return_value = mock_response_data
        mock_get.return_value = mock_response

        result = microceph.get_snap_info("test-snap")

        self.assertEqual(result, mock_response_data)
        mock_get.assert_called_once_with(
            "https://api.snapcraft.io/v2/snaps/info/test-snap",
            headers={"Snap-Device-Series": "16"},
        )

    @patch("microceph.get_snap_info")
    def test_get_snap_tracks(self, mock_get_snap_info):
        # Simulate get_snap_info output
        mock_snap_info = {
            "channel-map": [
                {"channel": {"track": "quincy/stable"}},
                {"channel": {"track": "reef/beta"}},
                {"channel": {"track": "quincy/stable"}},
            ]
        }
        mock_get_snap_info.return_value = mock_snap_info

        # Execute the code under test
        result = microceph.get_snap_tracks("test-snap")

        # Expected Assertion
        self.assertEqual(sorted(result), ["quincy/stable", "reef/beta"])

    @patch("microceph.get_snap_tracks")
    def test_can_upgrade_snap_empty_new_version(self, mock_get_snap_tracks):
        mock_get_snap_tracks.return_value = {"quincy", "reef"}
        result = microceph.can_upgrade_snap("quincy", "")
        self.assertFalse(result)

    @patch("microceph.get_snap_tracks")
    def test_can_upgrade_snap_to_latest(self, mock_get_snap_tracks):
        mock_get_snap_tracks.return_value = {"quincy", "reef"}
        result = microceph.can_upgrade_snap("latest", "latest")
        self.assertTrue(result)

    @patch("microceph.get_snap_tracks")
    def test_can_upgrade_snap_invalid_track(self, mock_get_snap_tracks):
        mock_get_snap_tracks.return_value = {"quincy"}
        result = microceph.can_upgrade_snap("latest", "invalid")
        self.assertFalse(result)

    @patch("microceph.get_snap_tracks")
    def test_can_upgrade_major_version(self, mock_get_snap_tracks):
        mock_get_snap_tracks.return_value = {"quincy", "reef"}
        result = microceph.can_upgrade_snap("quincy", "reef")
        self.assertTrue(result)

    @patch("microceph.get_snap_tracks")
    def test_cannot_downgrade_major_version(self, mock_get_snap_tracks):
        mock_get_snap_tracks.return_value = {"quincy", "reef"}
        result = microceph.can_upgrade_snap("reef", "quincy")
        self.assertFalse(result)

    @patch("microceph.get_snap_tracks")
    def test_can_upgrade_to_same_track(self, mock_get_snap_tracks):
        mock_get_snap_tracks.return_value = {"reef", "squid"}
        result = microceph.can_upgrade_snap("reef", "reef")
        self.assertTrue(result)

    @patch("microceph.get_snap_tracks")
    def test_can_upgrade_future(self, mock_get_snap_tracks):
        # hypothetical future releases
        mock_get_snap_tracks.return_value = {"zoidberg", "alphaville", "pyjama"}
        result = microceph.can_upgrade_snap("squid", "pyjama")
        self.assertTrue(result)
