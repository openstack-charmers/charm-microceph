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

from pathlib import Path
from subprocess import CalledProcessError
from unittest.mock import MagicMock, PropertyMock, mock_open, patch

import ops_sunbeam.test_utils as test_utils
from ops.testing import Harness

import charm
import microceph

DUMMY_CA_CERT = """-----BEGIN CERTIFICATE-----
MIIDdzCCAl+gAwIBAgIUexFR59kb53PwxGKCFFO32jHAGKwwDQYJKoZIhvcNAQEL
BQAwSzELMAkGA1UEBhMCSU4xCzAJBgNVBAgMAkFQMQwwCgYDVQQHDANWVFoxITAf
BgNVBAoMGEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDAeFw0yNDA2MjMwNTQ2Mjha
Fw0yOTA2MjIwNTQ2MjhaMEsxCzAJBgNVBAYTAklOMQswCQYDVQQIDAJBUDEMMAoG
A1UEBwwDVlRaMSEwHwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQwggEi
MA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCUmJ0xjPppm0YV8hPQjbZH9+LO
LU8HXUb2EYU9yb+UEP24grGar2zsVUBXWGJAXIAYejyDapSRjYoCnPECRHfrCqs2
vhZmQzPII+6Nllf3IpzS65TEfssfEtiSweN2sXLPymHaRKcq+rnmmpOM3vO396pc
COJX7WG/+qDJUhJthdbA008sKulG4Qq7NGaUA6Y4IMlZsZFEMp17rvFWNRSZBPVd
qrmW38v7rZfJwHrN4NL0me/1GZ+9ucnXnD5q/D1kRURt8J8cbFrPqGo4QwTzoNIi
D8Q7yRHUIMDY2MGmtpwzluh1HYg97IRJO0ciVXGL1yKEpELJ2Q32jS4xx2GJAgMB
AAGjUzBRMB0GA1UdDgQWBBSNg6SlHP06mM/vFPUoM9p37ZbUvzAfBgNVHSMEGDAW
gBSNg6SlHP06mM/vFPUoM9p37ZbUvzAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3
DQEBCwUAA4IBAQCGHlGuKr4L7nfZgFY1VZI14pSUvEZKPIXb4jPMvsVIdQY8wowM
9TDFmsDps0W+XZDNq5wwRtWiVKoNO6zw9ZKVlsKas4hnhqnaWD101xI9xN/ADax1
OHmBVcugXeYdWxmaz3JdiVKmwhiscmAiAWr4MS2FY/moZAl/U+YeIxbCxqKkZgJF
sEygfjVGcGUYrPvBB3SIyL+n8N9anht7u6ZY1chw6dnlT79mcx4huNE+NCSRK+7t
aU6GF5joUr0UWjFkoXpINM+ozet/bYvxa8MJ5OvSeU1ahHFeOmv0axs0JHvV0rW4
I7bWFePvjNsCPUyBSGu3GCisT5/FaxcS/IOA
-----END CERTIFICATE-----
"""


class _MicroCephCharm(charm.MicroCephCharm):
    """MicroCeph test charm."""

    def __init__(self, framework):
        """Setup event logging."""
        self.seen_events = []
        super().__init__(framework)

    def configure_ceph(self, event):
        return True


class TestCharm(test_utils.CharmTestCase):
    PATCHES = ["subprocess"]

    def setUp(self):
        """Setup MicroCeph Charm tests."""
        super().setUp(charm, self.PATCHES)
        with open("config.yaml", "r") as f:
            config_data = f.read()
        with open("metadata.yaml", "r") as f:
            metadata = f.read()
        self.harness = test_utils.get_harness(
            _MicroCephCharm,
            container_calls=self.container_calls,
            charm_config=config_data,
            charm_metadata=metadata,
        )
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    def add_complete_identity_relation(self, harness: Harness) -> None:
        """Add complete identity-service relation."""
        credentials_content = {"username": "svcuser1", "password": "svcpass1"}
        credentials_id = harness.add_model_secret("keystone", credentials_content)
        app_data = {
            "admin-domain-id": "admindomid1",
            "admin-project-id": "adminprojid1",
            "admin-user-id": "adminuserid1",
            "api-version": "3",
            "auth-host": "keystone.local",
            "auth-port": "12345",
            "auth-protocol": "http",
            "internal-host": "keystone.internal",
            "internal-port": "5000",
            "internal-protocol": "http",
            "internal-auth-url": "http://keystone.internal/v3",
            "service-domain": "servicedom",
            "service-domain_id": "svcdomid1",
            "service-host": "keystone.service",
            "service-port": "5000",
            "service-protocol": "http",
            "service-project": "svcproj1",
            "service-project-id": "svcprojid1",
            "service-credentials": credentials_id,
        }

        # Cannot use ops add_relation [1] directly due to secrets
        # [1] https://ops.readthedocs.io/en/latest/#ops.testing.Harness.add_relation
        rel_id = test_utils.add_base_identity_service_relation(harness)
        harness.grant_secret(credentials_id, harness.charm.app.name)
        harness.update_relation_data(rel_id, "keystone", app_data)

    def add_complete_ingress_relation(self, harness: Harness) -> None:
        """Add complete traefik-route relations."""
        harness.add_relation(
            "traefik-route-rgw",
            "traefik",
            app_data={"external_host": "dummy-ip", "scheme": "http"},
        )

    def add_complete_peer_relation(self, harness: Harness) -> None:
        """Add complete peer relation data."""
        harness.add_relation(
            "peers", harness.charm.app.name, unit_data={"public-address": "dummy-ip"}
        )

    def add_complete_certificate_transfer_relation(self, harness: Harness) -> None:
        """Add complete certificate_transfer relation."""
        harness.add_relation("receive-ca-cert", "keystone", unit_data={"ca": DUMMY_CA_CERT})

    @patch.object(microceph, "Client")
    @patch.object(microceph, "subprocess")
    @patch.object(Path, "chmod")
    @patch.object(Path, "write_bytes")
    @patch("builtins.open", new_callable=mock_open, read_data="mon host dummy-ip")
    def test_mandatory_relations(
        self, mock_file, mock_path_wb, mock_path_chmod, subprocess, cclient
    ):
        """Test the mandatory charm relations."""
        cclient.from_socket().cluster.list_services.return_value = []

        self.harness.set_leader()
        self.harness.update_config({"snap-channel": "1.0/stable"})
        self.add_complete_peer_relation(self.harness)

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

        # Assert RGW update configs is not called
        cclient.from_socket().cluster.update_config.assert_not_called()

    @patch.object(microceph, "Client")
    @patch.object(microceph, "subprocess")
    @patch.object(Path, "chmod")
    @patch.object(Path, "write_bytes")
    @patch("builtins.open", new_callable=mock_open, read_data="mon host dummy-ip")
    def test_all_relations(self, mock_file, mock_path_wb, mock_path_chmod, subprocess, cclient):
        """Test all the charms relations."""
        cclient.from_socket().cluster.list_services.return_value = []

        self.harness.set_leader()
        self.harness.update_config({"snap-channel": "1.0/stable"})
        self.add_complete_peer_relation(self.harness)
        self.add_complete_identity_relation(self.harness)
        self.add_complete_ingress_relation(self.harness)
        self.add_complete_certificate_transfer_relation(self.harness)

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

        # Assert RGW update configs is not called
        cclient.from_socket().cluster.update_config.assert_not_called()

    @patch("relation_handlers.Client", MagicMock())
    @patch.object(microceph, "Client")
    @patch.object(microceph, "subprocess")
    @patch.object(Path, "chmod")
    @patch.object(Path, "write_bytes")
    @patch("builtins.open", new_callable=mock_open, read_data="mon host dummy-ip")
    def test_all_relations_with_enable_rgw_config(
        self, mock_file, mock_path_wb, mock_path_chmod, subprocess, cclient
    ):
        """Test all the charms relations with rgw enabled."""
        cclient.from_socket().cluster.list_services.return_value = []

        self.harness.set_leader()
        self.harness.update_config({"snap-channel": "1.0/stable", "enable-rgw": "*"})
        test_utils.add_complete_peer_relation(self.harness)
        self.add_complete_identity_relation(self.harness)
        self.add_complete_ingress_relation(self.harness)
        self.add_complete_certificate_transfer_relation(self.harness)

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
        subprocess.run.assert_any_call(
            [
                "microceph",
                "enable",
                "rgw",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )

        # Check config rgw_swift_account_in_url is not updated since
        # namespace-projects is False by default.
        for call in cclient.from_socket().cluster.update_config.mock_calls:
            assert call.args[0] != "rgw_swift_account_in_url"

        # Check config rgw_keystone_verify_ssl is updated since certificate
        # transfer relation is set
        cclient.from_socket().cluster.update_config.assert_any_call(
            "rgw_keystone_verify_ssl", str(True).lower(), True
        )

    @patch("relation_handlers.Client", MagicMock())
    @patch.object(microceph, "Client")
    @patch.object(microceph, "subprocess")
    @patch.object(Path, "chmod")
    @patch.object(Path, "write_bytes")
    @patch("builtins.open", new_callable=mock_open, read_data="mon host dummy-ip")
    def test_all_relations_with_enable_rgw_config_and_namespace_projects(
        self, mock_file, mock_path_wb, mock_path_chmod, subprocess, cclient
    ):
        """Test all the charms relations with rgw and namespace_projects enabled."""
        cclient.from_socket().cluster.list_services.return_value = []

        self.harness.set_leader()
        self.harness.update_config(
            {"snap-channel": "1.0/stable", "enable-rgw": "*", "namespace-projects": True}
        )
        test_utils.add_complete_peer_relation(self.harness)
        self.add_complete_identity_relation(self.harness)
        self.add_complete_ingress_relation(self.harness)
        self.add_complete_certificate_transfer_relation(self.harness)

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
        subprocess.run.assert_any_call(
            [
                "microceph",
                "enable",
                "rgw",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )

        # Check config rgw_swift_account_in_url is updated since
        # namespace-projects is set to True.
        cclient.from_socket().cluster.update_config.assert_any_call(
            "rgw_swift_account_in_url", str(True).lower(), True
        )

        # Check config rgw_keystone_verify_ssl is updated since certificate
        # transfer relation is set
        cclient.from_socket().cluster.update_config.assert_any_call(
            "rgw_keystone_verify_ssl", str(True).lower(), True
        )

    @patch("relation_handlers.Client", MagicMock())
    @patch.object(microceph, "Client")
    @patch.object(microceph, "subprocess")
    @patch.object(Path, "chmod")
    @patch.object(Path, "write_bytes")
    @patch("builtins.open", new_callable=mock_open, read_data="mon host dummy-ip")
    def test_relations_without_certificate_transfer(
        self, mock_file, mock_path_wb, mock_path_chmod, subprocess, cclient
    ):
        """Test all the charms relations without certificate transfer relation."""
        cclient.from_socket().cluster.list_services.return_value = []

        self.harness.set_leader()
        self.harness.update_config(
            {"snap-channel": "1.0/stable", "enable-rgw": "*", "namespace-projects": True}
        )
        test_utils.add_complete_peer_relation(self.harness)
        self.add_complete_identity_relation(self.harness)
        self.add_complete_ingress_relation(self.harness)
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
        subprocess.run.assert_any_call(
            [
                "microceph",
                "enable",
                "rgw",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )

        # Check config rgw_swift_account_in_url is updated since
        # namespace-projects is set to True.
        cclient.from_socket().cluster.update_config.assert_any_call(
            "rgw_swift_account_in_url", str(True).lower(), True
        )

        # Check config rgw_keystone_verify_ssl is updated since certificate
        # transfer relation is set
        cclient.from_socket().cluster.update_config.assert_any_call(
            "rgw_keystone_verify_ssl", str(False).lower(), True
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
    def test_add_osds_action_with_already_added_device_id(self, _chk, subprocess):
        """Test action add_osds."""
        test_utils.add_complete_peer_relation(self.harness)
        self.harness._charm.peers.interface.state.joined = True

        disk = "/dev/sdb"
        error = 'Error: failed to record disk: This "disks" entry already exists\n'
        result = {"result": [{"spec": disk, "status": "failure", "message": error}]}
        subprocess.CalledProcessError = CalledProcessError
        subprocess.run.side_effect = CalledProcessError(returncode=1, cmd=["echo"], stderr=error)

        action_event = MagicMock()
        action_event.params = {"device-id": disk}
        self.harness.charm.storage._add_osd_action(action_event)

        subprocess.run.assert_called_with(
            ["microceph", "disk", "add", disk],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )
        action_event.set_results.assert_called_with(result)
        action_event.fail.assert_called()

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

        action_event.set_results.assert_called_with(
            {"message": "Node not yet joined in microceph cluster"}
        )
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
        action_event.set_results.assert_called_with(
            {"message": "Node not yet joined in microceph cluster"}
        )
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

    def test_get_rgw_endpoints_action_node_not_bootstrapped(self):
        """Test action get_rgw_endpoints when node not bootstrapped."""
        test_utils.add_complete_peer_relation(self.harness)

        action_event = MagicMock()
        self.harness.charm.rgw._get_rgw_endpoints_action(action_event)
        action_event.set_results.assert_called_with(
            {"message": "Rados gateway endpoints are not set yet"}
        )
        action_event.fail.assert_called()

    @patch("relation_handlers.Client", MagicMock())
    @patch.object(microceph, "Client")
    @patch.object(microceph, "subprocess")
    @patch("builtins.open", new_callable=mock_open, read_data="mon host dummy-ip")
    def test_get_rgw_endpoints_action_after_traefik_is_integrated(
        self, mock_file, subprocess, cclient
    ):
        """Test action get_rgw_endpoints after traefik is integrated."""
        cclient.from_socket().cluster.list_services.return_value = []
        self.harness.set_leader()
        self.harness.update_config(
            {"snap-channel": "1.0/stable", "enable-rgw": "*", "namespace-projects": True}
        )
        test_utils.add_complete_peer_relation(self.harness)
        self.add_complete_identity_relation(self.harness)
        self.add_complete_ingress_relation(self.harness)

        action_event = MagicMock()
        self.harness.charm.rgw._get_rgw_endpoints_action(action_event)
        expected_endpoints = {
            "swift": "http://dummy-ip/swift/v1/AUTH_$(project_id)s",
            "s3": "http://dummy-ip",
        }
        action_event.set_results.assert_called_with(expected_endpoints)
        action_event.fail.assert_not_called()

    @patch("charm.microceph_client.Client")
    def test_enter_maintenance_action_success(self, cclient):
        cclient.from_socket().cluster.enter_maintenance_mode.return_value = {
            "error": "",
            "actions": [],
        }
        action_event = MagicMock()
        action_event.params = {
            "force": False,
            "dry-run": False,
            "set-noout": True,
            "stop-osds": False,
        }

        self.harness.charm._enter_maintenance_action(action_event)
        action_event.set_results.assert_called_with(
            {"status": "success", "error": "", "actions": ""}
        )
        action_event.fail.assert_not_called()

    @patch("charm.microceph_client.Client")
    def test_enter_maintenance_action_failure(self, cclient):
        cclient.from_socket().cluster.enter_maintenance_mode.return_value = {
            "error": "check fails",
            "actions": [],
        }
        action_event = MagicMock()
        action_event.params = {
            "force": False,
            "dry-run": False,
            "set-noout": True,
            "stop-osds": False,
        }

        self.harness.charm._enter_maintenance_action(action_event)
        action_event.set_results.assert_called_with(
            {"status": "failure", "error": "check fails", "actions": ""}
        )
        action_event.fail.assert_called()

    @patch("charm.microceph_client.Client")
    def test_enter_maintenance_action_error(self, cclient):
        cclient.from_socket().cluster.enter_maintenance_mode.side_effect = Exception("some errors")
        action_event = MagicMock()
        action_event.params = {
            "force": False,
            "dry-run": False,
            "set-noout": True,
            "stop-osds": False,
        }

        self.harness.charm._enter_maintenance_action(action_event)
        action_event.set_results.assert_called_with(
            {"status": "failure", "error": "some errors", "actions": ""}
        )
        action_event.fail.assert_called()

    @patch("charm.microceph_client.Client")
    def test_exit_maintenance_action_success(self, cclient):
        cclient.from_socket().cluster.exit_maintenance_mode.return_value = {
            "error": "",
            "actions": [],
        }
        action_event = MagicMock()
        action_event.params = {"dry-run": False}

        self.harness.charm._exit_maintenance_action(action_event)
        action_event.set_results.assert_called_with(
            {"status": "success", "error": "", "actions": ""}
        )
        action_event.fail.assert_not_called()

    @patch("charm.microceph_client.Client")
    def test_exit_maintenance_action_failure(self, cclient):
        cclient.from_socket().cluster.exit_maintenance_mode.return_value = {
            "error": "check fails",
            "actions": [],
        }
        action_event = MagicMock()
        action_event.params = {"dry-run": False}

        self.harness.charm._exit_maintenance_action(action_event)
        action_event.set_results.assert_called_with(
            {"status": "failure", "error": "check fails", "actions": ""}
        )
        action_event.fail.assert_called()

    @patch("charm.microceph_client.Client")
    def test_exit_maintenance_action_error(self, cclient):
        cclient.from_socket().cluster.exit_maintenance_mode.side_effect = Exception("some errors")
        action_event = MagicMock()
        action_event.params = {"dry-run": False}

        self.harness.charm._exit_maintenance_action(action_event)
        action_event.set_results.assert_called_with(
            {"status": "failure", "error": "some errors", "actions": ""}
        )
        action_event.fail.assert_called()
