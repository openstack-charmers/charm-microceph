# Copyright 2024 Canonical Ltd.
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

from unittest.mock import MagicMock, patch

import ops_sunbeam.test_utils as test_utils

import ceph_broker as broker


class TestBroker(test_utils.CharmTestCase):
    def setUp(self):
        pass

    @staticmethod
    def _raise_subproc(*args, **kwargs):
        raise broker.CalledProcessError(1, "")

    @patch.object(broker, "pool_exists")
    @patch.object(broker, "ErasurePool")
    @patch.object(broker, "erasure_profile_exists")
    def test_erasure_pool(self, ep_exists, epool, pool_exists):
        req = {"name": "mypool"}
        ep_exists.side_effect = lambda service, name: name == "some-profile"
        rv = broker.handle_erasure_pool(req, "admin")
        self.assertEqual(rv["exit-code"], 1)
        pool_exists.assert_not_called()
        epool.assert_not_called()

        req.update({"erasure-profile": "some-profile"})
        rv = broker.handle_erasure_pool(req, "admin")
        self.assertIsNone(rv)
        epool.assert_called()
        pool_exists.assert_called_with(service="admin", name="mypool")

    @patch.object(broker, "pool_exists")
    @patch.object(broker, "ReplicatedPool")
    @patch.object(broker, "get_osds")
    def test_replicated_pool(self, get_osds, rpool, pool_exists):
        req = {}
        get_osds.return_value = (1, 2, 3)
        rpool.side_effect = KeyError
        rv = broker.handle_replicated_pool(req, "admin")
        self.assertEqual(rv["exit-code"], 1)

        req = {"name": "mypool", "pg_num": 4, "replicas": 200}
        pool = MagicMock()
        rpool.reset_mock()
        rpool.side_effect = lambda *args, **kwargs: pool
        pool_exists.return_value = False
        rv = broker.handle_replicated_pool(req, "admin")
        self.assertIsNone(rv)
        self.assertEqual(req["pg_num"], 1)
        pool.create.assert_called()
        pool.update.assert_called()

    @patch.object(broker, "check_output")
    @patch.object(broker, "pool_exists")
    def test_create_cephfs(self, pool_exists, check_output):
        req = {}
        rv = broker.handle_create_cephfs(req, "admin")
        self.assertEqual(rv["exit-code"], 1)

        pool_exists.return_value = True
        check_output.return_value = broker.CalledProcessError(22, "")
        req = {"mds_name": "mds", "data_pool": "data", "metadata_pool": "meta"}
        rv = broker.handle_create_cephfs(req, "admin")
        self.assertIsNone(rv)
        check_output.assert_called_once()

        check_output.reset_mock()
        check_output.return_value = None
        rv = broker.handle_create_cephfs(req, "admin")

    @patch.object(broker, "check_output")
    def test_rgw_region_set(self, check_output):
        req = {}
        rv = broker.handle_rgw_region_set(req, "admin")
        self.assertEqual(rv["exit-code"], 1)

        check_output.side_effect = self._raise_subproc
        req = {
            "region-json": "region",
            "client-name": "client",
            "region-name": "name",
            "zone-name": "zone",
        }
        rv = broker.handle_rgw_region_set(req, "admin")
        self.assertEqual(rv["exit-code"], 1)
        check_output.assert_called_once()

        check_output.reset_mock()
        check_output.side_effect = MagicMock
        rv = broker.handle_rgw_region_set(req, "admin")
        self.assertIsNone(rv)

    @patch.object(broker, "check_output")
    def test_rgw_zone_set(self, check_output):
        req = {}
        rv = broker.handle_rgw_region_set(req, "admin")
        self.assertEqual(rv["exit-code"], 1)

        check_output.side_effect = self._raise_subproc
        req = {
            "zone-json": "json",
            "client-name": "client",
            "region-name": "name",
            "zone-name": "zone",
        }
        rv = broker.handle_rgw_zone_set(req, "admin")
        check_output.assert_called_once()

        check_output.reset_mock()
        check_output.side_effect = MagicMock
        rv = broker.handle_rgw_zone_set(req, "admin")
        self.assertIsNone(rv)

    @patch.object(broker, "check_output")
    def test_rgw_regionmap_update(self, check_output):
        req = {}
        rv = broker.handle_rgw_regionmap_update(req, "admin")
        self.assertEqual(rv["exit-code"], 1)

        check_output.side_effect = self._raise_subproc
        req = {"client-name": "client"}
        rv = broker.handle_rgw_regionmap_update(req, "admin")
        self.assertEqual(rv["exit-code"], 1)

        check_output.side_effect = MagicMock
        rv = broker.handle_rgw_regionmap_update(req, "admin")
        self.assertIsNone(rv)

    @patch.object(broker, "check_output")
    def test_rgw_regionmap_default(self, check_output):
        req = {}
        rv = broker.handle_rgw_regionmap_default(req, "admin")
        self.assertEqual(rv["exit-code"], 1)

        check_output.side_effect = self._raise_subproc
        req = {"rgw-region": "region", "client-name": "client"}
        rv = broker.handle_rgw_regionmap_default(req, "admin")
        self.assertEqual(rv["exit-code"], 1)

        check_output.side_effect = MagicMock
        rv = broker.handle_rgw_regionmap_default(req, "admin")
        self.assertIsNone(rv)

    @patch.object(broker, "check_output")
    def test_rgw_create_user(self, check_output):
        req = {}
        rv = broker.handle_rgw_create_user(req, "admin")
        self.assertEqual(rv["exit-code"], 1)

        check_output.side_effect = self._raise_subproc
        req = {"rgw-uid": "uid", "display-name": "name", "client-name": "client"}
        rv = broker.handle_rgw_create_user(req, "admin")
        self.assertEqual(rv["exit-code"], 1)

        check_output.side_effect = lambda *args: b'["some-user"]'
        rv = broker.handle_rgw_create_user(req, "admin")
        self.assertEqual(rv["exit-code"], 0)
        self.assertEqual(rv["user"][0], "some-user")

    @patch.object(broker, "get_osd_weight")
    @patch.object(broker, "check_output")
    def test_put_osd_in_bucket(self, check_output, gow):
        req = {}
        rv = broker.handle_put_osd_in_bucket(req, "admin")
        self.assertEqual(rv["exit-code"], 1)

        check_output.side_effect = self._raise_subproc
        req = {"osd": 1, "bucket": 1}
        rv = broker.handle_put_osd_in_bucket(req, "admin")
        self.assertEqual(rv["exit-code"], 1)

        check_output.side_effect = MagicMock
        rv = broker.handle_put_osd_in_bucket(req, "admin")
        self.assertIsNone(rv)

    @patch.object(broker, "check_call")
    @patch.object(broker, "check_output")
    def test_broker_misc(self, check_output, check_call):
        req = {}
        reqs = [req]

        for op in ("delete-pool", "rename-pool", "snapshot-pool", "remove-pool-snapshot"):
            req["op"] = op
            ret = broker.process_requests_v1(reqs)
            self.assertEqual(ret["exit-code"], 0)
