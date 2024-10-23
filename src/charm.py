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
import subprocess
from pathlib import Path
from socket import gethostname
from typing import List

import charms.operator_libs_linux.v2.snap as snap
import netifaces
import ops.framework
import ops_sunbeam.charm as sunbeam_charm
import ops_sunbeam.guard as sunbeam_guard
import ops_sunbeam.relation_handlers as sunbeam_rhandlers
from ops.main import main

import ceph
import cluster
import microceph
from ceph_broker import get_named_key
from microceph_client import ClusterServiceUnavailableException
from radosgw import RadosGWHandler
from relation_handlers import (
    CephClientProviderHandler,
    CephMdsProviderHandler,
    CephRadosGWProviderHandler,
    MicroClusterNewNodeEvent,
    MicroClusterNodeAddedEvent,
    MicroClusterPeerHandler,
    UpgradeNodeDoneEvent,
    UpgradeNodeRequestEvent,
)
from storage import StorageHandler

logger = logging.getLogger(__name__)
CACERT_FILE = "/usr/local/share/ca-certificates/receive-keystone-ca-bundle.crt"


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
        self.rgw = RadosGWHandler(self)

        # Initialise handlers for events.
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.stop, self._on_stop)
        self.framework.observe(self.on.set_pool_size_action, self._set_pool_size_action)
        self.framework.observe(self.on.peers_relation_created, self._on_peer_relation_created)
        self.framework.observe(self.on["peers"].relation_departed, self._on_peer_relation_departed)

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
        process = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=900)
        logger.debug(f"Command finished. stdout={process.stdout}, " f"stderr={process.stderr}")

        cmd = ["sudo", "snap", "alias", "microceph.ceph", "ceph"]
        logger.debug(f'Running command {" ".join(cmd)}')
        process = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=180)
        logger.debug(f"Command finished. stdout={process.stdout}, " f"stderr={process.stderr}")

        try:
            snap.SnapCache()["microceph"].hold()
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

    def _on_peer_relation_created(self, event: ops.framework.EventBase) -> None:
        # save self hostname in the unit databag
        self.peers.set_unit_data({self.unit.name: str(gethostname())})

        public_address = self.model.get_binding(binding_key="public").network.bind_address
        if public_address:
            logger.debug("Setting peer unit data")
            self.peers.set_unit_data({"public-address": str(public_address)})

    def _on_peer_relation_departed(self, event: ops.framework.EventBase) -> None:
        self.handle_traefik_ready(event)

    def configure_unit(self, event: ops.framework.EventBase) -> None:
        """Run configuration on this unit."""
        super().configure_unit(event)
        self._handle_receive_ca_cert(event)

    def _on_config_changed(self, event: ops.framework.EventBase) -> None:
        with sunbeam_guard.guard(self, "Checking configs"):
            if not self.is_valid_placement_directive(self.model.config.get("enable-rgw")):
                raise sunbeam_guard.BlockedExceptionError("Improper value for config enable-rgw")

            namespace_projects = self.leader_get("namespace-projects")
            if namespace_projects and json.loads(namespace_projects) != self.model.config.get(
                "namespace-projects"
            ):
                raise sunbeam_guard.BlockedExceptionError(
                    "Config namespace-projects cannot be changed after deployment"
                )

            self.configure_charm(event)

    def _handle_receive_ca_cert(self, event: ops.framework.EventBase) -> None:
        contexts = self.contexts()
        cacert_path = Path(CACERT_FILE)
        if hasattr(contexts.receive_ca_cert, "ca_bundle") and contexts.receive_ca_cert.ca_bundle:
            cacert_in_bytes = contexts.receive_ca_cert.ca_bundle.encode()
            if cacert_path.exists():
                cert_from_file = cacert_path.read_bytes()
                if cert_from_file == cacert_in_bytes:
                    # CA Cert already exists with same content
                    logger.debug("CA cert exists with same content")
                    return

            # Write CACert if file does not exist or ca cert content is modified
            logger.debug("Write CA Cert file to /usr/local/share/ca-certificates/")
            cacert_path.write_bytes(contexts.receive_ca_cert.ca_bundle.encode())
            cacert_path.chmod(0o640)
        else:
            try:
                cacert_path.unlink()
            except FileNotFoundError:
                # No CA Cert file to unlink
                # No need to run update-ca-certificates
                logger.debug("No CA Cert to delete, ignore")
                return

        try:
            logger.debug("Running update-ca-certificates to update the certs")
            subprocess.run(["update-ca-certificates"], check=True)
            if self.model.config.get("enable-rgw") == "*":
                self.configure_rgw_service(event)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            raise sunbeam_guard.BlockedExceptionError("Updating CA Certificates failed")

    def is_valid_placement_directive(self, directive: str) -> bool:
        """Check if placement directive is valid or not."""
        supported_directives = ["*", ""]
        return directive in supported_directives

    def _set_pool_size_action(self, event: ops.framework.EventBase) -> None:
        """Set the size for one or more pools."""
        pools = event.params.get("pools")
        size = event.params.get("size")

        try:
            microceph.set_pool_size(pools, size)
            event.set_results({"status": "success"})
        except subprocess.CalledProcessError:
            logger.warning("Failed to set new pool size")
            event.set_results({"message": "set-pool-size failed"})
            event.fail()

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

    @property
    def rgw_port(self):
        """Port used for RGW service."""
        return 80

    @property
    def service_endpoints(self):
        """Describe the swift/s3 service endpoints."""
        ingress_url = None
        try:
            if self.traefik_route_rgw and self.traefik_route_rgw.ready:
                scheme = self.traefik_route_rgw.interface.scheme
                external_host = self.traefik_route_rgw.interface.external_host
                ingress_url = f"{scheme}://{external_host}"
        except (AttributeError, KeyError):
            pass

        if not ingress_url:
            return []

        namespace_projects = self.leader_get("namespace-projects")
        if namespace_projects and json.loads(namespace_projects):
            swift_url = f"{ingress_url}/swift/v1/AUTH_$(project_id)s"
        else:
            swift_url = f"{ingress_url}/swift/v1"

        return [
            {
                "service_name": "swift",
                "type": "object-store",
                "description": "Object Store service",
                "internal_url": swift_url,
                "public_url": swift_url,
                "admin_url": f"{ingress_url}/swift",
            },
            {
                "service_name": "s3",
                "type": "s3",
                "description": "s3 service",
                "internal_url": f"{ingress_url}",
                "public_url": f"{ingress_url}",
                "admin_url": f"{ingress_url}",
            },
        ]

    @property
    def traefik_config(self) -> dict:
        """Config to publish to traefik."""
        model = self.model.name
        app = "rgw"

        router_cfg = {
            f"juju-{model}-{app}-router": {
                "rule": "PathPrefix(`/`)",
                "service": f"juju-{model}-{app}-service",
                "entryPoints": ["web"],
            },
            f"juju-{model}-{app}-router-tls": {
                "rule": "PathPrefix(`/`)",
                "service": f"juju-{model}-{app}-service",
                "entryPoints": ["websecure"],
                "tls": {},
            },
        }

        # Note: Currently rgw, mon services are enabled on all storage nodes.
        # So get RGW IPs from mon ip addresses
        # https://github.com/canonical/microceph/issues/368
        # Get host key value from all units
        ips = self.peers.get_all_unit_values(key="public-address", include_local_unit=True)
        # ips = microceph.get_mon_public_addresses()
        rgw_lb_servers = [{"url": f"http://{ip}:{self.rgw_port}"} for ip in ips]
        health_check = {"path": "/swift/healthcheck", "scheme": "http"}
        service_cfg = {
            f"juju-{model}-{app}-service": {
                "loadBalancer": {"servers": rgw_lb_servers, "healthCheck": health_check}
            },
        }

        config = {
            "http": {
                "routers": router_cfg,
                "services": service_cfg,
            },
        }
        return config

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
        if self.can_add_handler("mds", handlers):
            self.mds = CephMdsProviderHandler(self, self.handle_ceph)
        if self.can_add_handler("traefik-route-rgw", handlers):
            self.traefik_route_rgw = sunbeam_rhandlers.TraefikRouteHandler(
                self,
                "traefik-route-rgw",
                self.handle_traefik_ready,
                "traefik-route-rgw" in self.mandatory_relations,
            )
            handlers.append(self.traefik_route_rgw)
        if self.can_add_handler("identity-service", handlers):
            self.id_svc = sunbeam_rhandlers.IdentityServiceRequiresHandler(
                self,
                "identity-service",
                self.configure_charm,
                self.service_endpoints,
                self.model.config["region"],
                "identity-service" in self.mandatory_relations,
            )
            handlers.append(self.id_svc)

        handlers = super().get_relation_handlers(handlers)
        logger.debug(f"Relation handlers: {handlers}")
        return handlers

    def ready_for_service(self) -> bool:
        """Check if service is ready or not."""
        if not snap.SnapCache()["microceph"].present:
            logger.warning("Snap microceph not installed yet.")
            return False

        if not microceph.is_cluster_member(gethostname()):
            logger.warning("Microceph not bootstrapped yet.")
            return False

        if not ceph.is_quorum():
            logger.debug("Ceph cluster not in quorum, not ready yet")
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
            "ceph-mon-public-addresses": public_addrs,
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

        self.set_leader_ready()
        if self.leader_get("namespace-projects") is None:
            self.leader_set(
                {"namespace-projects": json.dumps(self.model.config.get("namespace-projects"))}
            )
        self.configure_ceph(event)
        self.manage_rgw_service(event)
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

        if self.peers.interface.state.joined:
            self.manage_rgw_service(event)

    def _get_space_subnet(self, space: str):
        """Get the first available subnet in the network space."""
        space_nets = self.model.get_binding(binding_key=space).network.interfaces
        if space_nets:
            return space_nets[0].subnet

    def _get_bootstrap_params(self) -> dict:
        """Fetch bootstrap parameters."""
        micro_ip = cluster_net = public_net = ""
        snap_channel = snap.SnapCache()["microceph"].channel

        if "quincy" in snap_channel:
            # some quincy snap revisions do not support network configuration
            logger.warning("Juju spaces incompatible with quincy revision snaps")
            return {}

        try:
            # Public Network
            public_net = self._get_space_subnet(space="public")
            # Cluster Network
            cluster_net = self._get_space_subnet(space="cluster")
            # MicroCeph IP
            micro_ip = self.model.get_binding(binding_key="admin").network.bind_address

            logger.info(
                {
                    "snap-channel": snap_channel,
                    "public_net": public_net,
                    "cluster_net": cluster_net,
                    "micro_ip": micro_ip,
                }
            )
        except ops.model.ModelError as e:
            logger.exception(e)
        finally:
            return {
                "public_net": format(public_net),
                "cluster_net": format(cluster_net),
                "micro_ip": format(micro_ip),
            }

    def bootstrap_cluster(self, event: ops.framework.EventBase) -> None:
        """Bootstrap microceph cluster."""
        try:
            microceph.bootstrap_cluster(**self._get_bootstrap_params())
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

    def remove_rgw_configs(self, event: ops.framework.EventBase) -> None:
        """Remove RGW configs."""
        if not self.unit.is_leader():
            logger.debug("Not a leader unit, skipping setting rgw config")
            return

        configs = [
            "rgw_keystone_url",
            "rgw_keystone_admin_user",
            "rgw_keystone_admin_password",
            "rgw_keystone_api_version",
            "rgw_keystone_admin_domain",
            "rgw_keystone_admin_project",
            "rgw_keystone_accepted_roles",
            "rgw_keystone_accepted_admin_roles",
            "rgw_keystone_token_cache_size",
            "rgw_keystone_verify_ssl",
            "rgw_keystone_service_token_enabled",
            "rgw_keystone_service_token_accepted_roles",
            "rgw_s3_auth_use_keystone",
            "rgw_swift_account_in_url",
            "rgw_keystone_implicit_tenants",
            "rgw_swift_versioning_enabled",
        ]

        logger.info("Removing RGW Cluster configs")
        microceph.delete_cluster_configs(configs)

    def configure_rgw_service(self, event: ops.framework.EventBase) -> None:
        """Configure RGW service."""
        if not self.unit.is_leader():
            logger.debug("Not a leader unit, skipping setting rgw config")
            return

        configs = {}
        contexts = self.contexts()
        if self.id_svc.ready:
            try:
                configs = {
                    "rgw_keystone_url": self.id_svc.interface.internal_auth_url.removesuffix("v3"),
                    "rgw_keystone_admin_user": self.id_svc.interface.service_user_name,
                    "rgw_keystone_admin_password": self.id_svc.interface.service_password,
                    "rgw_keystone_api_version": "3",
                    "rgw_keystone_admin_domain": self.id_svc.interface.service_domain_name,
                    "rgw_keystone_admin_project": self.id_svc.interface.service_project_name,
                    "rgw_keystone_accepted_roles": "Member,member",
                    "rgw_keystone_accepted_admin_roles": f"{self.id_svc.interface.admin_role},ResellerAdmin",
                    "rgw_keystone_token_cache_size": "500",
                    "rgw_keystone_service_token_enabled": str(True).lower(),
                    "rgw_keystone_service_token_accepted_roles": self.id_svc.interface.admin_role,
                    "rgw_s3_auth_use_keystone": str(True).lower(),
                    "rgw_swift_versioning_enabled": str(True).lower(),
                }

                namespace_projects = self.leader_get("namespace-projects")
                if namespace_projects and json.loads(namespace_projects):
                    configs.update(
                        {
                            "rgw_swift_account_in_url": str(True).lower(),
                            "rgw_keystone_implicit_tenants": str(True).lower(),
                        }
                    )

                if (
                    hasattr(contexts.receive_ca_cert, "ca_bundle")
                    and contexts.receive_ca_cert.ca_bundle
                ):
                    configs["rgw_keystone_verify_ssl"] = str(True).lower()
                else:
                    configs["rgw_keystone_verify_ssl"] = str(False).lower()

                logger.info("Updating RGW Cluster configs")
                microceph.update_cluster_configs(configs)
            except (AttributeError, KeyError) as e:
                logger.warning(f"Not configuring rgw: {str(e)}")
        else:
            self.remove_rgw_configs(event)

    def manage_rgw_service(self, event: ops.framework.EventBase) -> None:
        """Enable/Disable RGW service."""
        try:
            enabled = microceph.is_rgw_enabled(gethostname())
            logger.info(f"Microceph RGW enabled: {enabled}")
            if self.model.config.get("enable-rgw") == "*":
                self.configure_rgw_service(event)
                if not enabled:
                    logger.info("Enabling RGW service")
                    microceph.enable_rgw()
            else:
                if enabled:
                    logger.info("Disabling RGW service")
                    microceph.disable_rgw()
                self.remove_rgw_configs(event)

            # Update traefik lb members
            self.handle_traefik_ready(event)
        except ClusterServiceUnavailableException as e:
            logger.warning(str(e))
            event.defer()
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.warning(e.stderr)
            raise e

    def configure_ceph(self, event) -> None:
        """Configure Ceph."""
        if not self.ready_for_service():
            event.defer()
            return

        default_rf = self.model.config.get("default-pool-size")
        try:
            microceph.set_pool_size("", str(default_rf))
        except subprocess.CalledProcessError as e:
            if "unknown command" in e.stderr:
                # Instead of checking for Reef+ in the channel, we run the
                # command and then check against the error message. If the
                # above string is found, then the command isn't present
                # in microceph. Note that we only fail if the replication
                # factor isn't 3, as that is the default, and we run this
                # method on configuration changes.
                if default_rf != 3:
                    event.set_results(
                        {"message": "cannot set pool size: command not supported by microceph"}
                    )
                    event.fail()
                return
            raise e

    def _update_service_endpoints(self):
        try:
            if self.id_svc.update_service_endpoints:
                logger.debug("Updating service endpoints after ingress relation changed")
                self.id_svc.update_service_endpoints(self.service_endpoints)
        except (AttributeError, KeyError) as e:
            # Ignore AttributeError and KeyError as the exceptions are raised
            # if integration with identity_service is not yet set.
            logger.warning(f"Identity service relation not integrated: {str(e)}")

    def handle_traefik_ready(self, event: ops.framework.EventBase):
        """Handle Traefik route ready callback."""
        if not self.unit.is_leader():
            logger.debug("Not a leader unit, not updating traefik route config")
            return

        if self.traefik_route_rgw and self.traefik_route_rgw.interface.is_ready():
            logger.debug("Sending traefik config for rgw interface")
            self.traefik_route_rgw.interface.submit_to_traefik(config=self.traefik_config)

            if self.traefik_route_rgw.ready:
                self._update_service_endpoints()


if __name__ == "__main__":  # pragma: no cover
    main(MicroCephCharm)
