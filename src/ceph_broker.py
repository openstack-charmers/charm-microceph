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


"""Handle Ceph Broker requests.

The code is taken from below repo and changed bit to suit the needs of charm.
https://opendev.org/openstack/charms.ceph/src/branch/master/charms_ceph/broker.py
"""

import collections
import functools
import json
import os
import socket
from subprocess import CalledProcessError, check_call, check_output
from tempfile import NamedTemporaryFile

from ceph import (
    DEBUG,
    ERROR,
    INFO,
    WARNING,
    ErasurePool,
    ReplicatedPool,
    erasure_profile_exists,
    get_osds,
    log,
    monitor_key_get,
    monitor_key_set,
    pool_exists,
)

VAR_LIB_CEPH = "/var/snap/microceph/common/data"


def decode_req_encode_rsp(f):
    """Decorator to decode incoming requests and encode responses."""

    def decode_inner(req):
        return json.dumps(f(json.loads(req)))

    return decode_inner


def get_group_key(group_name):
    """Build group key."""
    return "cephx.groups.{}".format(group_name)


def get_group(group_name):
    """Get group based on group_name.

    A group is a structure to hold data about a named group, structured as:
    {
        pools: ['glance'],
        services: ['nova']
    }
    """
    group_key = get_group_key(group_name=group_name)
    group_json = monitor_key_get(service="admin", key=group_key)
    try:
        group = json.loads(group_json)
    except (TypeError, ValueError):
        group = None
    if not group:
        group = {"pools": [], "services": []}
    return group


def save_group(group, group_name):
    """Persist a group in the monitor cluster."""
    group_key = get_group_key(group_name=group_name)
    return monitor_key_set(service="admin", key=group_key, value=json.dumps(group, sort_keys=True))


def _build_service_groups(service, namespace=None):
    """Rebuild the 'groups' dict for a service group.

    :returns: dict: dictionary keyed by group name of the following
                    format:

                    {
                        'images': {
                            pools: ['glance'],
                            services: ['nova', 'glance]
                         },
                         'vms':{
                            pools: ['nova'],
                            services: ['nova']
                         }
                    }
    """
    all_groups = {}
    for groups in service["group_names"].values():
        for group in groups:
            name = group
            if namespace:
                name = "{}-{}".format(namespace, name)
            all_groups[group] = get_group(group_name=name)
    return all_groups


def get_service_groups(service, namespace=None):
    """Get service groups based on service name.

    Services are objects stored with some metadata, they look like (for a
    service named "nova"):
    {
        group_names: {'rwx': ['images']},
        groups: {}
    }
    After populating the group, it looks like:
    {
        group_names: {'rwx': ['images']},
        groups: {
            'images': {
                pools: ['glance'],
                services: ['nova']
            }
        }
    }
    """
    service_json = monitor_key_get(service="admin", key="cephx.services.{}".format(service))
    try:
        service = json.loads(service_json)
    except (TypeError, ValueError):
        service = None
    if service:
        service["groups"] = _build_service_groups(service, namespace)
    else:
        service = {"group_names": {}, "groups": {}}
    return service


def pool_permission_list_for_service(service):
    """Build the permission string for Ceph for a given service."""
    permissions = []
    permission_types = collections.OrderedDict()
    for permission, group in sorted(service["group_names"].items()):
        if permission not in permission_types:
            permission_types[permission] = []
        for item in group:
            permission_types[permission].append(item)
    for permission, groups in permission_types.items():
        permission = "allow {}".format(permission)
        for group in groups:
            for pool in service["groups"][group].get("pools", []):
                permissions.append("{} pool={}".format(permission, pool))
    for permission, prefixes in sorted(service.get("object_prefix_perms", {}).items()):
        for prefix in prefixes:
            permissions.append("allow {} object_prefix {}".format(permission, prefix))
    return [
        "mon",
        ('allow r, allow command "osd blacklist"' ', allow command "osd blocklist"'),
        "osd",
        ", ".join(permissions),
    ]


def update_service_permissions(service, service_obj=None, namespace=None):
    """Update the key permissions for the named client in Ceph."""
    if not service_obj:
        service_obj = get_service_groups(service=service, namespace=namespace)
    permissions = pool_permission_list_for_service(service_obj)
    call = ["ceph", "auth", "caps", "client.{}".format(service)] + permissions
    try:
        check_call(call)
    except CalledProcessError as e:
        log("Error updating key capabilities: {}".format(e))


def add_pool_to_group(pool, group, namespace=None):
    """Add a named pool to a named group."""
    group_name = group
    if namespace:
        group_name = "{}-{}".format(namespace, group_name)
    group = get_group(group_name=group_name)
    if pool not in group["pools"]:
        group["pools"].append(pool)
    save_group(group, group_name=group_name)
    for service in group["services"]:
        update_service_permissions(service, namespace=namespace)


@decode_req_encode_rsp
def process_requests(reqs):
    """Process Ceph broker request(s).

    This is a versioned api. API version must be supplied by the client making
    the request.

    :param reqs: dict of request parameters.
    :returns: dict. exit-code and reason if not 0
    """
    request_id = reqs.get("request-id")
    try:
        version = reqs.get("api-version")
        if version == 1:
            log("Processing request {}".format(request_id), level=DEBUG)
            resp = process_requests_v1(reqs["ops"])
            if request_id:
                resp["request-id"] = request_id

            return resp

    except Exception as exc:
        log(str(exc), level=ERROR)
        msg = "Unexpected error occurred while processing requests: %s" % reqs
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}

    msg = "Missing or invalid api version ({})".format(version)
    resp = {"exit-code": 1, "stderr": msg}
    if request_id:
        resp["request-id"] = request_id

    return resp


_BROKER_JUMP_TABLE = None


def _get_broker_jump_table():
    global _BROKER_JUMP_TABLE
    ret = _BROKER_JUMP_TABLE
    if ret is not None:
        return ret

    ret = {
        "create-pool": handle_create_pool,
        "create-cephfs": handle_create_cephfs,
        "create-erasure-profile": handle_create_erasure_profile,
        "delete-pool": delete_pool,
        "rename-pool": rename_pool,
        "snapshot-pool": snapshot_pool,
        "remove-pool-snapshot": remove_pool_snapshot,
        "set-pool-value": handle_set_pool_value,
        "rgw-region-set": handle_rgw_region_set,
        "rgw-zone-set": handle_rgw_zone_set,
        "rgw-regionmap-update": handle_rgw_regionmap_update,
        "reg-regionmap-default": handle_rgw_regionmap_default,
        "rgw-create-user": handle_rgw_create_user,
        "move-osd-to-bucket": handle_put_osd_in_bucket,
        "add-permissions-to-key": handle_add_permissions_to_key,
        "set-key-permissions": handle_set_key_permissions,
    }

    _BROKER_JUMP_TABLE = ret
    return ret


def process_requests_v1(reqs):  # noqa: C901
    """Process v1 requests.

    Takes a list of requests (dicts) and processes each one. If an error is
    found, processing stops and the client is notified in the response.

    Returns a response dict containing the exit code (non-zero if any
    operation failed along with an explanation).
    """
    ret = None
    log("Processing {} ceph broker requests".format(len(reqs)), level=INFO)
    for req in reqs:
        op = req.get("op")
        log("Processing op='{}'".format(op), level=DEBUG)
        # Use admin client since we do not have other client key locations
        # setup to use them for these operations.
        svc = "admin"
        jump_table = _get_broker_jump_table()
        fn = jump_table.get(op)
        if fn is None:
            msg = "Unknown operation '{}'".format(op)
            log(msg, level=ERROR)
            return {"exit-code": 1, "stderr": msg}
        else:
            ret = fn(request=req, service=svc)

    if type(ret) == dict and "exit-code" in ret:
        return ret

    return {"exit-code": 0}


def handle_create_pool(request, service):
    """Handle the creation of an erasure or replicated pool."""
    pool_type = request.get("pool-type")
    if pool_type == "erasure":
        return handle_erasure_pool(request=request, service=service)
    return handle_replicated_pool(request=request, service=service)


def handle_erasure_pool(request, service):
    """Create a new erasure coded pool.

    :param request: dict of request operations and params.
    :param service: The ceph client to run the command under.
    :returns: dict. exit-code and reason if not 0.
    """
    pool_name = request.get("name")
    erasure_profile = request.get("erasure-profile")
    group_name = request.get("group")

    if erasure_profile is None:
        erasure_profile = "default-canonical"

    if group_name:
        group_namespace = request.get("group-namespace")
        # Add the pool to the group named "group_name"
        add_pool_to_group(pool=pool_name, group=group_name, namespace=group_namespace)

    # TODO: Default to 3/2 erasure coding. I believe this requires min 5 osds
    if not erasure_profile_exists(service=service, name=erasure_profile):
        # TODO: Fail and tell them to create the profile or default
        msg = (
            "erasure-profile {} does not exist.  Please create it with: "
            "create-erasure-profile".format(erasure_profile)
        )
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}

    try:
        pool = ErasurePool(service=service, op=request)
    except KeyError:
        msg = "Missing parameter."
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}

    # Ok make the erasure pool
    if not pool_exists(service=service, name=pool_name):
        log(
            "Creating pool '{}' (erasure_profile={})".format(pool.name, erasure_profile),
            level=INFO,
        )
        pool.create()

    # Set/update properties that are allowed to change after pool creation.
    pool.update()


def handle_replicated_pool(request, service):
    """Create a new replicated pool.

    :param request: dict of request operations and params.
    :param service: The ceph client to run the command under.
    :returns: dict. exit-code and reason if not 0.
    """
    pool_name = request.get("name")
    group_name = request.get("group")

    # Optional params
    # NOTE: Check this against the handling in the Pool classes, reconcile and
    # remove.
    pg_num = request.get("pg_num")
    replicas = request.get("replicas")
    if pg_num:
        # Cap pg_num to max allowed just in case.
        osds = get_osds(service)
        if osds:
            pg_num = min(pg_num, (len(osds) * 100 // replicas))
            request.update({"pg_num": pg_num})

    if group_name:
        group_namespace = request.get("group-namespace")
        # Add the pool to the group named "group_name"
        add_pool_to_group(pool=pool_name, group=group_name, namespace=group_namespace)

    try:
        pool = ReplicatedPool(service=service, op=request)
    except KeyError:
        msg = "Missing parameter."
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}

    if not pool_exists(service=service, name=pool_name):
        log("Creating pool '{}' (replicas={})".format(pool.name, replicas), level=INFO)
        pool.create()
    else:
        log("Pool '{}' already exists - skipping create".format(pool.name), level=DEBUG)

    # Set/update properties that are allowed to change after pool creation.
    pool.update()


# Below functions are picked from
# https://opendev.org/openstack/charms.ceph/src/branch/master/charms_ceph/utils.py

LEADER = "leader"

_default_caps = collections.OrderedDict(
    [
        ("mon", ["allow r", 'allow command "osd blacklist"', 'allow command "osd blocklist"']),
        ("osd", ["allow rwx"]),
    ]
)


def parse_key(raw_key):
    """Parse the key."""
    # get-or-create appears to have different output depending
    # on whether its 'get' or 'create'
    # 'create' just returns the key, 'get' is more verbose and
    # needs parsing
    key = None
    if len(raw_key.splitlines()) == 1:
        key = raw_key
    else:
        for element in raw_key.splitlines():
            if "key" in element:
                return element.split(" = ")[1].strip()  # IGNORE:E1103
    return key


@functools.lru_cache()
def ceph_auth_get(key_name):
    """Get ceph auth key."""
    try:
        # Does the key already exist?
        output = str(
            check_output(
                [
                    "ceph",
                    "--name",
                    "mon.",
                    "--keyring",
                    f"{VAR_LIB_CEPH}/mon/ceph-{socket.gethostname()}/keyring",
                    "auth",
                    "get",
                    key_name,
                ]
            ).decode("UTF-8")
        ).strip()
        return parse_key(output)
    except CalledProcessError:
        # Couldn't get the key
        pass


def get_named_key(name, caps=None, pool_list=None):
    """Retrieve a specific named cephx key.

    :param name: String Name of key to get.
    :param pool_list: The list of pools to give access to
    :param caps: dict of cephx capabilities
    :returns: Returns a cephx key
    """
    key_name = "client.{}".format(name)
    key = ceph_auth_get(key_name)
    if key:
        return key

    log("Creating new key for {}".format(name), level=DEBUG)
    caps = caps or _default_caps
    cmd = [
        "ceph",
        "--name",
        "mon.",
        "--keyring",
        f"{VAR_LIB_CEPH}/mon/ceph-{socket.gethostname()}/keyring",
        "auth",
        "get-or-create",
        key_name,
    ]
    # Add capabilities
    for subsystem, subcaps in caps.items():
        if subsystem == "osd":
            if pool_list:
                # This will output a string similar to:
                # "pool=rgw pool=rbd pool=something"
                pools = " ".join(["pool={0}".format(i) for i in pool_list])
                subcaps[0] = subcaps[0] + " " + pools
        cmd.extend([subsystem, "; ".join(subcaps)])
    ceph_auth_get.cache_clear()

    log("Calling check_output: {}".format(cmd), level=DEBUG)
    return parse_key(str(check_output(cmd).decode("UTF-8")).strip())  # IGNORE:E1103


def is_leader():
    """Check if this node is ceph mon leader."""
    hostname = socket.gethostname()
    cmd = ["ceph", "tell", f"mon.{hostname}", "mon_status", "--format", "json"]
    try:
        result = json.loads(str(check_output(cmd).decode("UTF-8")))
    except CalledProcessError:
        return False
    except ValueError:
        # Non JSON response from mon_status
        return False

    if result["state"] == LEADER:
        return True
    else:
        return False


# Ceph broker implementation.


def handle_create_cephfs(request, service):
    """Create a new cephfs.

    :param request: The broker request
    :param service: The ceph client to run the command under.
    :returns: dict. exit-code and reason if not 0
    """
    cephfs_name = request.get("mds_name")
    data_pool = request.get("data_pool")
    extra_pools = request.get("extra_pools", None) or []
    metadata_pool = request.get("metadata_pool")
    # Check if the user params were provided
    if not cephfs_name or not data_pool or not metadata_pool:
        msg = "Missing mds_name, data_pool or metadata_pool params"
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}

    # Sanity check that the required pools exist
    for pool_name in [data_pool, metadata_pool] + extra_pools:
        if not pool_exists(service=service, name=pool_name):
            msg = "CephFS pool {} does not exist. Cannot create CephFS".format(pool_name)
            log(msg, level=ERROR)
            return {"exit-code": 1, "stderr": msg}

    # Finally create CephFS
    try:
        check_output(["ceph", "--id", service, "fs", "new", cephfs_name, metadata_pool, data_pool])
    except CalledProcessError as err:
        if err.returncode == 22:
            log("CephFS already created")
            return
        else:
            log(err.output, level=ERROR)
            return {"exit-code": 1, "stderr": err.output}
    for pool_name in extra_pools:
        cmd = ["ceph", "--id", service, "fs", "add_data_pool", cephfs_name, pool_name]
        try:
            check_output(cmd)
        except CalledProcessError as err:
            log(err.output, level=ERROR)
            return {"exit-code": 1, "stderr": err.output}


def handle_create_erasure_profile(request, service):
    """Create an erasure profile.

    :param request: dict of request operations and params
    :param service: The ceph client to run the command under.
    :returns: dict. exit-code and reason if not 0
    """
    # "isa" | "lrc" | "shec" | "clay" or it defaults to "jerasure"
    erasure_type = request.get("erasure-type")
    # dependent on erasure coding type
    erasure_technique = request.get("erasure-technique")
    # "host" | "rack" | ...
    failure_domain = request.get("failure-domain")
    name = request.get("name")
    # Binary Distribution Matrix (BDM) parameters
    bdm_k = request.get("k")
    bdm_m = request.get("m")
    # LRC parameters
    bdm_l = request.get("l")
    crush_locality = request.get("crush-locality")
    # SHEC parameters
    bdm_c = request.get("c")
    # CLAY parameters
    bdm_d = request.get("d")
    scalar_mds = request.get("scalar-mds")
    # Device Class
    device_class = request.get("device-class")

    create_erasure_profile(
        service=service,
        erasure_plugin_name=erasure_type,
        profile_name=name,
        failure_domain=failure_domain,
        data_chunks=bdm_k,
        coding_chunks=bdm_m,
        locality=bdm_l,
        durability_estimator=bdm_d,
        helper_chunks=bdm_c,
        scalar_mds=scalar_mds,
        crush_locality=crush_locality,
        device_class=device_class,
        erasure_plugin_technique=erasure_technique,
    )

    return {"exit-code": 0}


def create_erasure_profile(  # noqa: C901
    service,
    profile_name,
    erasure_plugin_name="jerasure",
    failure_domain=None,
    data_chunks=2,
    coding_chunks=1,
    locality=None,
    durability_estimator=None,
    helper_chunks=None,
    scalar_mds=None,
    crush_locality=None,
    device_class=None,
    erasure_plugin_technique=None,
):
    """Create a new erasure code profile if one does not already exist for it.

    Profiles are considered immutable so will not be updated if the named
    profile already exists.

    Please refer to [0] for more details.

    0: http://docs.ceph.com/docs/master/rados/operations/erasure-code-profile/

    :param service: The Ceph user name to run the command under.
    :type service: str
    :param profile_name: Name of profile.
    :type profile_name: str
    :param erasure_plugin_name: Erasure code plugin.
    :type erasure_plugin_name: str
    :param failure_domain: Failure domain, one of:
                           ('chassis', 'datacenter', 'host', 'osd', 'pdu',
                            'pod', 'rack', 'region', 'room', 'root', 'row').
    :type failure_domain: str
    :param data_chunks: Number of data chunks.
    :type data_chunks: int
    :param coding_chunks: Number of coding chunks.
    :type coding_chunks: int
    :param locality: Locality.
    :type locality: int
    :param durability_estimator: Durability estimator.
    :type durability_estimator: int
    :param helper_chunks: int
    :type helper_chunks: int
    :param device_class: Restrict placement to devices of specific class.
    :type device_class: str
    :param scalar_mds: one of ['isa', 'jerasure', 'shec']
    :type scalar_mds: str
    :param crush_locality: LRC locality failure domain, one of:
                           ('chassis', 'datacenter', 'host', 'osd', 'pdu', 'pod',
                            'rack', 'region', 'room', 'root', 'row') or unset.
    :type crush_locaity: str
    :param erasure_plugin_technique: Coding technique for EC plugin
    :type erasure_plugin_technique: str
    :return: None.  Can raise CalledProcessError, ValueError or AssertionError
    """
    if erasure_profile_exists(service, profile_name):
        log("EC profile {} exists, skipping update".format(profile_name), level=WARNING)
        return

    cmd = [
        "ceph",
        "--id",
        service,
        "osd",
        "erasure-code-profile",
        "set",
        profile_name,
        "plugin={}".format(erasure_plugin_name),
        "k={}".format(str(data_chunks)),
        "m={}".format(str(coding_chunks)),
    ]

    if erasure_plugin_technique:
        cmd.append("technique={}".format(erasure_plugin_technique))

    if failure_domain:
        cmd.append("crush-failure-domain={}".format(failure_domain))

    if device_class:
        cmd.append("crush-device-class={}".format(device_class))

    # Add plugin specific information
    if erasure_plugin_name == "lrc":
        # LRC mandatory configuration
        if locality:
            cmd.append("l={}".format(str(locality)))
        else:
            raise ValueError("locality must be provided for lrc plugin")
        # LRC optional configuration
        if crush_locality:
            cmd.append("crush-locality={}".format(crush_locality))

    if erasure_plugin_name == "shec":
        # SHEC optional configuration
        if durability_estimator:
            cmd.append("c={}".format((durability_estimator)))

    if erasure_plugin_name == "clay":
        # CLAY optional configuration
        if helper_chunks:
            cmd.append("d={}".format(str(helper_chunks)))
        if scalar_mds:
            cmd.append("scalar-mds={}".format(scalar_mds))

    check_call(cmd)


def delete_pool(service, request):
    """Delete a RADOS pool from ceph."""
    cmd = [
        "ceph",
        "--id",
        service,
        "osd",
        "pool",
        "delete",
        request.get("name"),
        "--yes-i-really-really-mean-it",
    ]
    check_call(cmd)


def rename_pool(service, request):
    """Rename a Ceph pool from old_name to new_name.

    :param service: The Ceph user name to run the command under.
    :type service: str
    :param request: The request with the old and new names for the pool.
    :type request: dict
    """
    cmd = [
        "ceph",
        "--id",
        service,
        "osd",
        "pool",
        "rename",
        request.get("name"),
        request.get("new-name"),
    ]
    check_call(cmd)


def snapshot_pool(service, request):
    """Snapshots a RADOS pool in Ceph.

    :param service: The Ceph user name to run the command under.
    :type service: str
    :param request: The request with the pool and snapshot names.
    :type snapshot_name: dict
    :raises: CalledProcessError
    """
    cmd = [
        "ceph",
        "--id",
        service,
        "osd",
        "pool",
        "mksnap",
        request.get("name"),
        request.get("snapshot-name"),
    ]
    check_call(cmd)


def remove_pool_snapshot(service, request):
    """Remove a snapshot from a RADOS pool in Ceph.

    :param service: The Ceph user name to run the command under.
    :type service: str
    :param pool_name: Name of pool to remove snapshot from.
    :type pool_name: str
    :param snapshot_name: Name of snapshot to remove.
    :type snapshot_name: str
    :raises: CalledProcessError
    """
    cmd = [
        "ceph",
        "--id",
        service,
        "osd",
        "pool",
        "rmsnap",
        request.get("name"),
        request.get("snapshot-name"),
    ]
    check_call(cmd)


def handle_set_pool_value(request, service, coerce=False):
    """Sets an arbitrary pool value.

    :param request: dict of request operations and params
    :param service: The ceph client to run the command under.
    :param coerce: Try to parse/coerce the value into the correct type.
                   Used by the action code that only gets Str from Juju
    :returns: dict. exit-code and reason if not 0
    """
    pool_name = request.get("name")
    key = request.get("key")
    value = request.get("value")

    cmd = ["ceph", "--id", service, "osd", "pool", "set", pool_name, key, str(value).lower()]
    check_call(cmd)


def handle_rgw_region_set(request, service):
    """Set the rados gateway region.

    :param request: dict. The broker request.
    :param service: The ceph client to run the command under.
    :returns: dict. exit-code and reason if not 0
    """
    json_file = request.get("region-json")
    name = request.get("client-name")
    region_name = request.get("region-name")
    zone_name = request.get("zone-name")
    if not json_file or not name or not region_name or not zone_name:
        msg = "Missing json-file or client-name params"
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}
    infile = NamedTemporaryFile(delete=False)
    with open(infile.name, "w") as infile_handle:
        infile_handle.write(json_file)
    try:
        check_output(
            [
                "radosgw-admin",
                "--id",
                service,
                "region",
                "set",
                "--rgw-zone",
                zone_name,
                "--infile",
                infile.name,
                "--name",
                name,
            ]
        )
    except CalledProcessError as err:
        log(err.output, level=ERROR)
        return {"exit-code": 1, "stderr": err.output}
    finally:
        os.unlink(infile.name)


def handle_rgw_zone_set(request, service):
    """Create a radosgw zone.

    :param request: dict of request operations and params
    :param service: The ceph client to run the command under.
    :returns: dict. exit-code and reason if not 0
    """
    json_file = request.get("zone-json")
    name = request.get("client-name")
    region_name = request.get("region-name")
    zone_name = request.get("zone-name")
    if not json_file or not name or not region_name or not zone_name:
        msg = "Missing json-file or client-name params"
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}
    infile = NamedTemporaryFile(delete=False)
    with open(infile.name, "w") as infile_handle:
        infile_handle.write(json_file)
    try:
        check_output(
            [
                "radosgw-admin",
                "--id",
                service,
                "zone",
                "set",
                "--rgw-zone",
                zone_name,
                "--infile",
                infile.name,
                "--name",
                name,
            ]
        )
    except CalledProcessError as err:
        log(err.output, level=ERROR)
        return {"exit-code": 1, "stderr": err.output}
    finally:
        os.unlink(infile.name)


def handle_rgw_regionmap_update(request, service):
    """Change the radosgw region map.

    :param request: dict of request operations and params
    :param service: The ceph client to run the command under.
    :returns: dict. exit-code and reason if not 0
    """
    name = request.get("client-name")
    if not name:
        msg = "Missing rgw-region or client-name params"
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}
    try:
        check_output(["radosgw-admin", "--id", service, "regionmap", "update", "--name", name])
    except CalledProcessError as err:
        log(err.output, level=ERROR)
        return {"exit-code": 1, "stderr": err.output}


def handle_rgw_regionmap_default(request, service):
    """Create a radosgw region map.

    :param request: dict of request operations and params
    :param service: The ceph client to run the command under.
    :returns: dict. exit-code and reason if not 0
    """
    region = request.get("rgw-region")
    name = request.get("client-name")
    if not region or not name:
        msg = "Missing rgw-region or client-name params"
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}
    try:
        check_output(
            [
                "radosgw-admin",
                "--id",
                service,
                "regionmap",
                "default",
                "--rgw-region",
                region,
                "--name",
                name,
            ]
        )
    except CalledProcessError as err:
        log(err.output, level=ERROR)
        return {"exit-code": 1, "stderr": err.output}


def handle_rgw_create_user(request, service):
    """Create a new rados gateway user.

    :param request: dict of request operations and params
    :param service: The ceph client to run the command under.
    :returns: dict. exit-code and reason if not 0
    """
    user_id = request.get("rgw-uid")
    display_name = request.get("display-name")
    name = request.get("client-name")
    if not name or not display_name or not user_id:
        msg = "Missing client-name, display-name or rgw-uid"
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}
    try:
        create_output = check_output(
            [
                "radosgw-admin",
                "--id",
                service,
                "user",
                "create",
                "--uid",
                user_id,
                "--display-name",
                display_name,
                "--name",
                name,
                "--system",
            ]
        )
        try:
            user_json = json.loads(str(create_output.decode("UTF-8")))
            return {"exit-code": 0, "user": user_json}
        except ValueError as err:
            log(err, level=ERROR)
            return {"exit-code": 1, "stderr": err}

    except CalledProcessError as err:
        log(err.output, level=ERROR)
        return {"exit-code": 1, "stderr": err.output}


def handle_put_osd_in_bucket(request, service):
    """Move an osd into a specified crush bucket.

    :param request: dict of request operations and params
    :param service: The ceph client to run the command under.
    :returns: dict. exit-code and reason if not 0
    """
    osd_id = request.get("osd")
    target_bucket = request.get("bucket")
    if not osd_id or not target_bucket:
        msg = "Missing OSD ID or Bucket"
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}
    try:
        check_output(
            [
                "ceph",
                "--id",
                service,
                "osd",
                "crush",
                "set",
                str(osd_id),
                str(get_osd_weight(osd_id)),
                "root={}".format(target_bucket),
            ]
        )

    except Exception as exc:
        msg = "Failed to move OSD " "{} into Bucket {} :: {}".format(osd_id, target_bucket, exc)
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}


def handle_set_key_permissions(request, service):
    """Ensure the key has the requested permissions."""
    permissions = request.get("permissions")
    client = request.get("client")
    call = ["ceph", "--id", service, "auth", "caps", "client.{}".format(client)] + permissions
    try:
        check_call(call)
    except CalledProcessError as e:
        log("Error updating key capabilities: {}".format(e), level=ERROR)


def handle_add_permissions_to_key(request, service):
    """Groups are defined by the key cephx.groups.(namespace-)?-(name).

    This key will contain a dict serialized to JSON with data about the group,
    including pools and members.

    A group can optionally have a namespace defined that will be used to
    further restrict pool access.
    """
    resp = {"exit-code": 0}

    service_name = request.get("name")
    group_name = request.get("group")
    group_namespace = request.get("group-namespace")
    if group_namespace:
        group_name = "{}-{}".format(group_namespace, group_name)
    group = get_group(group_name=group_name)
    service_obj = get_service_groups(service=service_name, namespace=group_namespace)
    if request.get("object-prefix-permissions"):
        service_obj["object_prefix_perms"] = request.get("object-prefix-permissions")
    permission = request.get("group-permission") or "rwx"
    if service_name not in group["services"]:
        group["services"].append(service_name)
    save_group(group=group, group_name=group_name)
    if permission not in service_obj["group_names"]:
        service_obj["group_names"][permission] = []
    if group_name not in service_obj["group_names"][permission]:
        service_obj["group_names"][permission].append(group_name)
    save_service(service=service_obj, service_name=service_name)
    service_obj["groups"] = _build_service_groups(service_obj, group_namespace)
    update_service_permissions(service_name, service_obj, group_namespace)

    return resp


def save_service(service_name, service):
    """Persist a service in the monitor cluster."""
    service["groups"] = {}
    return monitor_key_set(
        service="admin",
        key="cephx.services.{}".format(service_name),
        value=json.dumps(service, sort_keys=True),
    )


def get_osd_weight(osd_id):
    """Returns the weight of the specified OSD.

    :returns: Float
    :raises: ValueError if the monmap fails to parse.
    :raises: CalledProcessError if our Ceph command fails.
    """
    try:
        tree = check_output(["ceph", "osd", "tree", "--format=json"])
        tree = tree.decode("UTF-8")
        try:
            json_tree = json.loads(tree)
            # Make sure children are present in the JSON
            if not json_tree["nodes"]:
                return None
            for device in json_tree["nodes"]:
                if device["type"] == "osd" and device["name"] == osd_id:
                    return device["crush_weight"]
        except ValueError as v:
            log("Unable to parse ceph tree json: {}. Error: {}".format(tree, v))
            raise
    except CalledProcessError as e:
        log("ceph osd tree command failed with message: {}".format(e))
        raise
