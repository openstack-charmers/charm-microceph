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


"""Handle Ceph commands.

The code is taken from below repo and changed bit to suit the needs of charm.
https://github.com/juju/charm-helpers/blob/master/charmhelpers/contrib/storage/linux/ceph.py

* log function overridden to write directly to logger instead of juju-log
* config function is NULLIFIED and always returns 0
* removed cmp_pkgrevno as ceph-common is not installed in microceph and latest ceph version is used
  remove any function calls pertained to older ceph releases
* only moved functions that are required by the charm
"""

import json
import logging
import math
import os
import socket
from subprocess import CalledProcessError, check_call, check_output

CRITICAL = "CRITICAL"
ERROR = "ERROR"
WARNING = "WARNING"
INFO = "INFO"
DEBUG = "DEBUG"
TRACE = "TRACE"

# The number of placement groups per OSD to target for placement group
# calculations. This number is chosen as 100 due to the ceph PG Calc
# documentation recommending to choose 100 for clusters which are not
# expected to increase in the foreseeable future. Since the majority of the
# calculations are done on deployment, target the case of non-expanding
# clusters as the default.
DEFAULT_PGS_PER_OSD_TARGET = 100
DEFAULT_POOL_WEIGHT = 10.0
LEGACY_PG_COUNT = 200
DEFAULT_MINIMUM_PGS = 2
AUTOSCALER_DEFAULT_PGS = 32

LEADER = "leader"
PEON = "peon"
QUORUM = [LEADER, PEON]

logger = logging.getLogger(__name__)


def log(message, level=DEBUG):
    """Write a log message.

    This function is mainly introduced not to change much in
    charms_ceph code.
    """
    log_func = getattr(logger, level.lower())
    log_func(message)


def config(param):
    """Return param value from charm config.

    This function is not implemented yet and returns None.
    """
    return None


def validator(value, valid_type, valid_range=None):
    """Helper function for type validation.

    Used to validate these:
    https://docs.ceph.com/docs/master/rados/operations/pools/#set-pool-values
    https://docs.ceph.com/docs/master/rados/configuration/bluestore-config-ref/#inline-compression

    Example input:
        validator(value=1,
                  valid_type=int,
                  valid_range=[0, 2])

    This says I'm testing value=1.  It must be an int inclusive in [0,2]

    :param value: The value to validate.
    :type value: any
    :param valid_type: The type that value should be.
    :type valid_type: any
    :param valid_range: A range of values that value can assume.
    :type valid_range: Optional[Union[List,Tuple]]
    :raises: AssertionError, ValueError
    """
    assert isinstance(value, valid_type), "{} is not a {}".format(value, valid_type)
    if valid_range is not None:
        assert isinstance(valid_range, list) or isinstance(
            valid_range, tuple
        ), "valid_range must be of type List or Tuple, " "was given {} of type {}".format(
            valid_range, type(valid_range)
        )
        # If we're dealing with strings
        if isinstance(value, str):
            assert value in valid_range, "{} is not in the list {}".format(value, valid_range)
        # Integer, float should have a min and max
        else:
            if len(valid_range) != 2:
                raise ValueError(
                    "Invalid valid_range list of {} for {}. "
                    "List must be [min,max]".format(valid_range, value)
                )
            assert value >= valid_range[0], "{} is less than minimum allowed value of {}".format(
                value, valid_range[0]
            )
            assert (
                value <= valid_range[1]
            ), "{} is greater than maximum allowed value of {}".format(value, valid_range[1])


def monitor_key_get(service, key):
    """Get the value of an existing key in the monitor cluster.

    :param service: The Ceph user name to run the command under
    :type service: str
    :param key: The key to search for.
    :type key: str
    :return: Returns the value of that key or None if not found.
    :rtype: Optional[str]
    """
    try:
        output = check_output(["ceph", "--id", service, "config-key", "get", str(key)]).decode(
            "UTF-8"
        )
        return output
    except CalledProcessError as e:
        log("Monitor config-key get failed with message: {}".format(e.output))
        return None


def monitor_key_set(service, key, value):
    """Set a key value pair on the monitor cluster.

    :param service: The Ceph user name to run the command under.
    :type service str
    :param key: The key to set.
    :type key: str
    :param value: The value to set. This will be coerced into a string.
    :type value: str
    :raises: CalledProcessError
    """
    try:
        check_output(["ceph", "--id", service, "config-key", "put", str(key), str(value)])
    except CalledProcessError as e:
        log("Monitor config-key put failed with message: {}".format(e.output))
        raise


def erasure_profile_exists(service, name):
    """Check to see if an Erasure code profile already exists.

    :param service: The Ceph user name to run the command under
    :type service: str
    :param name: Name of profile to look for.
    :type name: str
    :returns: True if it exists, False otherwise.
    :rtype: bool
    """
    validator(value=name, valid_type=str)
    try:
        check_call(["ceph", "--id", service, "osd", "erasure-code-profile", "get", name])
        return True
    except CalledProcessError:
        return False


def pool_exists(service, name):
    """Check to see if a RADOS pool already exists."""
    try:
        """
        out = check_output(
            ['rados', '--id', service, 'lspools']).decode('utf-8')
        """
        out = check_output(["ceph", "osd", "lspools"]).decode("utf-8")
    except CalledProcessError:
        return False

    return name in out.split()


def update_pool(client, pool, settings):
    """Update pool properties.

    :param client: Client/User-name to authenticate with.
    :type client: str
    :param pool: Name of pool to operate on
    :type pool: str
    :param settings: Dictionary with key/value pairs to set.
    :type settings: Dict[str, str]
    :raises: CalledProcessError
    """
    cmd = ["ceph", "--id", client, "osd", "pool", "set", pool]
    for k, v in settings.items():
        # Add --yes-i-really-mean-it flag if setting pool size 1
        extend_cmd = [k, v]
        if k == "size" and v == "1":
            extend_cmd = extend_cmd + ["--yes-i-really-mean-it"]
        check_call(cmd + extend_cmd)


def set_app_name_for_pool(client, pool, name):
    """Calls `osd pool application enable` for the specified pool name.

    :param client: Name of the ceph client to use
    :type client: str
    :param pool: Pool to set app name for
    :type pool: str
    :param name: app name for the specified pool
    :type name: str

    :raises: CalledProcessError if ceph call fails
    """
    cmd = ["ceph", "--id", client, "osd", "pool", "application", "enable", pool, name]
    check_call(cmd)


def enabled_manager_modules():
    """Return a list of enabled manager modules.

    :rtype: List[str]
    """
    cmd = ["ceph", "mgr", "module", "ls"]
    cmd.append("--format=json")
    try:
        modules = check_output(cmd).decode("utf-8")
    except CalledProcessError as e:
        log("Failed to list ceph modules: {}".format(e), WARNING)
        return []
    modules = json.loads(modules)
    return modules["enabled_modules"]


def enable_pg_autoscale(service, pool_name):
    """Enable Ceph's PG autoscaler for the specified pool.

    :param service: The Ceph user name to run the command under
    :type service: str
    :param pool_name: The name of the pool to enable sutoscaling on
    :type pool_name: str
    :raises: CalledProcessError if the command fails
    """
    check_call(
        ["ceph", "--id", service, "osd", "pool", "set", pool_name, "pg_autoscale_mode", "on"]
    )


def set_pool_quota(service, pool_name, max_bytes=None, max_objects=None):
    """Set byte quota on a RADOS pool in Ceph.

    :param service: The Ceph user name to run the command under
    :type service: str
    :param pool_name: Name of pool
    :type pool_name: str
    :param max_bytes: Maximum bytes quota to apply
    :type max_bytes: int
    :param max_objects: Maximum objects quota to apply
    :type max_objects: int
    :raises: subprocess.CalledProcessError
    """
    cmd = ["ceph", "--id", service, "osd", "pool", "set-quota", pool_name]
    if max_bytes:
        cmd = cmd + ["max_bytes", str(max_bytes)]
    if max_objects:
        cmd = cmd + ["max_objects", str(max_objects)]
    check_call(cmd)


def get_osds(service, device_class=None):
    """Return a list of all Ceph Object Storage Daemons in cluster.

    :param device_class: Class of storage device for OSD's
    :type device_class: str
    """
    if device_class:
        out = check_output(
            [
                "ceph",
                "--id",
                service,
                "osd",
                "crush",
                "class",
                "ls-osd",
                device_class,
                "--format=json",
            ]
        ).decode("utf-8")
    else:
        out = check_output(["ceph", "--id", service, "osd", "ls", "--format=json"]).decode("utf-8")
    return json.loads(out)


def get_erasure_profile(service, name):
    """Get an existing erasure code profile if it exists.

    :param service: The Ceph user name to run the command under.
    :type service: str
    :param name: Name of profile.
    :type name: str
    :returns: Dictionary with profile data.
    :rtype: Optional[Dict[str]]
    """
    try:
        out = check_output(
            ["ceph", "--id", service, "osd", "erasure-code-profile", "get", name, "--format=json"]
        ).decode("utf-8")
        return json.loads(out)
    except (CalledProcessError, OSError, ValueError):
        return None


class PoolCreationError(Exception):
    """A custom exception to inform the caller that a pool creation failed.

    Provides an error message
    """

    def __init__(self, message):
        super(PoolCreationError, self).__init__(message)


class BasePool(object):
    """An object oriented approach to Ceph pool creation.

    This base class is inherited by ReplicatedPool and ErasurePool. Do not call
    create() on this base class as it will raise an exception.

    Instantiate a child class and call create().
    """

    # Dictionary that maps pool operation properties to Tuples with valid type
    # and valid range
    op_validation_map = {
        "compression-algorithm": (str, ("lz4", "snappy", "zlib", "zstd")),
        "compression-mode": (str, ("none", "passive", "aggressive", "force")),
        "compression-required-ratio": (float, None),
        "compression-min-blob-size": (int, None),
        "compression-min-blob-size-hdd": (int, None),
        "compression-min-blob-size-ssd": (int, None),
        "compression-max-blob-size": (int, None),
        "compression-max-blob-size-hdd": (int, None),
        "compression-max-blob-size-ssd": (int, None),
        "rbd-mirroring-mode": (str, ("image", "pool")),
    }

    def __init__(self, service, name=None, percent_data=None, app_name=None, op=None):
        """Initialize BasePool object.

        Pool information is either initialized from individual keyword
        arguments or from a individual CephBrokerRq operation Dict.

        :param service: The Ceph user name to run commands under.
        :type service: str
        :param name: Name of pool to operate on.
        :type name: str
        :param percent_data: The expected pool size in relation to all
                             available resources in the Ceph cluster. Will be
                             used to set the ``target_size_ratio`` pool
                             property. (default: 10.0)
        :type percent_data: Optional[float]
        :param app_name: Ceph application name, usually one of:
                         ('cephfs', 'rbd', 'rgw') (default: 'unknown')
        :type app_name: Optional[str]
        :param op: Broker request Op to compile pool data from.
        :type op: Optional[Dict[str,any]]
        :raises: KeyError
        """
        # NOTE: Do not perform initialization steps that require live data from
        # a running cluster here. The *Pool classes may be used for validation.
        self.service = service
        self.op = op or {}

        if op:
            # When initializing from op the `name` attribute is required and we
            # will fail with KeyError if it is not provided.
            self.name = op["name"]
            self.percent_data = op.get("weight")
            self.app_name = op.get("app-name")
        else:
            self.name = name
            self.percent_data = percent_data
            self.app_name = app_name

        # Set defaults for these if they are not provided
        self.percent_data = self.percent_data or 10.0
        self.app_name = self.app_name or "unknown"

    def validate(self):
        """Check that value of supplied operation parameters are valid.

        :raises: ValueError
        """
        log(self.op.items())
        for op_key, op_value in self.op.items():
            if op_key in self.op_validation_map and op_value is not None:
                valid_type, valid_range = self.op_validation_map[op_key]
                try:
                    log(f"validating {op_key} {op_value} {valid_type}, {valid_range}")
                    validator(op_value, valid_type, valid_range)
                except (AssertionError, ValueError) as e:
                    # Normalize on ValueError, also add information about which
                    # variable we had an issue with.
                    raise ValueError("'{}': {}".format(op_key, str(e)))

    def _create(self):
        """Perform the pool creation, method MUST be overridden by child class."""
        raise NotImplementedError

    def _post_create(self):
        """Perform common post pool creation tasks.

        Note that pool properties subject to change during the lifetime of a
        pool / deployment should go into the ``update`` method.

        Do not add calls for a specific pool type here, those should go into
        one of the pool specific classes.
        """
        try:
            set_app_name_for_pool(client=self.service, pool=self.name, name=self.app_name)
        except CalledProcessError:
            log("Could not set app name for pool {}".format(self.name), level=WARNING)
        if "pg_autoscaler" in enabled_manager_modules():
            try:
                enable_pg_autoscale(self.service, self.name)
            except CalledProcessError as e:
                log(
                    "Could not configure auto scaling for pool {}: {}".format(self.name, e),
                    level=WARNING,
                )

    def create(self):
        """Create pool and perform any post pool creation tasks.

        To allow for sharing of common code among pool specific classes the
        processing has been broken out into the private methods ``_create``
        and ``_post_create``.

        Do not add any pool type specific handling here, that should go into
        one of the pool specific classes.
        """
        if not pool_exists(self.service, self.name):
            self.validate()
            self._create()
            self._post_create()
            self.update()

    def set_quota(self):
        """Set a quota if requested.

        :raises: CalledProcessError
        """
        max_bytes = self.op.get("max-bytes")
        max_objects = self.op.get("max-objects")
        if max_bytes or max_objects:
            set_pool_quota(
                service=self.service,
                pool_name=self.name,
                max_bytes=max_bytes,
                max_objects=max_objects,
            )

    def set_compression(self):
        """Set compression properties if requested.

        :raises: CalledProcessError
        """
        compression_properties = {
            key.replace("-", "_"): value
            for key, value in self.op.items()
            if key
            in (
                "compression-algorithm",
                "compression-mode",
                "compression-required-ratio",
                "compression-min-blob-size",
                "compression-min-blob-size-hdd",
                "compression-min-blob-size-ssd",
                "compression-max-blob-size",
                "compression-max-blob-size-hdd",
                "compression-max-blob-size-ssd",
            )
            and value
        }
        if compression_properties:
            update_pool(self.service, self.name, compression_properties)

    def update(self):
        """Update properties for an already existing pool.

        Do not add calls for a specific pool type here, those should go into
        one of the pool specific classes.
        """
        self.validate()
        self.set_quota()
        self.set_compression()

    def get_pgs(self, pool_size, percent_data=DEFAULT_POOL_WEIGHT, device_class=None):
        """Return the number of placement groups to use when creating the pool.

        Returns the number of placement groups which should be specified when
        creating the pool. This is based upon the calculation guidelines
        provided by the Ceph Placement Group Calculator (located online at
        http://ceph.com/pgcalc/).

        The number of placement groups are calculated using the following:

            (Target PGs per OSD) * (OSD #) * (%Data)
            ----------------------------------------
                         (Pool size)

        Per the upstream guidelines, the OSD # should really be considered
        based on the number of OSDs which are eligible to be selected by the
        pool. Since the pool creation doesn't specify any of CRUSH set rules,
        the default rule will be dependent upon the type of pool being
        created (replicated or erasure).

        This code makes no attempt to determine the number of OSDs which can be
        selected for the specific rule, rather it is left to the user to tune
        in the form of 'expected-osd-count' config option.

        :param pool_size: pool_size is either the number of replicas for
            replicated pools or the K+M sum for erasure coded pools
        :type pool_size: int
        :param percent_data: the percentage of data that is expected to
            be contained in the pool for the specific OSD set. Default value
            is to assume 10% of the data is for this pool, which is a
            relatively low % of the data but allows for the pg_num to be
            increased. NOTE: the default is primarily to handle the scenario
            where related charms requiring pools has not been upgraded to
            include an update to indicate their relative usage of the pools.
        :type percent_data: float
        :param device_class: class of storage to use for basis of pgs
            calculation; ceph supports nvme, ssd and hdd by default based
            on presence of devices of each type in the deployment.
        :type device_class: str
        :returns: The number of pgs to use.
        :rtype: int
        """
        # Note: This calculation follows the approach that is provided
        # by the Ceph PG Calculator located at http://ceph.com/pgcalc/.
        validator(value=pool_size, valid_type=int)

        # Ensure that percent data is set to something - even with a default
        # it can be set to None, which would wreak havoc below.
        if percent_data is None:
            percent_data = DEFAULT_POOL_WEIGHT

        # If the expected-osd-count is specified, then use the max between
        # the expected-osd-count and the actual osd_count
        osd_list = get_osds(self.service, device_class)
        expected = config("expected-osd-count") or 0

        if osd_list:
            if device_class:
                osd_count = len(osd_list)
            else:
                osd_count = max(expected, len(osd_list))

            # Log a message to provide some insight if the calculations claim
            # to be off because someone is setting the expected count and
            # there are more OSDs in reality. Try to make a proper guess
            # based upon the cluster itself.
            if not device_class and expected and osd_count != expected:
                log(
                    "Found more OSDs than provided expected count. "
                    "Using the actual count instead",
                    INFO,
                )
        elif expected:
            # Use the expected-osd-count in older ceph versions to allow for
            # a more accurate pg calculations
            osd_count = expected
        else:
            # NOTE(james-page): Default to 200 for older ceph versions
            # which don't support OSD query from cli
            return LEGACY_PG_COUNT

        percent_data /= 100.0
        target_pgs_per_osd = config("pgs-per-osd") or DEFAULT_PGS_PER_OSD_TARGET
        num_pg = (target_pgs_per_osd * osd_count * percent_data) // pool_size

        # NOTE: ensure a sane minimum number of PGS otherwise we don't get any
        #       reasonable data distribution in minimal OSD configurations
        if num_pg < DEFAULT_MINIMUM_PGS:
            num_pg = DEFAULT_MINIMUM_PGS

        # The CRUSH algorithm has a slight optimization for placement groups
        # with powers of 2 so find the nearest power of 2. If the nearest
        # power of 2 is more than 25% below the original value, the next
        # highest value is used. To do this, find the nearest power of 2 such
        # that 2^n <= num_pg, check to see if its within the 25% tolerance.
        exponent = math.floor(math.log(num_pg, 2))
        nearest = 2**exponent
        if (num_pg - nearest) > (num_pg * 0.25):
            # Choose the next highest power of 2 since the nearest is more
            # than 25% below the original value.
            return int(nearest * 2)
        else:
            return int(nearest)


class ErasurePool(BasePool):
    """Default jerasure erasure coded pool."""

    def __init__(
        self,
        service,
        name=None,
        erasure_code_profile=None,
        percent_data=None,
        app_name=None,
        op=None,
        allow_ec_overwrites=False,
    ):
        """Initialize ErasurePool object.

        Pool information is either initialized from individual keyword
        arguments or from a individual CephBrokerRq operation Dict.

        Please refer to the docstring of the ``BasePool`` class for
        documentation of the common parameters.

        :param erasure_code_profile: EC Profile to use (default: 'default')
        :type erasure_code_profile: Optional[str]
        """
        # NOTE: Do not perform initialization steps that require live data from
        # a running cluster here. The *Pool classes may be used for validation.

        # The common parameters are handled in our parents initializer
        super(ErasurePool, self).__init__(
            service=service, name=name, percent_data=percent_data, app_name=app_name, op=op
        )

        if op:
            # Note that the different default when initializing from op stems
            # from different handling of this in the `charms.ceph` library.
            self.erasure_code_profile = op.get("erasure-profile", "default-canonical")
            self.allow_ec_overwrites = op.get("allow-ec-overwrites")
        else:
            # We keep the class default when initialized from keyword arguments
            # to not break the API for any other consumers.
            self.erasure_code_profile = erasure_code_profile or "default"
            self.allow_ec_overwrites = allow_ec_overwrites

    def _create(self):
        # Try to find the erasure profile information in order to properly
        # size the number of placement groups. The size of an erasure
        # coded placement group is calculated as k+m.
        erasure_profile = get_erasure_profile(self.service, self.erasure_code_profile)

        # Check for errors
        if erasure_profile is None:
            msg = "Failed to discover erasure profile named " "{}".format(
                self.erasure_code_profile
            )
            log(msg, level=ERROR)
            raise PoolCreationError(msg)
        if "k" not in erasure_profile or "m" not in erasure_profile:
            # Error
            msg = (
                "Unable to find k (data chunks) or m (coding chunks) "
                "in erasure profile {}".format(erasure_profile)
            )
            log(msg, level=ERROR)
            raise PoolCreationError(msg)

        k = int(erasure_profile["k"])
        m = int(erasure_profile["m"])
        pgs = self.get_pgs(k + m, self.percent_data)
        cmd = [
            "ceph",
            "--id",
            self.service,
            "osd",
            "pool",
            "create",
            "--pg-num-min={}".format(min(AUTOSCALER_DEFAULT_PGS, pgs)),
            self.name,
            str(pgs),
            str(pgs),
            "erasure",
            self.erasure_code_profile,
        ]
        check_call(cmd)

    def _post_create(self):
        super(ErasurePool, self)._post_create()
        if self.allow_ec_overwrites:
            update_pool(self.service, self.name, {"allow_ec_overwrites": "true"})


class ReplicatedPool(BasePool):
    """Handles Replicated Pool."""

    def __init__(
        self,
        service,
        name=None,
        pg_num=None,
        replicas=None,
        percent_data=None,
        app_name=None,
        op=None,
        profile_name=None,
    ):
        """Initialize ReplicatedPool object.

        Pool information is either initialized from individual keyword
        arguments or from a individual CephBrokerRq operation Dict.

        Please refer to the docstring of the ``BasePool`` class for
        documentation of the common parameters.

        :param pg_num: Express wish for number of Placement Groups (this value
                       is subject to validation against a running cluster prior
                       to use to avoid creating a pool with too many PGs)
        :type pg_num: int
        :param replicas: Number of copies there should be of each object added
                         to this replicated pool.
        :type replicas: int
        :raises: KeyError
        :param profile_name: Crush Profile to use
        :type profile_name: Optional[str]
        """
        # NOTE: Do not perform initialization steps that require live data from
        # a running cluster here. The *Pool classes may be used for validation.

        # The common parameters are handled in our parents initializer
        super(ReplicatedPool, self).__init__(
            service=service, name=name, percent_data=percent_data, app_name=app_name, op=op
        )
        if op:
            # When initializing from op `replicas` is a required attribute, and
            # we will fail with KeyError if it is not provided.
            self.replicas = op["replicas"]
            self.pg_num = op.get("pg_num")
            self.profile_name = op.get("crush-profile") or profile_name
        else:
            self.replicas = replicas or 2
            self.pg_num = pg_num
            self.profile_name = profile_name

    def _create(self):
        cmd = [
            "ceph",
            "--id",
            self.service,
            "osd",
            "pool",
            "create",
            self.name,
        ]
        
        # If the requested weight is more than 10%, use bulk flag.
        if self.percent_data > DEFAULT_POOL_WEIGHT:
            cmd.append("--bulk")
        # Otherwise use the provided pg_num as the starting value for the pool. 
        elif self.pg_num:
            # Sanity check for PG count using the weightage.
            max_pgs = self.get_pgs(self.replicas, self.percent_data)
            self.pg_num = min(self.pg_num, max_pgs)
            log("Optimum PG num {} based on {} replicas and {}% weightage."
                .format(self.pg_num, self.replicas, self.percent_data))

            # Append the PG count to pool create command. 
            str(self.pg_num),
            cmd.append("--pg-num-min={}".format(min(AUTOSCALER_DEFAULT_PGS, self.pg_num))) 
        
        if self.profile_name:
            cmd.append(self.profile_name)
        check_call(cmd)

    def _post_create(self):
        # Set the pool replica size
        update_pool(client=self.service, pool=self.name, settings={"size": str(self.replicas)})
        # Perform other common post pool creation tasks
        super(ReplicatedPool, self)._post_create()

    def update(self):
        """Update properties for an already existing pool."""
        # Set the pool replica size
        update_pool(client=self.service, pool=self.name, settings={"size": str(self.replicas)})
        # Perform other common post pool creation tasks
        super(ReplicatedPool, self).update()


def get_osd_count():
    """Return the number of OSDs."""
    try:
        ret = check_output(["ceph", "osd", "ls"])
        return ret.decode("utf8").count("\n")
    except Exception as e:
        log("Failed getting the number of OSDs: {}".format(str(e)), WARNING)
        return 0


def ceph_user():
    """Return the ceph user name."""
    return "ceph"


def is_quorum():
    """Check if the monitor is in quorum."""
    asok = "/var/snap/microceph/current/run/ceph-mon.{}.asok".format(socket.gethostname())
    cmd = ["ceph", "--admin-daemon", asok, "mon_status"]
    if os.path.exists(asok):
        try:
            result = json.loads(str(check_output(cmd).decode("UTF-8")))
        except CalledProcessError:
            return False
        except ValueError:
            # Non JSON response from mon_status
            return False
        if result["state"] in QUORUM:
            return True
        else:
            return False
    else:
        return False
