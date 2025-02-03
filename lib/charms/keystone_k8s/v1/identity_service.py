"""IdentityServiceProvides and Requires module.


This library contains the Requires and Provides classes for handling
the identity_service interface.

Import `IdentityServiceRequires` in your charm, with the charm object and the
relation name:
    - self
    - "identity_service"

Also provide additional parameters to the charm object:
    - service
    - internal_url
    - public_url
    - admin_url
    - region
    - username
    - vhost

Two events are also available to respond to:
    - connected
    - ready
    - goneaway

A basic example showing the usage of this relation follows:

```
from charms.keystone_k8s.v1.identity_service import IdentityServiceRequires

class IdentityServiceClientCharm(CharmBase):
    def __init__(self, *args):
        super().__init__(*args)
        # IdentityService Requires
        self.identity_service = IdentityServiceRequires(
            self, "identity_service",
            service = "my-service"
            internal_url = "http://internal-url"
            public_url = "http://public-url"
            admin_url = "http://admin-url"
            region = "region"
        )
        self.framework.observe(
            self.identity_service.on.connected, self._on_identity_service_connected)
        self.framework.observe(
            self.identity_service.on.ready, self._on_identity_service_ready)
        self.framework.observe(
            self.identity_service.on.goneaway, self._on_identity_service_goneaway)

    def _on_identity_service_connected(self, event):
        '''React to the IdentityService connected event.

        This event happens when n IdentityService relation is added to the
        model before credentials etc have been provided.
        '''
        # Do something before the relation is complete
        pass

    def _on_identity_service_ready(self, event):
        '''React to the IdentityService ready event.

        The IdentityService interface will use the provided config for the
        request to the identity server.
        '''
        # IdentityService Relation is ready. Do something with the completed relation.
        pass

    def _on_identity_service_goneaway(self, event):
        '''React to the IdentityService goneaway event.

        This event happens when an IdentityService relation is removed.
        '''
        # IdentityService Relation has goneaway. shutdown services or suchlike
        pass
```
"""

import json
import logging

from ops import ModelError
from ops.framework import (
    EventBase,
    EventSource,
    Object,
    ObjectEvents,
    StoredState,
)
from ops.model import (
    Relation,
    SecretNotFoundError,
)

logger = logging.getLogger(__name__)

# The unique Charmhub library identifier, never change it
LIBID = "0fa7fe7236c14c6e9624acf232b9a3b0"

# Increment this major API version when introducing breaking changes
LIBAPI = 1

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 5


logger = logging.getLogger(__name__)


class IdentityServiceConnectedEvent(EventBase):
    """IdentityService connected Event."""

    pass


class IdentityServiceReadyEvent(EventBase):
    """IdentityService ready for use Event."""

    pass


class IdentityServiceGoneAwayEvent(EventBase):
    """IdentityService relation has gone-away Event"""

    pass


class IdentityServiceServerEvents(ObjectEvents):
    """Events class for `on`"""

    connected = EventSource(IdentityServiceConnectedEvent)
    ready = EventSource(IdentityServiceReadyEvent)
    goneaway = EventSource(IdentityServiceGoneAwayEvent)


class IdentityServiceRequires(Object):
    """
    IdentityServiceRequires class
    """

    on = IdentityServiceServerEvents()
    _stored = StoredState()

    def __init__(
        self,
        charm,
        relation_name: str,
        service_endpoints: list[dict],
        region: str,
    ):
        super().__init__(charm, relation_name)
        self.charm = charm
        self.relation_name = relation_name
        self.service_endpoints = service_endpoints
        self.region = region
        self.framework.observe(
            self.charm.on[relation_name].relation_joined,
            self._on_identity_service_relation_joined,
        )
        self.framework.observe(
            self.charm.on[relation_name].relation_changed,
            self._on_identity_service_relation_changed,
        )
        self.framework.observe(
            self.charm.on[relation_name].relation_departed,
            self._on_identity_service_relation_changed,
        )
        self.framework.observe(
            self.charm.on[relation_name].relation_broken,
            self._on_identity_service_relation_broken,
        )

    def _on_identity_service_relation_joined(self, event):
        """IdentityService relation joined."""
        logging.debug("IdentityService on_joined")
        self.on.connected.emit()
        self.register_services(self.service_endpoints, self.region)

    def _on_identity_service_relation_changed(self, event):
        """IdentityService relation changed."""
        logging.debug("IdentityService on_changed")
        try:
            self.service_password
            self.on.ready.emit()
        except (AttributeError, KeyError, ModelError):
            pass

    def _on_identity_service_relation_broken(self, event):
        """IdentityService relation broken."""
        logging.debug("IdentityService on_broken")
        self.on.goneaway.emit()

    @property
    def _identity_service_rel(self) -> Relation:
        """The IdentityService relation."""
        return self.framework.model.get_relation(self.relation_name)

    def get_remote_app_data(self, key: str) -> str:
        """Return the value for the given key from remote app data."""
        data = self._identity_service_rel.data[self._identity_service_rel.app]
        return data.get(key)

    @property
    def api_version(self) -> str:
        """Return the api_version."""
        return self.get_remote_app_data("api-version")

    @property
    def auth_host(self) -> str:
        """Return the auth_host."""
        return self.get_remote_app_data("auth-host")

    @property
    def auth_port(self) -> str:
        """Return the auth_port."""
        return self.get_remote_app_data("auth-port")

    @property
    def auth_protocol(self) -> str:
        """Return the auth_protocol."""
        return self.get_remote_app_data("auth-protocol")

    @property
    def internal_host(self) -> str:
        """Return the internal_host."""
        return self.get_remote_app_data("internal-host")

    @property
    def internal_port(self) -> str:
        """Return the internal_port."""
        return self.get_remote_app_data("internal-port")

    @property
    def internal_protocol(self) -> str:
        """Return the internal_protocol."""
        return self.get_remote_app_data("internal-protocol")

    @property
    def admin_domain_name(self) -> str:
        """Return the admin_domain_name."""
        return self.get_remote_app_data("admin-domain-name")

    @property
    def admin_domain_id(self) -> str:
        """Return the admin_domain_id."""
        return self.get_remote_app_data("admin-domain-id")

    @property
    def admin_project_name(self) -> str:
        """Return the admin_project_name."""
        return self.get_remote_app_data("admin-project-name")

    @property
    def admin_project_id(self) -> str:
        """Return the admin_project_id."""
        return self.get_remote_app_data("admin-project-id")

    @property
    def admin_user_name(self) -> str:
        """Return the admin_user_name."""
        return self.get_remote_app_data("admin-user-name")

    @property
    def admin_user_id(self) -> str:
        """Return the admin_user_id."""
        return self.get_remote_app_data("admin-user-id")

    @property
    def service_domain_name(self) -> str:
        """Return the service_domain_name."""
        return self.get_remote_app_data("service-domain-name")

    @property
    def service_domain_id(self) -> str:
        """Return the service_domain_id."""
        return self.get_remote_app_data("service-domain-id")

    @property
    def service_host(self) -> str:
        """Return the service_host."""
        return self.get_remote_app_data("service-host")

    @property
    def service_credentials(self) -> str:
        """Return the service_credentials secret."""
        return self.get_remote_app_data("service-credentials")

    @property
    def service_password(self) -> str:
        """Return the service_password."""
        credentials_id = self.get_remote_app_data("service-credentials")
        if not credentials_id:
            return None

        try:
            credentials = self.charm.model.get_secret(id=credentials_id)
            return credentials.get_content(refresh=True).get("password")
        except SecretNotFoundError:
            logger.warning(f"Secret {credentials_id} not found")
            return None

    @property
    def service_port(self) -> str:
        """Return the service_port."""
        return self.get_remote_app_data("service-port")

    @property
    def service_protocol(self) -> str:
        """Return the service_protocol."""
        return self.get_remote_app_data("service-protocol")

    @property
    def service_project_name(self) -> str:
        """Return the service_project_name."""
        return self.get_remote_app_data("service-project-name")

    @property
    def service_project_id(self) -> str:
        """Return the service_project_id."""
        return self.get_remote_app_data("service-project-id")

    @property
    def service_user_name(self) -> str:
        """Return the service_user_name."""
        credentials_id = self.get_remote_app_data("service-credentials")
        if not credentials_id:
            return None

        try:
            credentials = self.charm.model.get_secret(id=credentials_id)
            return credentials.get_content(refresh=True).get("username")
        except SecretNotFoundError:
            logger.warning(f"Secret {credentials_id} not found")
            return None

    @property
    def service_user_id(self) -> str:
        """Return the service_user_id."""
        return self.get_remote_app_data("service-user-id")

    @property
    def internal_auth_url(self) -> str:
        """Return the internal_auth_url."""
        return self.get_remote_app_data("internal-auth-url")

    @property
    def admin_auth_url(self) -> str:
        """Return the admin_auth_url."""
        return self.get_remote_app_data("admin-auth-url")

    @property
    def public_auth_url(self) -> str:
        """Return the public_auth_url."""
        return self.get_remote_app_data("public-auth-url")

    @property
    def admin_role(self) -> str:
        """Return the admin_role."""
        return self.get_remote_app_data("admin-role")

    def register_services(
        self, service_endpoints: list[dict], region: str
    ) -> None:
        """Request access to the IdentityService server."""
        if self.model.unit.is_leader():
            logging.debug("Requesting service registration")
            app_data = self._identity_service_rel.data[self.charm.app]
            app_data["service-endpoints"] = json.dumps(
                service_endpoints, sort_keys=True
            )
            app_data["region"] = region


class HasIdentityServiceClientsEvent(EventBase):
    """Has IdentityServiceClients Event."""

    pass


class ReadyIdentityServiceClientsEvent(EventBase):
    """IdentityServiceClients Ready Event."""

    def __init__(
        self,
        handle,
        relation_id,
        relation_name,
        service_endpoints,
        region,
        client_app_name,
    ):
        super().__init__(handle)
        self.relation_id = relation_id
        self.relation_name = relation_name
        self.service_endpoints = service_endpoints
        self.region = region
        self.client_app_name = client_app_name

    def snapshot(self):
        return {
            "relation_id": self.relation_id,
            "relation_name": self.relation_name,
            "service_endpoints": self.service_endpoints,
            "client_app_name": self.client_app_name,
            "region": self.region,
        }

    def restore(self, snapshot):
        super().restore(snapshot)
        self.relation_id = snapshot["relation_id"]
        self.relation_name = snapshot["relation_name"]
        self.service_endpoints = snapshot["service_endpoints"]
        self.region = snapshot["region"]
        self.client_app_name = snapshot["client_app_name"]


class IdentityServiceClientEvents(ObjectEvents):
    """Events class for `on`"""

    has_identity_service_clients = EventSource(HasIdentityServiceClientsEvent)
    ready_identity_service_clients = EventSource(
        ReadyIdentityServiceClientsEvent
    )


class IdentityServiceProvides(Object):
    """
    IdentityServiceProvides class
    """

    on = IdentityServiceClientEvents()
    _stored = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.charm = charm
        self.relation_name = relation_name
        self.framework.observe(
            self.charm.on[relation_name].relation_joined,
            self._on_identity_service_relation_joined,
        )
        self.framework.observe(
            self.charm.on[relation_name].relation_changed,
            self._on_identity_service_relation_changed,
        )
        self.framework.observe(
            self.charm.on[relation_name].relation_broken,
            self._on_identity_service_relation_broken,
        )

    def _on_identity_service_relation_joined(self, event):
        """Handle IdentityService joined."""
        logging.debug("IdentityService on_joined")
        self.on.has_identity_service_clients.emit()

    def _on_identity_service_relation_changed(self, event):
        """Handle IdentityService changed."""
        logging.debug("IdentityService on_changed")
        REQUIRED_KEYS = ["service-endpoints", "region"]

        values = [
            event.relation.data[event.relation.app].get(k)
            for k in REQUIRED_KEYS
        ]
        # Validate data on the relation
        if all(values):
            service_eps = json.loads(
                event.relation.data[event.relation.app]["service-endpoints"]
            )
            self.on.ready_identity_service_clients.emit(
                event.relation.id,
                event.relation.name,
                service_eps,
                event.relation.data[event.relation.app]["region"],
                event.relation.app.name,
            )

    def _on_identity_service_relation_broken(self, event):
        """Handle IdentityService broken."""
        logging.debug("IdentityServiceProvides on_departed")
        # TODO clear data on the relation

    def set_identity_service_credentials(
        self,
        relation_name: int,
        relation_id: str,
        api_version: str,
        auth_host: str,
        auth_port: str,
        auth_protocol: str,
        internal_host: str,
        internal_port: str,
        internal_protocol: str,
        service_host: str,
        service_port: str,
        service_protocol: str,
        admin_domain: dict,
        admin_project: dict,
        admin_user: dict,
        service_domain: dict,
        service_project: dict,
        service_user: dict,
        internal_auth_url: str,
        admin_auth_url: str,
        public_auth_url: str,
        service_credentials: str,
        admin_role: str,
    ):
        logging.debug("Setting identity_service connection information.")
        _identity_service_rel = None
        for relation in self.framework.model.relations[relation_name]:
            if relation.id == relation_id:
                _identity_service_rel = relation
        if not _identity_service_rel:
            # Relation has disappeared so skip send of data
            return
        app_data = _identity_service_rel.data[self.charm.app]
        app_data["api-version"] = api_version
        app_data["auth-host"] = auth_host
        app_data["auth-port"] = str(auth_port)
        app_data["auth-protocol"] = auth_protocol
        app_data["internal-host"] = internal_host
        app_data["internal-port"] = str(internal_port)
        app_data["internal-protocol"] = internal_protocol
        app_data["service-host"] = service_host
        app_data["service-port"] = str(service_port)
        app_data["service-protocol"] = service_protocol
        app_data["admin-domain-name"] = admin_domain.get("name")
        app_data["admin-domain-id"] = admin_domain.get("id")
        app_data["admin-project-name"] = admin_project.get("name")
        app_data["admin-project-id"] = admin_project.get("id")
        app_data["admin-user-name"] = admin_user.get("name")
        app_data["admin-user-id"] = admin_user.get("id")
        app_data["service-domain-name"] = service_domain.get("name")
        app_data["service-domain-id"] = service_domain.get("id")
        app_data["service-project-name"] = service_project.get("name")
        app_data["service-project-id"] = service_project.get("id")
        app_data["service-user-id"] = service_user.get("id")
        app_data["internal-auth-url"] = internal_auth_url
        app_data["admin-auth-url"] = admin_auth_url
        app_data["public-auth-url"] = public_auth_url
        app_data["service-credentials"] = service_credentials
        app_data["admin-role"] = admin_role
