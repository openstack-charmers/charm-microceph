<a href="https://charmhub.io/microceph">
    <img alt="" src="https://charmhub.io/microceph/badge.svg" />
</a>
<br>
<a href="https://charmhub.io/microceph">
  <img alt="" src="https://charmhub.io/static/images/badges/en/charmhub-black.svg" />
</a>

# Overview

A charming way to deploy the microceph snap.

[MicroCeph][microceph-snap] is a lightweight way to deploy Ceph cluster in
reliable and resilient distributed storage.

The microceph charm deploys the microceph snap and can scale out to form a
Ceph cluster with [Juju](https://juju.is/).

# Usage

## Configuration

This section covers common and/or important configuration options. See file
`config.yaml` for the full list of options, along with their descriptions and
default values. See the [Juju documentation][juju-docs-config-apps] for details
on configuring applications.

#### `snap-channel`

The `snap-channel` option determines the microceph version to be deployed.

## Deployment

A cloud with three nodes is a typical design to deploy a minimal Ceph cluster.

    juju deploy -n 3 microceph --channel latest/edge --to 0,1,2

Add the disks on each node

    juju run microceph/0 add-osd <DISK PATH>,<DISK PATH>

## Storage

The microceph charm can take advantage of Juju storage devices automatically
enrolling disks as standalone OSDs or just provision them to be configured by
the operator manually.

For (say) 4 standalone OSDs on unit microceph/n.

    juju add-storage microceph/n osd-standalone='cinder,10G,4'

Provisioning block storage for manual configuration.

    juju add-storage microceph/n manual='cinder,10G,4'

## Actions

This section lists Juju [actions][juju-docs-actions] supported by the charm.
Actions allow specific operations to be performed on a per-unit basis. To
display action descriptions run `juju actions microceph`. If the charm is not
deployed then see file `actions.yaml`.

* `list-disks`
* `add-osd`

## Integrations

MicroCeph charm is expected to be integrated with the openstack control plane
via cross model relations.
For example to integrate glance application in k8s model to microceph, run the
below commands:

    juju offer microceph:ceph
    juju integrate -m k8s glance:ceph admin/controller.microceph

# Bugs

Please report bugs on [Github][charm-microceph-issues].
For general charm questions refer to the OpenStack [Charm Guide][cg].

<!-- LINKS -->

[cg]: https://docs.openstack.org/charm-guide
[charm-microceph-issues]: https://github.com/openstack-charmers/charm-microceph/issues
[juju-docs-actions]: https://jaas.ai/docs/actions
[juju-docs-config-apps]: https://juju.is/docs/configuring-applications
[microceph-snap]: https://snapcraft.io/microceph

