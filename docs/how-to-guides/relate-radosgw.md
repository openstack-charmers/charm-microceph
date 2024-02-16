# Relating the ceph-radosgw charm to charm-microceph

The Microceph charm can be related to the ceph-radosgw charm, enabling the use of the Rados Gateway interfaces. To do so, at least one Microceph and one ceph-radosgw unit needs to be deployed.

## Procedure

### 1. Deploy a ceph-radosgw unit to the juju model

Use the typical juju commands to deploy a ceph-radosgw unit:

       juju deploy ch:ceph-radosgw

With the above command, the number of units and the target machines can also be specified.

### 2. Relate the charms

The command needed to relate both charms differ, depending on the Juju version.

With juju 3.x:

       juju integrate ceph-radosgw charm-microceph

With juju 2.x:

       juju relate ceph-radosgw charm-microceph

Note that in order for the relation to complete, there needs to be available storage in the form of OSD's in the charm-microceph charm (i.e: by using `add-osd` action in a charm-microceph unit). If there's no storage, the ceph-radosgw unit(s) will remain blocked until there are enough OSD's.

After the model settles, we can check that everything is in place by running `juju status`. Here's sample output from a model with such a relation established:

```
Model  Controller               Cloud/Region             Version  SLA          Timestamp
ceph   lmlogiudice-serverstack  serverstack/serverstack  2.9.43   unsupported  18:37:50Z

App           Version  Status   Scale  Charm         Channel        Rev  Exposed  Message
ceph-radosgw  17.2.6   active       1  ceph-radosgw  quincy/stable  564  no       
microceph              active       1  microceph                      0  no       

Unit             Workload  Agent  Machine  Public address  Ports   Message
ceph-radosgw/0*  active    idle   0        10.5.2.150      80/tcp
microceph/0*     active    idle   1        10.5.0.21               

Machine  State    Address     Inst id                               Series  AZ    Message
0        started  10.5.2.150  e9100551-ae28-4989-85db-73c0e11806a0  jammy   nova  ACTIVE
1        started  10.5.0.21   4568d2a8-5c9b-4113-888b-fd78b9f82aa9  jammy   nova  ACTIVE
```
