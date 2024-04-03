#!/usr/bin/env bash

function check_osd_count() {
    local unit="${1?missing}"
    local count="${2?missing}"

    echo $USER

    osd_count=$(juju exec --unit ${unit} -- microceph disk list --json | jq '.ConfiguredDisks | length')
    if [[ $osd_count -ne $count ]] ; then
        echo "Expected OSDs $count, Actual ${osd_count}"
        exit 1
    fi

    juju status
}

function install_deps() {
    date
    sudo apt-get -qq install jq
    sudo snap install juju
    mkdir -p ~/.local/share/juju
    juju bootstrap localhost
    juju add-model microceph-test
    date
}

function install_juju_simple() {
    sudo snap install juju
    mkdir -p ~/.local/share/juju
    juju bootstrap localhost
}

function setup_juju_spaces() {
    set -x
    date
    juju add-model spacetest
    juju add-space cluster
    # Subnet value from LXD profile.
    juju move-to-space cluster 10.85.4.0/24
}

function seed_lxd_profile() {
    set -x
    date
    local file_path="${1?missing}"
    lxd init --verbose --preseed < $file_path
    lxc profile show default
    lxc network list
}

run="${1}"
shift

$run "$@"
