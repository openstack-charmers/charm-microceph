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

run="${1}"
shift

$run "$@"
