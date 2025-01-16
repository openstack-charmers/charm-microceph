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
    set -ex
    date
    juju add-model microceph-test
    juju add-space cluster
    # Subnet value from LXD profile.
    juju move-to-space cluster 10.85.4.0/24
}

function ensure_osd_count_on_host() {
  set -ex
  local location="${1?missing}"
  local count="${2?missing}"

  disk_list=$(juju ssh microceph/leader -- "sudo microceph disk list --json")
  osd_count=$(echo $disk_list | jq '.ConfiguredDisks[].location' | grep -c $location || true)
  if [[ $osd_count -ne $count ]] ; then
    echo "Unexpected OSD count on node microceph/2 $osd_count"
    exit 1
  fi
}

function remove_unit_wait() {
  set -ex
  local unit_name="${1?missing}"

  juju remove-unit $unit_name --no-prompt
  # wait and check if the unit is still present.
  for i in $(seq 1 40); do
    res=$( ( juju status | grep -cF "$unit_name" ) || true )
    if [[ $res -gt 0 ]] ; then
      echo -n '.'
      sleep 5
    else
      echo "Unit removed successfully"
      break
    fi
  done
  # fail if unit still present.
  if [[ $res -gt 0 ]] ; then
    echo "Unit still present"
    juju status
    exit 1
  fi
}

function verify_juju_spaces_config() {
    set -ex
    date
    local public="10.196.231.0/24"
    local cluster="10.85.4.0/24"

    # Verify the cluster network is correct subnet.
    output=$(juju ssh microceph/0 -- "sudo microceph cluster config get cluster_network")
    echo $output | grep $cluster

    # Verify the public network is correct subnet.
    output=$(juju ssh microceph/0 -- "sudo microceph cluster sql 'select value from config where key is \"public_network\"'")
    echo $output | grep $public
}

function seed_lxd_profile() {
    set -ex
    date
    local file_path="${1?missing}"
    lxd init --verbose --preseed < $file_path
    lxc profile show default
    lxc network list
}

function wait_for_microceph_bootstrap() {
    # wait for install hook to trigger on one unit.
    juju wait-for unit microceph/0 --query='workload-message=="(install) (bootstrap) Service not bootstrapped"' --timeout=20m
    # Now wait for the model to settle.
    juju wait-for application microceph --query='name=="microceph" && (status=="active" || status=="idle")' --timeout=10m
}

function juju_crashdump() {
    local model=${1:-microceph-test}
    if ! snap list | grep -q "^juju-crashdump"; then
        sudo snap install --classic juju-crashdump
    fi
    juju-crashdump \
      -m $model \
      -o logs \
      -a juju-show-unit \
      -a juju-show-unit \
      -a juju-show-status-log \
      -a sosreport \
      --as-root \
      -j '*' \
      /var/snap/microceph/
}

function collect_microceph_logs() {
    mkdir -p logs
    local model=${1:-microceph-test}
    # Replace slash with - in model name if any
    local model_=${model/\//-}
    juju status -m $model -o logs/$model_.yaml
    juju debug-log -m $model --replay &> logs/$model_-debug-log.txt || echo "Not able to get logs for model $model"
    juju ssh microceph/leader sudo microceph status &> logs/microceph-status.txt || true
    juju ssh microceph/leader sudo microceph.ceph status &> logs/ceph-status.txt || true
    cat logs/$model_.yaml
    cat logs/microceph-status.txt
    juju_crashdump $model
}

function collect_sunbeam_and_microceph_logs() {
    mkdir -p logs
    kubectl="microk8s.kubectl"
    cp -rf $HOME/snap/openstack/common/logs/*.log logs/
    models=$(juju models --format json | jq -r .models[].name)
    for model in $models;
    do
      name=$(echo $model | cut -d/ -f2);
      juju status -m $model -o logs/$name.yaml;
      cat logs/$name.yaml;
      juju debug-log -m $model --replay &> logs/$name-debug-log.txt || echo "Not able to get logs for model $model"
      for pod in $(sudo $kubectl get pods -n $name -o=jsonpath='{.items[*].metadata.name}');
      do
        sudo $kubectl logs --ignore-errors -n $name --all-containers $pod &> logs/$pod.log || echo "Not able to get log for $pod"
      done
      juju_crashdump $model
    done
    juju ssh microceph/leader sudo microceph status &> logs/microceph-status.txt || true
    juju ssh microceph/leader sudo microceph.ceph status &> logs/ceph-status.txt || true
}

run="${1}"
shift

$run "$@"
