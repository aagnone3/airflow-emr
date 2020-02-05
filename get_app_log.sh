#!/usr/bin/env bash
set -eou pipefail

cluster_master_ip=${1:-BAD}
application_id=${2:-BAD}

function usage() {
    echo ""
    echo "Usage: $0 <cluster_master_ip> <application_id>"
    echo "    cluster_master_ip: IP address of the EMR cluster's master node"
    echo "    application_id: Spark application ID to provide to Yarn to acquire logs"
    echo ""
    exit 1
}

( [[ "$cluster_master_ip" == "BAD" ]] || [[ "$application_id" == "BAD" ]] ) && usage
ssh hadoop@${cluster_master_ip} "yarn logs -applicationId ${application_id}" > log.json
