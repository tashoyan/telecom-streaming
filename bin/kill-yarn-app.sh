#!/bin/sh

set -o nounset
set -o errexit
set -o pipefail

rm_url='ossvert1.gre.hpecorp.net:8088'

app_ids="$@"
for app_id in $app_ids
do
    echo "Killing: $app_id"
    curl -X PUT -H "Content-Type: application/json" -d '{"state": "KILLED"}' "http://$rm_url/ws/v1/cluster/apps/$app_id/state"
done
