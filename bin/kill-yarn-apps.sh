#!/bin/sh

set -o nounset
set -o errexit
set -o pipefail

app_ids="$@"

jar_file="$(ls writer/target/yarn-killer-*.jar | grep -vi javadoc || true)"
if test -z "$jar_file"
then
    echo "Cannot find the application jar file. Is the project built? Exiting."
    exit 1
fi

app_name="$(basename $0)"
spark-submit \
--name "$app_name" \
--master local \
--class com.github.tashoyan.telecom.killer.KillYarnApps \
"$jar_file"
