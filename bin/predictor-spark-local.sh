#!/bin/sh

set -o nounset
set -o errexit
set -o pipefail

jar_file="$(ls predictor-spark/target/predictor-spark-*.jar | grep -vi javadoc || true)"
if test -z "$jar_file"
then
    echo "Cannot find the application jar file. Is the project built? Exiting."
    exit 1
fi

app_name="$(basename $0)"
spark-submit \
--name "$app_name" \
--master "local[*]" \
--deploy-mode client \
--class com.github.tashoyan.telecom.predictor.SparkPredictorLocal1 \
"$jar_file"
