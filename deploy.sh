#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
mvn clean package -Passembly -Dmaven.test.skip=true

cd "${SCRIPT_DIR}/debezium-server-bigquery-dist/target/" || exit 1
echo "$(pwd)"
unzip -q -o "debezium-server-bigquery-dist*.zip"
cd debezium-server-bigquery || exit 1

if [ ! -f conf/application.properties ]; then
  echo "Copying default application.properties configuration example..."
  cp conf/application.properties.example conf/application.properties
else
  echo "Existing conf/application.properties found, keeping it."
fi

bash run.sh