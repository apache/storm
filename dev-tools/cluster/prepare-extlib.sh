#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds the storm-metrics-prometheus reporter and collects it plus its runtime
# dependencies into ./extlib-daemon, which docker-compose mounts onto the Nimbus
# container's daemon classpath. Re-run after changing the module or Storm version.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
MODULE="external/storm-metrics-prometheus"
VERSION="${STORM_VERSION:-3.0.0-SNAPSHOT}"
OUT="${HERE}/extlib-daemon"

cd "${REPO_ROOT}"

echo "==> Building ${MODULE} (${VERSION})"
mvn -q -pl "${MODULE}" -am install -DskipTests

echo "==> Collecting runtime dependencies into ${OUT}"
rm -f "${OUT}"/*.jar
mvn -q -pl "${MODULE}" dependency:copy-dependencies \
    -DincludeScope=runtime -DexcludeScope=provided \
    -DoutputDirectory="${OUT}"

cp "${REPO_ROOT}/${MODULE}/target/storm-metrics-prometheus-${VERSION}.jar" "${OUT}/"

echo "==> ${OUT} now contains:"
ls -1 "${OUT}"/*.jar
