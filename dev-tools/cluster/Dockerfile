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

# Runtime image for a local Storm cluster, built FROM the locally compiled
# distribution so it runs *your* code, not a release from Docker Hub.
#
# The build context must be the directory that holds the distribution tarball,
# i.e. storm-dist/binary/final-package/target. Build with:
#
#   docker build -f dev-tools/cluster/Dockerfile \
#       --build-arg STORM_VERSION=3.0.0-SNAPSHOT \
#       -t storm-local:3.0.0-SNAPSHOT \
#       storm-dist/binary/final-package/target
#
# or simply `docker compose up --build` from this directory.
FROM eclipse-temurin:25-jre-jammy

ARG STORM_VERSION=3.0.0-SNAPSHOT

# The `storm` CLI is a Python 3 script; daemons shell out to `ps` (procps).
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV STORM_HOME=/opt/storm
ENV PATH=${STORM_HOME}/bin:${PATH}

# ADD auto-extracts the tarball; then move the versioned dir to a stable path.
ADD apache-storm-${STORM_VERSION}.tar.gz /opt/
RUN mv /opt/apache-storm-${STORM_VERSION} ${STORM_HOME} \
    && groupadd -r storm && useradd -r -g storm -d ${STORM_HOME} storm \
    && mkdir -p /data/storm \
    && chown -R storm:storm ${STORM_HOME} /data/storm

USER storm
WORKDIR ${STORM_HOME}

# 6627 nimbus thrift | 8080 UI | 8000 logviewer | 6700-6703 worker slots
EXPOSE 6627 8080 8000 6700 6701 6702 6703

# Default daemon; overridden per-service in docker-compose.yml.
CMD ["storm", "nimbus"]
