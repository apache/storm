FROM eclipse-temurin:21-jre

ENV STORM_CONF_DIR=/conf \
    STORM_DATA_DIR=/data \
    STORM_LOG_DIR=/logs

# Remove the default user 'ubuntu' and its group
RUN userdel -r ubuntu && \
    groupdel ubuntu || true

# Add a user with an explicit UID/GID and create necessary directories
RUN set -eux; \
    groupadd -r storm --gid=1000; \
    useradd -r -g storm --uid=1000 storm; \
    mkdir -p "$STORM_CONF_DIR" "$STORM_DATA_DIR" "$STORM_LOG_DIR"; \
    chown -R storm:storm "$STORM_CONF_DIR" "$STORM_DATA_DIR" "$STORM_LOG_DIR"

# Install required packages
RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        gosu \
        python3 \
        procps; \
    rm -rf /var/lib/apt/lists/*; \
    gosu nobody true

ARG DISTRO_NAME=apache-storm-2.8.5-SNAPSHOT

# Copy the local tarball and unpack it (skip download and GPG verification)
COPY ${DISTRO_NAME}.tar.gz /tmp/${DISTRO_NAME}.tar.gz
RUN set -eux; \
    tar -xzf /tmp/${DISTRO_NAME}.tar.gz -C /; \
    rm -f /tmp/${DISTRO_NAME}.tar.gz; \
    mv /${DISTRO_NAME} /apache-storm; \
    chown -R storm:storm /apache-storm

WORKDIR /apache-storm

ENV PATH $PATH:/apache-storm/bin

COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
