ARG SUPABASE_POSTGRES_TAG=17.6.1.173
ARG GO_VERSION=1.26
ARG PGBACKREST_VERSION

FROM alpine:3.23 AS pgbackrest-builder
ARG PGBACKREST_VERSION
RUN apk add --no-cache \
      build-base meson pkgconf perl curl linux-headers \
      openssl-dev libxml2-dev libpq-dev xz-dev zlib-dev \
      bzip2-dev zstd-dev lz4-dev yaml-dev
WORKDIR /src
RUN curl --fail --retry 3 --silent --show-error --location \
      "https://github.com/pgbackrest/pgbackrest/archive/refs/tags/release/${PGBACKREST_VERSION}.tar.gz" \
    | tar -xz --strip-components=1
RUN meson setup build --buildtype=release && meson compile -C build

FROM golang:${GO_VERSION}-alpine AS pgctld-builder

ARG GIT_COMMIT=unknown
ARG COMMIT_DATE=unknown

WORKDIR /src

RUN apk add --no-cache git make bash

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN GIT_COMMIT="${GIT_COMMIT}" COMMIT_DATE="${COMMIT_DATE}" make build-release

FROM supabase/postgres:${SUPABASE_POSTGRES_TAG}

USER root

RUN apk add --no-cache \
      libssl3 libcrypto3 libxml2 libpq xz-libs zlib libbz2 zstd-libs lz4-libs yaml
COPY --from=pgbackrest-builder /src/build/src/pgbackrest /usr/local/bin/pgbackrest
RUN pgbackrest version

COPY --from=pgctld-builder /src/bin/pgctld /usr/local/bin/pgctld-bin
COPY --chmod=0755 supabase/pgctld-wrapper.sh /usr/local/bin/pgctld
COPY supabase/postgresql.conf.tmpl /etc/pgctld-custom/postgresql.conf.tmpl

RUN sed -i \
        -e "s|^include = '/etc/postgresql-custom/wal-g.conf'|#include = '/etc/postgresql-custom/wal-g.conf'|" \
        -e "s|^data_directory |#data_directory |" \
        -e "s/ pgsodium,//g" \
        /etc/postgresql/postgresql.conf

RUN set -e && \
    manifest=/docker-entrypoint-initdb.d/multigres-init.sql && \
    printf -- "-- Auto-generated: run init-scripts and migrations after initdb\n" > "$manifest" && \
    printf "DO \$\$ BEGIN\n  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'postgres') THEN\n    CREATE ROLE postgres SUPERUSER LOGIN;\n  END IF;\nEND \$\$;\n" >> "$manifest" && \
    printf "ALTER DATABASE postgres OWNER TO postgres;\n\n" >> "$manifest" && \
    printf "SET SESSION AUTHORIZATION postgres;\n" >> "$manifest" && \
    for f in $(ls /docker-entrypoint-initdb.d/init-scripts/*.sql 2>/dev/null | sort); do \
        printf '\\ir init-scripts/%s\n' "$(basename "$f")" >> "$manifest"; \
    done && \
    printf "\nRESET SESSION AUTHORIZATION;\n\n" >> "$manifest" && \
    for f in $(ls /docker-entrypoint-initdb.d/migrations/*.sql 2>/dev/null | sort); do \
        printf '\\ir migrations/%s\n' "$(basename "$f")" >> "$manifest"; \
    done && \
    chown postgres:postgres "$manifest"

ENV POSTGRES_CONFIG_TEMPLATE_PATH=/etc/pgctld-custom/postgresql.conf.tmpl
ENV POSTGRES_INITDB_SQL_FILES=/docker-entrypoint-initdb.d/multigres-init.sql

STOPSIGNAL SIGINT
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD pg_isready -U postgres -h localhost || true
USER postgres
EXPOSE 5432
ENTRYPOINT ["tail"]
CMD ["-f", "/dev/null"]
