#!/bin/sh
# Copyright 2026 Supabase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

mkdir -p /var/log/postgresql 2>/dev/null || true
ln -sf /proc/1/fd/1 /var/log/postgresql/postgresql.json

exec /usr/local/bin/pgctld-bin \
	--postgres-config-template "${POSTGRES_CONFIG_TEMPLATE_PATH:-/etc/pgctld-custom/postgresql.conf.tmpl}" \
	"$@"
