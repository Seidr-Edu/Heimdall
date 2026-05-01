#!/usr/bin/env bash
set -euo pipefail

cert_path="${1:-/etc/squid/andvari-intercept.crt}"
key_path="${2:-/etc/squid/andvari-intercept.key}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root: sudo bash $0 [cert_path] [key_path]" >&2
  exit 1
fi

install -d -m 0755 "$(dirname "$cert_path")"
install -d -m 0755 "$(dirname "$key_path")"

openssl req -x509 -nodes -newkey rsa:2048 \
  -subj "/CN=andvari-intercept.local" \
  -days 3650 \
  -keyout "$key_path" \
  -out "$cert_path"

chmod 0600 "$key_path"
chmod 0644 "$cert_path"

echo "wrote $cert_path and $key_path"
