#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
staging_dir="$repo_root/.codex-staging"

require_file() {
  if [[ ! -f "$1" ]]; then
    echo "missing required file: $1" >&2
    exit 1
  fi
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

ensure_package() {
  local pkg="$1"
  dpkg-query -W -f='${Status}' "$pkg" 2>/dev/null | grep -q "ok installed" || missing_packages+=("$pkg")
}

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root: sudo bash $0" >&2
  exit 1
fi

require_cmd apt-get
require_cmd docker
require_cmd iptables
require_cmd runuser
require_cmd squid
require_cmd systemctl

if ! squid -v 2>&1 | grep -qi openssl; then
  echo "installed squid lacks OpenSSL/ssl_bump support; install squid-openssl first" >&2
  exit 1
fi

declare -a missing_packages=()
ensure_package ulogd2
ensure_package ulogd2-json
if ((${#missing_packages[@]} > 0)); then
  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y "${missing_packages[@]}"
fi

require_file "$staging_dir/andvari-github-block.conf"
require_file "$staging_dir/andvari-access-log.conf"
require_file "$staging_dir/andvari-blocked-egress-ulogd.conf"
require_file "$staging_dir/heimdall-worker.override.conf"
require_file "$repo_root/proxy/andvari-egress-firewall.sh"
require_file "$repo_root/proxy/andvari-egress-firewall.service"
require_file "$repo_root/proxy/andvari-blocked-egress-logger.service"
require_file "$repo_root/proxy/create_andvari_intercept_cert.sh"
require_file /etc/squid/andvari-intercept.crt
require_file /etc/squid/andvari-intercept.key
require_file /usr/lib/squid/security_file_certgen
require_file /usr/sbin/ulogd

rm -f \
  /etc/squid/conf.d/andvari-github-block.conf \
  /etc/squid/conf.d/andvari-access-log.conf \
  /etc/rsyslog.d/30-andvari-blocked-egress.conf

install -m 0644 \
  "$staging_dir/andvari-github-block.conf" \
  /etc/squid/conf.d/10-andvari-github-block.conf
install -m 0644 \
  "$staging_dir/andvari-access-log.conf" \
  /etc/squid/conf.d/20-andvari-access-log.conf
install -d -m 0755 /etc/andvari
install -d -m 0755 /etc/systemd/system/heimdall-worker.service.d
install -m 0644 \
  "$staging_dir/andvari-blocked-egress-ulogd.conf" \
  /etc/andvari/blocked-egress-ulogd.conf
install -m 0644 \
  "$staging_dir/heimdall-worker.override.conf" \
  /etc/systemd/system/heimdall-worker.service.d/override.conf
install -m 0755 \
  "$repo_root/proxy/andvari-egress-firewall.sh" \
  /usr/local/sbin/andvari-egress-firewall
install -m 0644 \
  "$repo_root/proxy/andvari-egress-firewall.service" \
  /etc/systemd/system/andvari-egress-firewall.service
install -m 0644 \
  "$repo_root/proxy/andvari-blocked-egress-logger.service" \
  /etc/systemd/system/andvari-blocked-egress-logger.service

touch /var/log/squid/andvari-access.jsonl
chown proxy:proxy /var/log/squid/andvari-access.jsonl
chmod 0640 /var/log/squid/andvari-access.jsonl

install -d -m 0750 -o root -g adm /var/log/andvari
touch /var/log/andvari/blocked-egress.jsonl
chown root:adm /var/log/andvari/blocked-egress.jsonl
chmod 0640 /var/log/andvari/blocked-egress.jsonl

if [[ -d /var/spool/squid/ssl_db && ! -f /var/spool/squid/ssl_db/index.txt ]]; then
  rm -rf /var/spool/squid/ssl_db
fi
if [[ ! -f /var/spool/squid/ssl_db/index.txt ]]; then
  runuser -u proxy -- /usr/lib/squid/security_file_certgen -c -s /var/spool/squid/ssl_db -M 4MB
fi
chown -R proxy:proxy /var/spool/squid/ssl_db

squid -k parse
systemctl daemon-reload
systemctl enable andvari-blocked-egress-logger.service andvari-egress-firewall.service >/dev/null
systemctl disable --now ulogd2.service >/dev/null 2>&1 || true

if systemctl is-active --quiet squid; then
  systemctl reload squid
else
  systemctl start squid
fi
systemctl restart andvari-blocked-egress-logger.service
systemctl restart andvari-egress-firewall.service
systemctl try-restart rsyslog.service >/dev/null 2>&1 || true
systemctl restart heimdall-worker.service

echo "applied Squid, ulogd, firewall, and Heimdall worker updates"
