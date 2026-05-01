#!/usr/bin/env bash
set -euo pipefail

worker_config="${HEIMDALL_WORKER_CONFIG:-/srv/pipeline/worker.yaml}"
filter_chain="ANDVARI_EGRESS_GUARD"
legacy_filter_chain="ANDVARI_PROXY_ONLY"
http_redirect_comment="andvari-egress-http-redirect"
https_redirect_comment="andvari-egress-https-redirect"
established_comment="andvari-egress-established"
guard_jump_comment="andvari-egress-guard"
nflog_group="${ANDVARI_NFLOG_GROUP:-32}"
http_intercept_port="${ANDVARI_HTTP_INTERCEPT_PORT:-3129}"
https_intercept_port="${ANDVARI_HTTPS_INTERCEPT_PORT:-3130}"
nflog_prefix="ANDVARI_EGRESS_BLOCK"

usage() {
  cat <<'EOF'
usage: andvari-egress-firewall.sh [apply|inspect]

apply    Detect the Andvari Docker network and apply transparent-egress rules.
inspect  Print the detected network values and the current live firewall state.
EOF
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "run as root" >&2
    exit 1
  fi
}

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

trim_quotes() {
  local value="$1"
  value="${value%\"}"
  value="${value#\"}"
  printf '%s\n' "$value"
}

read_worker_value() {
  local key="$1"
  awk -F':[[:space:]]*' -v key="$key" '$1 == key { print $2; exit }' "$worker_config"
}

detect_network() {
  require_file "$worker_config"

  network_name="$(trim_quotes "$(read_worker_value andvari_internal_network_name)")"
  if [[ -z "${network_name}" ]]; then
    echo "could not determine andvari_internal_network_name from $worker_config" >&2
    exit 1
  fi

  docker network inspect "$network_name" >/dev/null

  network_subnet="$(docker network inspect -f '{{with index .IPAM.Config 0}}{{.Subnet}}{{end}}' "$network_name")"
  network_gateway="$(docker network inspect -f '{{with index .IPAM.Config 0}}{{.Gateway}}{{end}}' "$network_name")"
  if [[ -z "${network_subnet}" || -z "${network_gateway}" ]]; then
    echo "could not determine subnet/gateway for docker network $network_name" >&2
    exit 1
  fi
}

chain_exists() {
  local table="$1"
  local chain="$2"
  iptables -w -t "$table" -n -L "$chain" >/dev/null 2>&1
}

delete_rule() {
  local table="$1"
  local chain="$2"
  shift 2

  while iptables -w -t "$table" -C "$chain" "$@" >/dev/null 2>&1; do
    iptables -w -t "$table" -D "$chain" "$@"
  done
}

ensure_chain() {
  local table="$1"
  local chain="$2"

  if ! chain_exists "$table" "$chain"; then
    iptables -w -t "$table" -N "$chain"
  fi
  iptables -w -t "$table" -F "$chain"
}

apply_filter_rules() {
  ensure_chain filter "$filter_chain"

  iptables -w -t filter -A "$filter_chain" \
    -j NFLOG --nflog-group "$nflog_group" --nflog-prefix "$nflog_prefix"
  iptables -w -t filter -A "$filter_chain" \
    -p tcp -j REJECT --reject-with tcp-reset
  iptables -w -t filter -A "$filter_chain" \
    -p udp -j REJECT --reject-with icmp-port-unreachable
  iptables -w -t filter -A "$filter_chain" \
    -j REJECT --reject-with icmp-admin-prohibited

  delete_rule filter DOCKER-USER \
    -s "$network_subnet" -m conntrack --ctstate RELATED,ESTABLISHED \
    -m comment --comment "$established_comment" -j RETURN
  delete_rule filter DOCKER-USER \
    -s "$network_subnet" -m conntrack --ctstate RELATED,ESTABLISHED -j RETURN
  delete_rule filter DOCKER-USER \
    -s "$network_subnet" -m comment --comment "$guard_jump_comment" \
    -j "$filter_chain"
  delete_rule filter DOCKER-USER \
    -s "$network_subnet" -j "$filter_chain"
  delete_rule filter DOCKER-USER \
    -s "$network_subnet" -j "$legacy_filter_chain"

  iptables -w -t filter -I DOCKER-USER 1 \
    -s "$network_subnet" -m conntrack --ctstate RELATED,ESTABLISHED \
    -m comment --comment "$established_comment" -j RETURN
  iptables -w -t filter -I DOCKER-USER 2 \
    -s "$network_subnet" -m comment --comment "$guard_jump_comment" \
    -j "$filter_chain"

  if chain_exists filter "$legacy_filter_chain"; then
    iptables -w -t filter -F "$legacy_filter_chain"
    iptables -w -t filter -X "$legacy_filter_chain" || true
  fi
}

apply_nat_rules() {
  delete_rule nat PREROUTING \
    -s "$network_subnet" ! -d "${network_gateway}/32" \
    -p tcp -m tcp --dport 80 \
    -m comment --comment "$http_redirect_comment" \
    -j REDIRECT --to-ports "$http_intercept_port"
  delete_rule nat PREROUTING \
    -s "$network_subnet" ! -d "${network_gateway}/32" \
    -p tcp -m tcp --dport 80 \
    -j REDIRECT --to-ports "$http_intercept_port"
  delete_rule nat PREROUTING \
    -s "$network_subnet" ! -d "${network_gateway}/32" \
    -p tcp -m tcp --dport 443 \
    -m comment --comment "$https_redirect_comment" \
    -j REDIRECT --to-ports "$https_intercept_port"
  delete_rule nat PREROUTING \
    -s "$network_subnet" ! -d "${network_gateway}/32" \
    -p tcp -m tcp --dport 443 \
    -j REDIRECT --to-ports "$https_intercept_port"

  iptables -w -t nat -I PREROUTING 1 \
    -s "$network_subnet" ! -d "${network_gateway}/32" \
    -p tcp -m tcp --dport 443 \
    -m comment --comment "$https_redirect_comment" \
    -j REDIRECT --to-ports "$https_intercept_port"
  iptables -w -t nat -I PREROUTING 1 \
    -s "$network_subnet" ! -d "${network_gateway}/32" \
    -p tcp -m tcp --dport 80 \
    -m comment --comment "$http_redirect_comment" \
    -j REDIRECT --to-ports "$http_intercept_port"
}

print_inspect() {
  echo "network_name=$network_name"
  echo "network_subnet=$network_subnet"
  echo "network_gateway=$network_gateway"
  echo
  echo "== live iptables-save =="
  iptables-save | awk '
    BEGIN { print_mode = 0 }
    /^\*filter$/ { print_mode = 1 }
    /^\*nat$/ { print_mode = 1 }
    /^\*raw$/ { print_mode = 0 }
    print_mode == 1 && ($0 ~ /ANDVARI/ || $0 ~ /3129/ || $0 ~ /3130/ || $0 ~ /172\.31\./) { print }
    /^\*nat$/ { next }
  '
}

apply_rules() {
  require_root
  require_cmd docker
  require_cmd iptables
  detect_network
  apply_filter_rules
  apply_nat_rules
}

inspect_rules() {
  require_root
  require_cmd docker
  require_cmd iptables-save
  detect_network
  print_inspect
}

action="${1:-apply}"
case "$action" in
  apply)
    apply_rules
    ;;
  inspect)
    inspect_rules
    ;;
  *)
    usage >&2
    exit 1
    ;;
esac
