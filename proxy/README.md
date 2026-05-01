# Andvari Transparent Egress

This directory contains the worker-host assets for Andvari transparent egress:

- Squid config fragments for explicit `3128` plus transparent `3129/3130`
- a dynamic firewall applicator that targets the live Docker network instead of
  loading a frozen `iptables-save` snapshot
- a structured blocked-egress logger backed by `NFLOG` and `ulogd2`

## Purpose

`andvari*` containers should make normal outbound HTTP(S) connections while the
worker host forces those connections through Squid and blocks non-HTTP/S
bypasses. The resulting logs should make it possible to answer two questions:

- what HTTP/S destinations did the model actually try to reach?
- did it attempt blocked bypasses such as SSH or direct DNS?

## Runtime Shape

- `172.31.240.1:3128`
  Transitional explicit proxy listener. Keep this only while Heimdall still
  injects proxy env vars.
- `172.31.240.1:3129`
  Transparent HTTP intercept.
- `172.31.240.1:3130`
  Transparent HTTPS intercept using `ssl_bump` peek/splice with SNI-based
  GitHub blocking.
- `/var/log/squid/andvari-access.jsonl`
  Canonical allowed/denied HTTP/S request log.
- `/var/log/andvari/blocked-egress.jsonl`
  Structured blocked-bypass log from `NFLOG` via `ulogd2`.

## Prerequisites

- install `squid-openssl`, not the GnuTLS `squid` package
- create the intercept certificate:

```bash
cd /home/munin/Heimdall
sudo bash proxy/create_andvari_intercept_cert.sh
```

The apply script installs `ulogd2` and `ulogd2-json` automatically if they are
missing.

The distro `ulogd2.service` is not used for Andvari logging. The apply script
disables it and uses the dedicated `andvari-blocked-egress-logger.service`
instead.

## Applying

```bash
cd /home/munin/Heimdall
sudo bash proxy/create_andvari_intercept_cert.sh
sudo bash proxy/apply_andvari_proxy_vps_changes.sh
sudo bash proxy/validate_andvari_proxy_vps.sh
```

What the apply script does:

- installs the Squid config fragments
- initializes `/var/spool/squid/ssl_db` correctly for `security_file_certgen`
- installs `/usr/local/sbin/andvari-egress-firewall`
- installs and enables:
  - `andvari-egress-firewall.service`
  - `andvari-blocked-egress-logger.service`
- reloads or starts Squid
- restarts the blocked-egress logger, firewall service, and Heimdall worker

## Validation

From a container on `andvari-egress` with proxy env unset:

- `curl https://example.com` succeeds
- `curl https://github.com` is denied and appears in the Squid log
- `mvn ... dependency:get` succeeds and appears in the Squid log
- a raw TCP connect to `github.com:22` fails and appears in the blocked-egress
  JSONL log

Do not use `curl --noproxy` as the bypass test anymore. Transparent
interception will still catch `80/443`.
