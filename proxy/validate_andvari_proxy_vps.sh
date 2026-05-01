#!/usr/bin/env bash
set -euo pipefail

echo "== worker config =="
tail -n 5 /srv/pipeline/worker.yaml

echo
echo "== service state =="
systemctl --no-pager --full status \
  squid.service \
  ulogd2.service \
  andvari-blocked-egress-logger.service \
  andvari-egress-firewall.service \
  heimdall-worker.service || true

echo
echo "== squid build =="
squid -v | sed -n '1,20p'

echo
echo "== squid listeners =="
ss -ltnp | grep -E ':(3128|3129|3130)\s' || true

echo
echo "== squid block config =="
sed -n '1,140p' /etc/squid/conf.d/10-andvari-github-block.conf

echo
echo "== squid access log config =="
sed -n '1,80p' /etc/squid/conf.d/20-andvari-access-log.conf

echo
echo "== blocked egress ulogd config =="
sed -n '1,80p' /etc/andvari/blocked-egress-ulogd.conf

echo
echo "== worker override =="
sed -n '1,80p' /etc/systemd/system/heimdall-worker.service.d/override.conf

echo
echo "== squid ssl_db =="
find /var/spool/squid/ssl_db -maxdepth 2 -printf '%M %u %g %p\n'

echo
echo "== access logs =="
ls -l /var/log/squid/andvari-access.jsonl /var/log/andvari/blocked-egress.jsonl

echo
echo "== detected firewall values =="
/usr/local/sbin/andvari-egress-firewall inspect

echo
echo "== live iptables-save =="
iptables-save | sed -n '1,220p'

echo
echo "== live nft tables =="
nft list table ip filter || true
echo
nft list table ip nat || true

echo
echo "== next manual checks =="
echo "1. From an andvari-egress container with proxy env unset, verify curl https://example.com succeeds."
echo "2. Verify curl or git requests to GitHub are denied in /var/log/squid/andvari-access.jsonl."
echo "3. Verify Maven dependency resolution succeeds and shows allowed hosts in /var/log/squid/andvari-access.jsonl."
echo "4. Verify a raw TCP connect to github.com:22 or a direct DNS attempt fails and shows up in /var/log/andvari/blocked-egress.jsonl."
echo "5. Run Heimdall smoke-provider and then a real pipeline job after the runtime handoff removes proxy env injection."
