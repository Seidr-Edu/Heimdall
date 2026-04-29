# Andvari Proxy Infra Guide

This document is the VPS-side contract for Heimdall's always-on Andvari proxy
path.

## Guarantee Boundary

Heimdall's repo-side changes do not, by themselves, prove that all outbound
network access goes through Squid. Heimdall can:

- place `andvari*` on the restricted Docker network
- inject proxy environment variables
- capture the Squid access-log slice after each step

The VPS still has to make that true in practice. The "all internet fetches go
through the proxy" guarantee exists only after host/network egress enforcement
is in place for the Andvari Docker subnet.

## What Heimdall Expects

- `andvari`, `andvari-v2`, and `andvari-v3` run on the configured
  `andvari_internal_network_name`.
- Those containers receive `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY`.
- The worker host exposes a readable Squid access log at
  `/var/log/squid/andvari-access.jsonl`.
- Heimdall treats that file as append-only and copies the per-step byte slice
  into each Andvari step's `artifacts/andvari/logs/proxy_access.jsonl`.

## Squid Requirements

- Keep Squid as the proxy implementation.
- Write an Andvari-dedicated JSONL access log to
  `/var/log/squid/andvari-access.jsonl`.
- Log both allowed and denied proxied requests.
- Any tool that uses HTTP(S) through Squid should appear here, including normal
  package-resolution traffic such as Maven or Gradle if that traffic actually
  honors the proxy and uses HTTP(S).
- Squid logs network requests, not shell command names. In practice you should
  expect to see the destination and request metadata, not a reliable label such
  as "`mvn` did this" unless the request headers or user agent make that clear.
- Each JSON object should include at least:
  - `timestamp`
  - `source_ip`
  - `method`
  - `url` or `connect_target`
  - `destination_host`
  - `destination_port`
  - `decision`
  - `status` or Squid result code
  - `bytes`

## Denylist

The proxy stays denylist-based. It must deny:

- `github.com`
- `api.github.com`
- `gist.github.com`
- `raw.githubusercontent.com`
- `codeload.github.com`
- `objects.githubusercontent.com`
- `*.githubusercontent.com`
- `*.githubassets.com`
- `ghcr.io`

Other proxied traffic may remain allowed.

This means package-resolution traffic is not blocked by policy just because it
comes from Maven or Gradle. If it uses Squid, it should be logged as allowed.

## Host Egress Enforcement

The proxy log alone is not enough. The host must prevent direct bypasses from
the Andvari Docker subnet.

- Allow the Andvari subnet to reach only the Squid listener.
- Block direct outbound TCP and UDP from that subnet.
- Block SSH bypasses such as `git@github.com`.
- Block direct DNS egress from that subnet.
- Keep Docker IPv6 disabled unless equivalent IPv6 enforcement is added.

If you also want visibility into blocked bypass attempts, add firewall logging
at this layer. Squid can only log traffic that actually reaches Squid.

## Validation Checklist

- `curl https://github.com` from Andvari fails through Squid and appears in the
  Squid log as denied.
- `git ls-remote https://github.com/...` from Andvari fails and appears in the
  Squid log as denied.
- `curl --noproxy '*' https://github.com` from Andvari fails before it can
  bypass Squid.
- A raw TCP connect to `github.com:443` from Andvari fails before it can bypass
  Squid.
- Non-denylisted proxied traffic appears in the Squid log as allowed.
- Maven or Gradle dependency traffic, if it uses HTTP(S) through Squid, appears
  in the Squid log as allowed.
