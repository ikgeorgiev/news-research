CEF NEWS: SINGLE-SERVER STAGING AND CUTOVER
Prepared 2026-11-08 Kyiv. No server has been purchased or deployed.
The user clarified that Hetzner is hypothetical and far in the future. This
preparation is parked; do not prioritize or activate it without a new request.

This is a separate deployment layout, not a replacement for the running Windows
application or its docker-compose.yml. It keeps one backend process and its
existing ingestion owner. Collection defaults OFF. Candidate research feeds,
Yahoo Search comparison and the Godel browser bridge are not automatically started.

SERVICES AND STORAGE

The proxy exposes 80/443, obtains TLS certificates for the configured hostname,
and protects the personal reader with Basic authentication. API and SSE share
the same origin. Admin, metrics and API documentation paths are not exposed.
PostgreSQL has no published port. Its network is internal. Application outbound
access uses the separate ingress network; the proxy has one explicitly trusted IP.
Do not reuse the default subnet if it overlaps a server/VPN/Docker network.
The automatic address pool excludes the fixed proxy IP to prevent an earlier
backend/frontend container from taking that address during startup.

postgres_data holds articles, raw records, feed state, page refusals, retry
deadlines, ticker assignments and subscriptions. news_state holds FinancialContent
and free-discovery SQLite state, controls/cursors/baselines, immutable reader
archives and timing logs. The frontend mounts only news_state/article-bodies
read-only. Its UID 1000 can read archives created with mode 0600 by the backend.
caddy_data/config persist certificates and proxy state. Keep all four volumes.
Never run 'down -v' against the real project.

Dockerfile-specific ignore files exclude .tmp research captures, environment
files, local databases, node_modules and virtual environments from build contexts.
Only the ticker registry is copied from data/. Runtime state is restored separately.
The Python image uses 3.13 to match local execution, with Linux timezone data.
The frontend build embeds PUBLIC_ORIGIN; changing the hostname requires rebuilding.

PREPARE WITHOUT ACTIVATING

1. Choose a Linux x86-64 host. A provisional 4-vCPU/8-GB machine with 80+ GB disk
   is a reasonable staging target; this is an estimate, not a measured minimum.
   Build images locally/CI if conserving server RAM. Record image digests after
   testing, and retain the previous images for rollback.
2. Set DNS for your hostname. Configure the Hetzner firewall with SSH limited
   to your administration addresses and HTTPS/HTTP as needed. Do not expose
   PostgreSQL, the backend, metrics, Grafana or the Godel receiver.
3. Install supported Docker Engine and the Compose plugin. Copy this repository
   snapshot and the exact ticker registry. Do not copy a populated .env into images.
4. Copy server.env.example to /etc/cef-news/server.env and application.env.example
   to /etc/cef-news/application.env. chmod 600 both files. Generate distinct random
   hexadecimal database/admin secrets. Generate the reader bcrypt hash with
   'docker run --rm -it caddy:2-alpine caddy hash-password'; enter the password at
   its prompt, not in shell history. Preserve single quotes around the hash.
5. Reconcile ALL application settings against the active local configuration;
   the example is deliberately small. Preserve source enables, request caps,
   Retry-After/refusals, the expired November 6 detailed-timing setting, and the
   November 9 Guggenheim enrollment. Do not reset controls for a new IP address.
6. Leave ENABLE_COLLECTION=false. Run:
     python3 deploy/hetzner/preflight.py --env /etc/cef-news/server.env
     docker compose --env-file /etc/cef-news/server.env -f deploy/hetzner/stack.yml build
   Preflight validates settings without printing secrets or starting containers.

MIGRATION REQUIRES A SEPARATELY APPROVED MAINTENANCE WINDOW

7. At cutover, verify the current Windows backend/reloader PIDs, creation times,
   commands, health and matching study/comparison workers. Stop only the verified
   ingestion owner under the user's maintenance authorization. Confirm it exited.
8. While that owner is stopped, produce a PostgreSQL custom-format dump using
   pg_dump 16 and copy the entire FinancialContent SQLite database plus reader
   archives and all independent comparison stores/controls. Use SQLite's backup
   API or a confirmed closed database; copying a live main file alone can miss WAL.
   Record checksums, row counts, tickers, refusals, cursor/baseline state and archive
   counts in a dated manifest. Keep original research archives separately intact.
9. Transfer the bundle over SSH. Start only db, restore the dump with pg_restore
   --exit-on-error into an empty target, and restore news_state with UID/GID 1000.
   Ensure its article-bodies subdirectory exists, including for an empty archive.
   Restore every database table, including source_page_controls and feed_poll_state.
   Reconcile manifest checksums/counts; verify the retained FC seed-review state.
10. Start backend/frontend/proxy with collection OFF. Verify /health internally,
    authenticated reader/API access, article links/bodies, ticker mapping and SSE.
    Test browser notifications on the real HTTPS origin. Old localhost browser
    push subscriptions do not establish delivery on the new origin.
11. Confirm there is ONE ingestion owner, the desktop owner is stopped, all
    controls survived and the real server restore passed. Only then change
    ENABLE_COLLECTION=true and recreate this server backend. Observe the first
    allowed weekday responses. New datacenter access refusals remain refusals.
    Do not change user agents, domains or paths to evade them.
12. Rollback: stop and verify the server owner first. Preserve its new state and
    records. Reconcile any server-only writes before resuming the desktop. Never
    run two independent collectors against divergent databases or queue copies.

BACKUPS AND OPERATIONS

Use database-aware pg_dump backups, SQLite backups, reader archives, control/
comparison stores and the settings/registry manifest. Encrypt copies, keep an
independent off-host copy, verify hashes and rehearse a restore on a separate
database with collection disabled. Do not treat a crash-consistent VM image as
proof that the PostgreSQL+SQLite+archive combination restores coherently.
Hetzner server Backups/Snapshots EXCLUDE attached Hetzner Volumes. If using a
Volume for Docker data, it needs its own backup path. This package does not yet
install a backup timer or claim a successful full production restore.

Local read-only audit: PostgreSQL was 3,268,615,191 bytes on November 7; the
ingestion_runs relation accounted for 3,161,726,976 bytes. The retained research
directory was about 2.96 GB. Keep those originals. Retention/compaction is a
separate task and is not performed by this deployment package.

The normal Python/Node services run without Codex/model usage. Research summaries
and desktop automation still depend on the desktop. Linux scheduling of the
bounded Yahoo comparison and OS-independent snapshot tooling remain separate
work; the Windows .ps1/CIM launcher is not a server scheduler.
Godel currently requires the user's open terminal tab and explicit Start/Stop.
A user-started SSH tunnel can carry its loopback receiver traffic to a server;
the receiver itself must stay private. This package does not create that tunnel
or claim unattended Godel access.

AUTHORITATIVE REFERENCES
https://docs.hetzner.com/cloud/servers/backups-snapshots/overview/
https://docs.hetzner.com/cloud/volumes/overview/
https://docs.hetzner.com/cloud/firewalls/overview/
https://docs.docker.com/build/building/context/#dockerignore-files
https://caddyserver.com/docs/caddyfile/directives/basic_auth
https://caddyserver.com/docs/caddyfile/directives/reverse_proxy
