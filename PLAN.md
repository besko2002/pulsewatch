# Pulsewatch — Plan

Multi-tenant SaaS for uptime and server monitoring with public status pages. Organizations
add monitors (HTTP(S) status/keyword, TCP port, SSL certificate expiry, heartbeat "dead man's
switch"). Checks run from several simulated regions. A failure confirmed by more than one
region opens an incident. Notifications go out by email, Slack, Discord and Telegram, with
retries and a DLT. Every organization gets a public status page that updates live. A small
Node.js agent pushes CPU/RAM/disk/load metrics that are checked against threshold rules, and
Stripe subscriptions enforce plan limits.

The goal is to show skills the other portfolio projects do not cover: Keycloak/OIDC,
multi-tenant SaaS design, TimescaleDB, distributed scheduling, compacted-topic read models,
non-blocking Kafka retries, live updates over SSE, Stripe *subscriptions*, SSRF-safe outbound
calls, and a Node/TypeScript CLI (full list at the end).

GitHub repo: `besko2002/pulsewatch`. Maven groupId: `io.github.besko2002.pulsewatch`.

## Architecture

```
  Browser SPA (React)        Public visitors         Servers (agent)        Cron jobs
  OIDC code+PKCE             /status/:slug           API key                ping URL
       │   ▲ login                │                       │                     │
       │   │                      │                       │                     │
  ┌────┴───┴───┐                  │                       │                     │
  │  Keycloak  │◄──── JWKS ───┐   │                       │                     │
  └────────────┘              │   ▼                       ▼                     ▼
                     ┌────────┴──────────────────────────────────────────────────────┐
  Redis ◄────────────┤ api-gateway: JWT check, per-tenant rate limit, routing, SSE pass │
  (rate limit,       └──┬───────────────┬──────────────────┬──────────────────┬───────┘
   page cache,          │               │                  │                  │
   pub/sub)      ┌──────▼───────┐ ┌─────▼────────┐ ┌───────▼───────┐ ┌────────▼───────┐
      ▲          │monitor-service│ │telemetry-svc │ │incident-svc   │ │notification-svc│
      │          │ control plane │ │ Timescale:   │ │ region quorum │ │ email (MailHog)│
      │          │ orgs, monitors│ │ results,     │ │ state machine │ │ Slack, Discord,│
      │  Stripe ─► status pages, │ │ host metrics,│ │ incidents     │ │ Telegram       │
      │ webhooks │ API keys,     │ │ ingest, pings│ │ public status │ │ retry + DLT    │
      │          │ billing       │ │ live SSE     │ │ pages + SSE   │ │                │
      │          └──────┬────────┘ └──▲───┬───────┘ └──▲─────┬──────┘ └──▲─────┬───────┘
      │           outbox│             │   │outbox      │     │outbox     │     │outbox
  ════│═════════════════▼═════════════╪═══▼════════════╪═════▼═══════════╪═════▼══════ Kafka
      │   pw.config.* (compacted)   pw.checks.results  pw.host.alerts  pw.incidents.events
      │   orgs/monitors/statuspages/  pw.host.metrics                  pw.notifications.requests
      │   apikeys                                                      (+ retry topics, -dlt)
      │                 │ (consumer group per region; partition = ownership)
      │     ┌───────────▼──────┬──────────────────┬──────────────────┐
      │     │ checker-service  │ checker-service  │ checker-service  │  stateless workers,
      │     │ region=eu-central│ region=us-east   │ region=ap-south  │  N replicas per region
      │     └───────────┬──────┴────────┬─────────┴────────┬─────────┘
      │                 ▼ HTTP / TCP / TLS (SSRF-guarded)   ▼
      │           customer endpoints
      └── live events (telemetry + incident publish, every instance's SSE hub subscribes)
```

- **Database per service.** One Postgres 16 + TimescaleDB container with separate databases
  and owners: `monitor_db`, `telemetry_db` (Timescale extension), `incident_db`,
  `notification_db`, and `keycloak_db` for Keycloak. No service reads another service's tables.
- **Synchronous REST only at the edge,** through the gateway. Services talk to each other
  only through Kafka.
- **Event-carried state transfer.** monitor-service owns all tenant configuration and
  publishes full snapshots to compacted topics. The other services keep local read replicas,
  so checks and incident evaluation keep running while monitor-service is down. That matters
  for a monitoring product.
- **Kafka is for durable facts; Redis pub/sub is for short-lived live fan-out** to SSE
  connections on any service instance.

### Main flows

1. **Monitor lifecycle.** `POST /api/monitors` writes the monitor and an outbox row in one
   transaction. The relay publishes a `MonitorConfig` snapshot to `pw.config.monitors`, keyed by
   monitorId. Checkers, telemetry and incident-service update their state. A delete publishes
   a tombstone.
2. **Check.** The checker worker that owns the monitor's partition runs it at the monitor's
   deterministic slot and publishes a `CheckResult` to `pw.checks.results`, keyed by monitorId.
3. **Store and evaluate.** telemetry-service stores the result in the `check_results`
   hypertable and pushes a live event. incident-service updates the per-region state, applies
   the quorum rule, and may open or resolve an incident (outbox → `pw.incidents.events`).
4. **Notify.** notification-service fans out each incident event into one
   `NotificationRequested` per enabled channel and delivers it, with non-blocking retries and
   a DLT.
5. **Status page.** incident-service invalidates the Redis-cached page and pushes the change
   to SSE subscribers.
6. **Agent.** The agent calls `POST /api/ingest/v1/metrics` with an API key. telemetry-service
   validates the request, publishes to `pw.host.metrics` (keyed by hostId), and the consumer
   stores the samples, evaluates rules, and may emit `pw.host.alerts`. Those alerts become
   incidents through step 3, and their notifications go out through step 4.
7. **Billing.** Stripe Checkout (subscription mode) leads to webhooks. monitor-service changes
   the plan, applies downgrade rules, and publishes an `OrgSnapshot` with the new limits.

## Design decisions

The description implies more services than this plan builds (scheduler, billing, status page,
ingest, realtime, auth). The plan builds **six backend services**, chosen as follows:

| Decision | Why |
|---|---|
| **Billing merged into monitor-service** (the "control plane") | Plan limits must be enforced *transactionally* with monitor creation: lock the org row, count, insert. A separate billing service would make limit checks either eventually consistent (over-provisioning races) or a synchronous call on every create. Stripe code stays in its own `billing` package. |
| **Status page *config* in monitor-service, status page *serving* in incident-service** | All tenant CRUD and plan limits live in one place. Serving needs the live monitor status and incidents, which incident-service already owns, so the public page is a read model there. No cross-service query at request time. |
| **One telemetry-service for every Timescale write** (check results, host metrics, heartbeat pings, agent ingest, host alert rules) | One database with the Timescale extension, one owner of the hypertables and continuous aggregates. The ingest HTTP edge is a thin layer in front of Kafka in the same service. |
| **No scheduler service.** Scheduling lives in the checker workers through Kafka partition ownership | A central scheduler is a single point of failure and a throughput bottleneck. The consumer group already provides membership, failover and partitioning. See "Check scheduling". |
| **checker-service has no database** | Its state is derived from a compacted topic. Workers are disposable and scale horizontally. |
| **No realtime/websocket service** | An `SseHub` in `service-support` (Redis pub/sub fan-out) is reused by telemetry (org dashboard stream) and incident-service (public status stream). A service that only relays messages adds a network hop and nothing else. |
| **No auth-service** | Keycloak issues every token. Services are OAuth2 resource servers only. |
| **SSE, not WebSocket** | All updates go server→client. SSE is plain HTTP through the gateway, reconnects automatically, and is trivial to test with MockMvc. The dashboard uses `@microsoft/fetch-event-source` so it can send the bearer token. The public page uses native `EventSource`. |
| **JSON events with `schemaVersion`, no Avro/Schema Registry** | One less container. Additive-only evolution plus `FAIL_ON_UNKNOWN_PROPERTIES=false` is enough for a single repo. |
| **Hibernate `@TenantId` plus explicit `org_id` in JDBC; Postgres RLS deferred** | `@TenantId` filters every JPA query automatically. Native/JDBC queries (Timescale) go through a helper that requires the orgId. RLS needs `SET LOCAL` per transaction with pooled connections; listed as an open question. |
| **Status page uptime % comes from incidents, not raw checks** | Public uptime counts confirmed outages only, which is industry practice. It also needs no cross-service call. The private dashboard shows raw-check uptime from Timescale. |
| **Regions are simulated** by separate checker deployments with a region label and optional latency/fault injection | Honest about running on one host. Fault injection is what demonstrates false-positive suppression. |
| **API key secrets hashed with SHA-256, not bcrypt** | The secret is 256 random bits, so brute force is infeasible. Agents authenticate every 10 s, so bcrypt would burn CPU for nothing. Passwords are Keycloak's job. |
| **Outbound HTTP uses Apache HttpClient 5,** not the JDK `HttpClient` | Its pluggable `DnsResolver` lets the SSRF guard filter resolved IPs, including on redirects and against DNS rebinding. The JDK client cannot do that. |

Patterns ported from `order-processing-microservices` (Transactional Outbox, idempotent
consumers with `processed_events`, correlation id, `ApiError` model) are reimplemented in
`service-support`. They are not what this project showcases.

## Services

| Service | Responsibility | Datastore | Key patterns |
|---|---|---|---|
| api-gateway | Single entry point, routing, JWT validation (Keycloak JWKS), per-tenant/per-key/per-IP rate limiting, correlation id, CORS, SSE pass-through, strips spoofed tenant headers | Redis | Spring Cloud Gateway (WebFlux), `RedisRateLimiter` + custom `KeyResolver` |
| monitor-service | Control plane: orgs and onboarding, monitors (4 types), status page config, API keys, Stripe billing, plan limits | Postgres `monitor_db` | Hibernate `@TenantId`, Transactional Outbox → compacted topics, pessimistic org-row lock for limits, Stripe webhooks (idempotent, out-of-order safe), Keycloak Admin API |
| checker-service | Executes HTTP/TCP/SSL checks for the monitors it owns, in its region | none (in-memory from `pw.config.monitors`) | Consumer group per region = work partitioning, deterministic slots, virtual threads, SSRF-safe DNS, per-host concurrency limits, fault injection |
| telemetry-service | Stores check results and host metrics, query/rollup APIs, agent ingest (API key auth), heartbeat pings and deadlines, host alert rules, host offline detection, org live SSE stream | Postgres+Timescale `telemetry_db`, Redis pub/sub | Hypertables, compression, continuous aggregates (hierarchical), retention policies, batch idempotent inserts, `SKIP LOCKED` deadline sweepers, compacted-topic replicas |
| incident-service | Per-region state, quorum confirmation, monitor status, incident state machine with flap handling, incidents API, public status pages | Postgres `incident_db`, Redis (page cache, pub/sub) | Pure-domain state machine, partitioned-by-monitor serial processing, partial unique index "one open incident per subject", cache-aside + ETag, SSE |
| notification-service | Channels (encrypted secrets), fan-out, delivery via email/Slack/Discord/Telegram, delivery log, DLT replay | Postgres `notification_db` | Deterministic notification ids, `@RetryableTopic` non-blocking retries + DLT, AES-256-GCM secrets, outbound host allow-list |
| frontend | Dashboard (live), monitor/host charts, incidents, settings, billing, public status page | — | React + TS + Vite, OIDC PKCE, TanStack Query, SSE snapshot-then-deltas |
| agent | Collects host metrics and pushes them with an API key | local state file | Node 22 / TS, zero runtime deps, buffering, backoff with jitter, gzip |

Shared Maven modules: `common-events` (event records, topic names, enums) and
`service-support` (tenancy, security, outbox, idempotency, errors, correlation id, live SSE
hub; Spring Boot auto-configurations, mirroring the sibling project).

## Kafka topics

All topics are declared explicitly as `NewTopic` beans by the owning (producing) service.
Auto-create is disabled in compose, because compacted topics need explicit config.

| Topic | Key | Cleanup / retention | Partitions | Producer | Consumers |
|---|---|---|---|---|---|
| `pw.config.orgs` | orgId | compact | 6 | monitor | telemetry, incident |
| `pw.config.monitors` | monitorId | compact | 12 | monitor | checker (group `checker-{region}`), telemetry, incident |
| `pw.config.statuspages` | pageId | compact | 3 | monitor | incident |
| `pw.config.apikeys` | keyId | compact | 3 | monitor | telemetry |
| `pw.checks.results` | monitorId | delete, 3 d | 12 | checker, telemetry (heartbeat results) | telemetry (store), incident (evaluate) |
| `pw.host.metrics` | hostId | delete, 1 d | 6 | telemetry (ingest edge) | telemetry (store + rules) |
| `pw.host.alerts` | hostId | delete, 7 d | 6 | telemetry | incident |
| `pw.incidents.events` | subjectKey | delete, 7 d | 6 | incident | notification |
| `pw.notifications.requests` (+ `-retry-N`, `-dlt`) | channelId | delete, 7 d (DLT 30 d) | 6 | notification | notification |

Twelve partitions on `pw.config.monitors` caps a region at 12 workers. Raising it later
reshuffles ownership, which is safe because state is rebuilt on assignment.

Headers on every record: `pw-event-type`, `pw-org-id`, `X-Correlation-Id`, and W3C
`traceparent` (added by Micrometer observation). Tombstones (null value) delete state in
every replica.

### Event schemas (`common-events`, Java records serialized as JSON)

```java
record EventEnvelope<T>(UUID eventId, String eventType, int schemaVersion, Instant occurredAt,
                        UUID orgId, String correlationId, T payload)

// ---- config snapshots (compacted; whole state, last write wins by `version`)
record OrgSnapshot(UUID orgId, String name, String slug, Plan plan, PlanLimits limits,
                   OrgStatus status, long version)
record PlanLimits(int maxMonitors, int minIntervalSeconds, int maxStatusPages, int maxHosts,
                  int minPushIntervalSeconds)
record MonitorConfig(UUID monitorId, UUID orgId, String name, MonitorType type,
                     int intervalSeconds, int timeoutMs, List<String> regions,
                     int confirmRegions, int failureThreshold, int recoveryThreshold,
                     boolean paused, MonitorSettings settings, long version)
sealed interface MonitorSettings permits HttpSettings, TcpSettings, SslSettings, HeartbeatSettings
record HttpSettings(URI url, String method, Set<Integer> expectedStatuses, String keyword,
                    KeywordMode keywordMode /*PRESENT, ABSENT*/, boolean followRedirects,
                    Map<String,String> headers)
record TcpSettings(String host, int port)
record SslSettings(String host, int port, int warnDays)
record HeartbeatSettings(String token, int periodSeconds, int graceSeconds)
record StatusPageConfig(UUID pageId, UUID orgId, String slug, String title, String description,
                        boolean published, List<PageMonitor> monitors, long version)
record PageMonitor(UUID monitorId, String displayName, String group, int sortOrder)
record ApiKeySnapshot(UUID keyId, UUID orgId, String prefix, String secretSha256,
                      Set<String> scopes, Instant revokedAt, long version)

// ---- facts
record CheckResult(UUID monitorId, UUID orgId, String region, Instant slot, Instant startedAt,
                   int durationMs, Outcome outcome /*UP, DEGRADED, DOWN*/, Integer httpStatus,
                   ErrorCode errorCode /*TIMEOUT, DNS, CONNECT_REFUSED, TLS_INVALID,
                   TLS_EXPIRING, STATUS_MISMATCH, KEYWORD_MISMATCH, BLOCKED_TARGET,
                   HEARTBEAT_MISSED, HEARTBEAT_FAIL_SIGNAL, SIMULATED_FAULT, INTERNAL*/,
                   String errorMessage, Instant tlsExpiresAt, String checkerInstance)
record HostMetricsBatch(UUID hostId, UUID orgId, HostInfo host, List<MetricSample> samples,
                        Instant receivedAt)
record HostAlert(UUID orgId, UUID hostId, String hostname, UUID ruleId /*null for OFFLINE*/,
                 HostAlertKind kind /*THRESHOLD, OFFLINE*/, AlertState state /*FIRING, RESOLVED*/,
                 String metric, Double value, Double threshold, Instant at)
record IncidentEvent(UUID incidentId, UUID orgId, String subjectKey, UUID monitorId,
                     UUID hostId, String subjectName, IncidentKind kind /*MONITOR_DOWN,
                     SSL_EXPIRING, HEARTBEAT_MISSED, HOST_THRESHOLD, HOST_OFFLINE*/,
                     Severity severity /*WARNING, CRITICAL*/,
                     IncidentTransition transition /*OPENED, ACKNOWLEDGED, REOPENED, RESOLVED*/,
                     IncidentState state, Instant openedAt, Instant at, String cause,
                     List<String> failingRegions, int flapCount)
record NotificationRequested(UUID notificationId, UUID orgId, UUID channelId,
                             ChannelType channelType, UUID incidentId,
                             IncidentTransition transition, NotificationContent content)
```

Evolution rule: only add fields. Bump `schemaVersion` on semantic changes. Consumers ignore
unknown fields.

### Redis live-event contract (not Kafka: ephemeral, may be lost)

Channels `pw:live:{orgId}` (private dashboard) and `pw:status:{orgId}` (public page changes).
Payload: `{type: "check.result" | "monitor.status" | "incident" | "host.sample", orgId, at, data}`.

## Core data model

All tenant-owned tables carry `org_id uuid NOT NULL` and are indexed by it. Optimistic
locking (`version`) is used on mutable aggregates.

**monitor-service (`monitor_db`)**
- `organizations(id, name, slug UNIQUE, status [PENDING_OWNER|ACTIVE], created_by_sub, plan
  [FREE|PRO], subscription_status, stripe_customer_id UNIQUE, stripe_subscription_id,
  current_period_end, plan_changed_at, plan_event_created_at, created_at, version)`
- `monitors(id, org_id, name, type, interval_seconds, timeout_ms, regions text[],
  confirm_regions, failure_threshold, recovery_threshold, settings jsonb, heartbeat_token
  UNIQUE NULL, paused, paused_reason [USER|PLAN_LIMIT], created_at, updated_at, version)`
- `status_pages(id, org_id, slug UNIQUE, title, description, published, created_at, version)`,
  `status_page_monitors(page_id, monitor_id, display_name, group_name, sort_order)`
- `api_keys(id, org_id, name, prefix UNIQUE, secret_sha256, scopes text[], created_by_sub,
  created_at, revoked_at)`
- `stripe_events(event_id PK, type, created_at, processed_at)`
- `outbox(...)` (same shape as the sibling project)

**checker-service.** No tables. It holds `Map<monitorId, MonitorConfig>` per owned partition
and a slot timer queue.

**telemetry-service (`telemetry_db`, TimescaleDB)**
- `check_results(time = slot timestamptz, monitor_id, org_id, region, outcome, latency_ms,
  http_status, error_code, error_message, tls_expires_at, checker_instance)`. **Hypertable**
  with 1-day chunks, `UNIQUE (monitor_id, region, time)` for idempotency, index
  `(org_id, monitor_id, time DESC)`. Compression after 7 d (`segmentby monitor_id, region`),
  retention 30 d.
- `check_results_1h`: **continuous aggregate** (total, up, degraded, avg/max latency per
  monitor, region, hour), `materialized_only=false`, retention 400 d.
- `check_results_1d`: **hierarchical continuous aggregate** on top of 1h, kept forever.
- `host_metrics(time, host_id, org_id, cpu_pct, mem_used, mem_total, swap_used, load1, load5,
  load15)`. **Hypertable** with `UNIQUE (host_id, time)`, compression after 2 d, retention 7 d.
- `host_disk_metrics(time, host_id, org_id, mount, used_bytes, total_bytes)`. **Hypertable**,
  same policies.
- `host_metrics_1m` (continuous aggregate, retention 30 d) and `host_metrics_1h`
  (hierarchical, retention 400 d) with avg/max per metric.
- `hosts(id = agent host id, org_id, hostname, os, arch, cpu_count, agent_version,
  first_seen_at, last_seen_at, push_interval_s, status [ONLINE|OFFLINE], next_offline_deadline)`
- `heartbeat_state(monitor_id PK, org_id, period_s, grace_s, last_ping_at, next_deadline,
  state)`, indexed on `next_deadline`
- `alert_rules(id, org_id, host_id NULL = all hosts, metric [CPU_PCT|MEM_PCT|DISK_PCT|LOAD1],
  operator, threshold, for_seconds, enabled, version)`;
  `alert_rule_state(rule_id, host_id, state [OK|PENDING|FIRING], since, last_value, PK(rule_id, host_id))`
- Replicas: `monitor_replica`, `api_key_replica`, `org_entitlements`; plus `outbox` and
  `processed_events`.

**incident-service (`incident_db`)**
- `monitor_replica(monitor_id PK, org_id, name, type, interval_s, regions, confirm_regions,
  failure_threshold, recovery_threshold, paused, deleted, version)`
- `monitor_region_state(monitor_id, region, last_slot, last_outcome, consecutive_failures,
  consecutive_successes, region_status [UP|DOWN], last_error, updated_at, PK(monitor_id, region))`
- `monitor_status(monitor_id PK, org_id, status [UNKNOWN|UP|SUSPECT|DEGRADED|DOWN|PAUSED],
  since, current_incident_id, version)`
- `incidents(id, org_id, subject_key, monitor_id NULL, host_id NULL, kind, severity, state
  [OPEN|ACKNOWLEDGED|RESOLVED], opened_at, acknowledged_at, acknowledged_by, resolved_at,
  flap_count, last_notified_at, cause jsonb, version)` with **`UNIQUE (subject_key) WHERE
  state <> 'RESOLVED'`** (one open incident per subject, enforced by the database)
- `incident_timeline(id, incident_id, org_id, at, type, message, region)`
- `status_page_replica(page_id PK, org_id, slug UNIQUE, title, description, published,
  monitors jsonb, version)`; plus `outbox` and `processed_events`

**notification-service (`notification_db`)**
- `channels(id, org_id, type [EMAIL|SLACK|DISCORD|TELEGRAM], name, config_ciphertext,
  config_nonce, key_id, masked_target, min_severity, enabled, created_at, version)`
- `deliveries(id = notificationId PK, org_id, channel_id, incident_id, transition, status
  [PENDING|SENT|RETRYING|FAILED], attempts, last_error, created_at, sent_at)`
- `outbox`, `processed_events`

## Multi-tenancy strategy

Shared database and shared schema per service. The discriminator is `org_id`.

1. **Identity.** Keycloak puts `org_id` (a user attribute exposed by a protocol mapper) and the
   `pulsewatch-api` client roles into the access token. The tenant is **only ever** read from
   the validated token. Path, query, body and headers are never trusted for it, and the
   gateway strips any incoming `X-Org-Id`.
2. **JPA.** Tenant entities use Hibernate `@TenantId UUID orgId`. A
   `CurrentTenantIdentifierResolver` reads `TenantContext` (filled from the JWT). Hibernate
   adds `org_id = ?` to every entity query and sets it on insert. Kafka consumers and
   schedulers run in `TenantContext.system()`, which `isRoot()` treats as unfiltered. That is
   the only bypass, and it is greppable.
3. **JDBC (Timescale queries).** All native SQL goes through `TenantJdbc`. Its methods take an
   explicit `OrgId` value type, so a query without a tenant does not compile.
4. **Cross-tenant access returns 404, not 403,** so resource existence is not leaked.
5. **Kafka.** `orgId` is in the envelope and in the `pw-org-id` header. Consumers store it as
   data. Only internal services produce events.
6. **Live channels and caches** are keyed by orgId. The SSE hub delivers an event only to
   emitters registered for that org.

**How isolation is tested**
- `EndpointInventoryTest` (local, `@WebMvcTest`) enumerates `RequestMappingHandlerMapping`. It
  fails if a handler is neither covered by the service's isolation matrix nor annotated
  `@PublicEndpoint`, so new endpoints cannot skip isolation tests.
- A reflection/ArchUnit test (local) fails if a controller parameter is named
  `orgId`/`tenantId`, or if `JdbcTemplate` is used outside `TenantJdbc`.
- `TenantIsolationIntegrationTest` per service (CI, Testcontainers): org A creates resources.
  For every endpoint in the matrix, org B gets 404 on read, update and delete, and A's
  resources are missing from B's lists, series and live stream.
- The e2e smoke script (Phase 22) repeats this through the gateway with real Keycloak tokens
  for `alice@acme.test` and `bob@globex.test`.

## Check scheduling for scale

**Requirements:** every monitor runs once per interval *per region*. Adding workers spreads
the load. A dead worker's monitors move automatically. No region double-executes a slot in a
way that changes results.

**Design: Kafka partition ownership.**
- Each region is a consumer group `checker-{region}` on the compacted `pw.config.monitors`
  topic. Kafka assigns each partition to exactly one live member of the group, so **owning a
  partition means owning (running) its monitors.** Different regions are different groups,
  so every region gets every monitor.
- On `onPartitionsAssigned` the worker seeks to the beginning and replays the compacted
  partition into memory, then keeps consuming live updates. Offsets are never committed. On
  `onPartitionsRevoked` it cancels those monitors' timers.
- Settings: `CooperativeStickyAssignor` (minimal movement) and static membership
  (`group.instance.id = ${HOSTNAME}`), so a restart within `session.timeout.ms` causes no
  rebalance.
- **Deterministic slots.** With interval `I` and `phase = hash(monitorId) mod I`,
  `slot(t) = floor((t − phase) / I) · I + phase`. The hashed phase spreads load (no
  thundering herd at :00). Regions are staggered by `regionIndex · I / regionCount`, so a
  second region confirms an outage about `I/3` after the first notices it, instead of a full
  interval later.
- **Execution.** One timer thread pops due slots and hands them to a virtual-thread executor.
  A global semaphore caps in-flight checks, and a per-target-host semaphore (default 4)
  avoids hammering one origin. Timeout is `min(timeoutMs, I/2)`. After a rebalance the new
  owner runs only the most recent missed slot, never a backlog.

**Why not the alternatives**

| Option | Verdict |
|---|---|
| DB polling with `SELECT … FOR UPDATE SKIP LOCKED` on `next_run_at` | Simple and correct, but every check is a DB write (`next_run_at` update). 50 k monitors × 3 regions / 30 s = 5 k writes/s for scheduling alone. It would also give checkers a database. **Used where the rate is low:** heartbeat deadlines and host-offline detection. |
| Leader (ShedLock) dispatching jobs into a work topic | Leader is a throughput bottleneck and a failover delay. Adds an extra hop and topic. |
| Redis lock per monitor per slot | Lock churn equal to the check rate. Correctness depends on lock TTL versus check duration. |
| Quartz clustered | Row-lock contention on its tables at high trigger rates. |
| Hand-rolled consistent hashing | Reimplements membership and failure detection that the consumer group already provides. |

**Double-execution guarantee.** A partition has one owner. A zombie owner (for example a long
GC pause past the session timeout) can still run a slot the new owner also runs. Both results
carry the same `(monitorId, region, slot)`. telemetry dedupes with
`ON CONFLICT DO NOTHING` on the hypertable's unique key. incident-service ignores any result
whose `slot <= last_slot` for that region. The result is **at-least-once execution with an
exactly-once effect per slot.**

**Scale math.** 100 k monitors × ~500 B of config = ~50 MB over 12 partitions, so a full
replay on assignment takes seconds. Check throughput scales linearly with workers, up to 12
per region.

## Incident state machine and false-positive suppression

Results for one monitor share a partition (key = monitorId), so incident-service processes a
monitor's results **serially**. No in-process locks are needed. `@Version` is the safety net.

**Per region (with hysteresis):**
- `region_status` becomes DOWN when `consecutive_failures ≥ failureThreshold` (default 2).
- It returns to UP when `consecutive_successes ≥ recoveryThreshold` (default 2).
- A region is **fresh** if `last_slot ≥ now − 2.5 · interval`.

**Per monitor:**
- `quorum = min(confirmRegions (default 2), freshRegions)`. When fewer regions than
  `confirmRegions` are fresh, the monitor runs in degraded mode: the remaining fresh regions
  are enough, and the timeline records `confirmation=DEGRADED`.
- If there are zero fresh regions, the status is `UNKNOWN`. That raises the internal checker
  lag metric, not a customer incident.

```
            ≥1 region DOWN, < quorum                 ≥ quorum regions DOWN
   UP ───────────────────────────────► SUSPECT ───────────────────────────► DOWN ──► opens incident
   ▲  ◄────────────── all recovered ────┘                                   │
   └──────────────────── fewer than quorum regions DOWN ────────────────────┘  ──► resolves incident
   SSL days_left ≤ warnDays: UP ⇄ DEGRADED (quorum 1, WARNING incident SSL_EXPIRING)
   paused/deleted: any ──► PAUSED (open incident resolved, reason "paused")

   Incident:  OPEN ──ack──► ACKNOWLEDGED ──recover──► RESOLVED
              OPEN ─────────────recover──────────────► RESOLVED
              RESOLVED ──DOWN again within reopenWindow (10 min)──► OPEN (flap_count++, REOPENED)
```

**False-positive suppression, in layers:**
1. Consecutive-failure threshold per region (absorbs single blips).
2. Multi-region quorum (absorbs one region's network problems; demonstrated with checker
   fault injection).
3. Stale regions are ignored rather than counted as failures.
4. Recovery hysteresis (no UP/DOWN chatter around a threshold).
5. Flap handling: a reopen within 10 minutes reuses the same incident. After 3 reopens in an
   hour, one "flapping" notification is sent and further reopen notifications are throttled
   (`last_notified_at`).
6. SSL and heartbeat checks are region-independent, so their quorum is 1.

Duplicate or out-of-order results (`slot <= last_slot`) are no-ops, which gives idempotency
without `processed_events` on this hot path.

## Notification reliability

- **Fan-out.** One consumer of `pw.incidents.events` writes `deliveries` rows plus outbox rows
  in one transaction. `notificationId = UUIDv5(incidentId, transition, flapCount, channelId)`
  is deterministic and unique, so a redelivered incident event cannot create a second
  delivery.
- **Per-channel delivery.** Each delivery is its own Kafka message keyed by channelId. A
  failing Slack webhook never delays or re-sends email.
- **Non-blocking retries.** `@RetryableTopic(attempts = 5, backoff 10 s × 3, max 10 min)`
  moves messages through retry topics instead of blocking the partition.
  - Retryable: 5xx, timeouts, 429 (Telegram `retry_after` and `Retry-After` are honoured,
    capped).
  - Not retryable: other 4xx, which go straight to `-dlt`.
- **DLT.** The `@DltHandler` marks the delivery `FAILED`, records the error, and increments
  `pulsewatch_notifications_dlt_total`. `POST /api/notifications/deliveries/{id}/retry`
  re-publishes it, and the UI shows a "retry" button.
- **Before sending,** the consumer skips deliveries already `SENT` (redelivery after a
  successful send).
- **Honest limit:** a crash *between* the HTTP call and the DB commit can duplicate a
  Slack/Discord/Telegram message, because those APIs have no idempotency keys. This
  at-least-once behaviour is documented.
- **Outbound safety.** Webhook hosts are on an allow-list (`hooks.slack.com`, `discord.com`,
  `discordapp.com`, `api.telegram.org`; overridable in test profiles for WireMock). Channel
  secrets are encrypted at rest with AES-256-GCM (`PULSEWATCH_SECRETS_KEY`, with key id for
  rotation) and masked in API responses.

## Status page: live updates and caching

- **Read model** in incident-service: `status_page_replica`, `monitor_status`, `incidents`.
- **Public payload** `GET /api/public/status/{slug}` returns `{page, overall
  [OPERATIONAL|DEGRADED|PARTIAL_OUTAGE|MAJOR_OUTAGE], monitors[{displayName, group, status,
  uptime90d[{day, uptimePct, downtimeMin}]}], activeIncidents, pastIncidents(14 d),
  generatedAt}`.
  - Data minimization: no URLs, hosts, error messages or region details. A test asserts this.
  - Uptime per UTC day = 1 − (overlap of confirmed `MONITOR_DOWN` incidents with the day) / day.
- **Caching:**
  - Redis cache-aside, key `pw:sp:{slug}`, TTL 60 s with jitter.
  - `ETag` (hash of the JSON) with `If-None-Match` → 304, and
    `Cache-Control: public, max-age=15, stale-while-revalidate=30`.
  - The gateway rate-limits public routes per IP.
- **Invalidation.** When a monitor's status or incident changes, incident-service finds the
  pages containing that monitor (reverse lookup in `status_page_replica`), deletes their
  cache keys, and publishes `{pageIds}` on `pw:status:{orgId}`.
- **Live.** `GET /api/public/status/{slug}/events` (SSE). Every instance's `SseHub`
  subscribes to Redis, debounces per page (1 s), rebuilds the payload once (cache fill) and
  pushes `event: status`. Clients use **snapshot then deltas:** on (re)connect they fetch the
  REST snapshot, because Redis pub/sub does not replay. A comment heartbeat every 15 s keeps
  proxies open. Emitters time out after 30 min and clients reconnect.
- **Dashboard stream** `GET /api/live/stream` (telemetry, authenticated) uses the same hub on
  `pw:live:{orgId}`. It carries check results, monitor status changes, incidents and host
  samples. A per-org connection limit protects the hub.

## Agent protocol

- **Endpoint.** `POST /api/ingest/v1/metrics`.
  - Headers: `Authorization: Bearer pw_live_<prefix8>_<secret43>`,
    `Content-Type: application/json`, optional `Content-Encoding: gzip`,
    `User-Agent: pulsewatch-agent/<ver> (<os>; <arch>)`.
- **Body:**
  ```json
  { "agentVersion": "0.1.0",
    "host": { "hostId": "uuid (generated once, persisted)", "hostname": "web-1",
              "os": "linux", "platform": "ubuntu 24.04", "arch": "x64", "cpuCount": 4,
              "bootTime": "2025-01-01T00:00:00Z" },
    "samples": [ { "ts": "…", "cpuPct": 12.5, "mem": {"used": 0, "total": 0, "swapUsed": 0},
                   "load": [0.3, 0.2, 0.1], "disks": [{"mount": "/", "used": 0, "total": 0}] } ] }
  ```
  Limits: at most 120 samples per batch, 256 KB per request, 32 disks per sample. `ts` must be
  within `[now − 1 h, now + 5 min]` and percentages within 0–100.
- **Responses** and how the agent reacts:

  | Response | Agent behaviour |
  |---|---|
  | `202 {accepted, rejected, nextPushIntervalSeconds}` | Success. The server-driven interval enforces the plan's minimum. |
  | `400` | Drop the batch and log it. |
  | `401` | Invalid or revoked key: log loudly, retry every 5 min. |
  | `403 HOST_LIMIT_REACHED` | Back off 10 min. |
  | `413` | Split the batch. |
  | `429` | Honour `Retry-After`. |
  | `5xx` / network error | Exponential backoff with full jitter, 1 s → 5 min. |

  `GET /api/ingest/v1/whoami` returns `{orgName, keyName, hostsUsed, hostsLimit}` for
  `agent check`.
- **Idempotency.** Samples are unique on `(host_id, time)` with `ON CONFLICT DO NOTHING`, so
  retries are safe.
- **Agent internals:**
  - Ring buffer of up to 1 h of samples; the oldest are dropped when full.
  - Flush on SIGTERM.
  - Config precedence: flags > env (`PULSEWATCH_API_KEY`, `PULSEWATCH_ENDPOINT`,
    `PULSEWATCH_INTERVAL`) > `~/.config/pulsewatch/agent.json` (mode 0600).
  - Host id is stored in a state file.
  - Collectors:
    - CPU % from `os.cpus()` deltas.
    - Memory from Linux `/proc/meminfo` `MemAvailable` (`os.freemem` fallback).
    - `os.loadavg()`, reported as null on Windows.
    - Disks from `fs.promises.statfs` for configured mounts (default `/`).
  - **Zero runtime dependencies:** `node:util` `parseArgs`, global `fetch`, `node:zlib`.
- **CLI:** `pulsewatch-agent run | once [--json] | check | --version`.
- **Heartbeat (cron) protocol.** `GET|POST /api/ping/{token}` records success.
  `/api/ping/{token}/fail` records an explicit failure. Tokens are 22-character random
  capability URLs, rate-limited per IP at the gateway.

## Plan limits and Stripe

| Limit | FREE | PRO |
|---|---|---|
| Monitors | 5 | 50 |
| Minimum check interval | 300 s | 30 s |
| Status pages | 1 | 5 |
| Agent hosts | 1 | 20 |
| Minimum agent push interval | 60 s | 10 s |
| Regions | 3 (needed for quorum) | 3 |

- **Source of truth:** `PlanCatalog` in monitor-service. Limits travel as data in
  `OrgSnapshot.limits`, so telemetry enforces host and push limits without redeploying.
- **Enforcement in monitor-service.** Create monitor / status page / API key runs
  `SELECT … FROM organizations WHERE id = ? FOR UPDATE`, then counts, then inserts, in one
  transaction. This serializes per tenant and closes the concurrent-create race. The interval
  is validated against `minIntervalSeconds`. Errors use `403 {code: "PLAN_LIMIT_REACHED",
  limit, used}`.
- **Enforcement in telemetry.**
  - Host limit: the first N hosts by `first_seen_at` are accepted, new ones get 403.
  - Push interval: returned in the 202 response; pushes that are too frequent get 429.
- **Stripe flow (test mode):**
  - `POST /api/billing/checkout` (owner) creates a Checkout Session in `mode=subscription` with
    `STRIPE_PRICE_PRO`, `client_reference_id=orgId` and `metadata.orgId`.
  - `POST /api/billing/portal` opens the Customer Portal.
  - `GET /api/billing/subscription` returns the plan, status, period end and usage.
- **Webhook** `POST /api/billing/stripe/webhook` (public, verified with the Stripe SDK
  `Webhook.constructEvent`):
  - `checkout.session.completed` links the customer and subscription to the org.
  - `customer.subscription.created|updated`: `active`/`trialing` → PRO; `past_due` → stay PRO
    with a 7-day grace period (banner); `canceled`/`unpaid` → FREE.
  - `customer.subscription.deleted` → FREE.
  - `invoice.payment_failed` → email the owner.
- **Webhook idempotency and ordering.** `stripe_events` stores the event id (duplicates are
  no-ops). An event whose `created` is older than `plan_event_created_at` is ignored.
- **Downgrade, in one transaction:**
  - keep the oldest N monitors active and pause the rest (`paused_reason=PLAN_LIMIT`);
  - raise intervals to the new minimum;
  - unpublish excess status pages;
  - write outbox snapshots for every changed row plus the new `OrgSnapshot`.
  An upgrade un-pauses `PLAN_LIMIT` monitors.
- Without Stripe keys, billing endpoints return 503 and everything else works (same pattern
  as `movie-reservation-system`). Local testing uses
  `stripe listen --forward-to localhost:8100/api/billing/stripe/webhook`.

## Security

- **Keycloak realm `pulsewatch`** (export at `infra/keycloak/realm-pulsewatch.json`, imported
  by `start-dev --import-realm`, stored in `keycloak_db`):
  - Clients:
    - `pulsewatch-web`: public, standard flow, PKCE S256, redirect `http://localhost:5173/*`.
    - `pulsewatch-cli`: public, direct-access grants. Dev/smoke tests only; documented as
      "disable in production".
    - `pulsewatch-api`: no flows. Holds the client roles `owner`, `admin`, `viewer`.
    - `pulsewatch-admin`: confidential service account with `realm-management` `manage-users`
      and `view-users`, used by onboarding.
  - Client scope `pulsewatch` (default on web and cli) with an audience mapper
    (`aud: pulsewatch-api`) and a user-attribute mapper `org_id` → claim `org_id`. `org_id` is
    declared in the realm **user profile** (Keycloak 24+ rejects unmanaged attributes); users
    can't view it and only admins can edit it.
  - Self-registration is enabled. Access tokens last 5 min.
  - Demo users (dev passwords): `alice@acme.test` (owner, Acme), `carol@acme.test` (viewer,
    Acme), `bob@globex.test` (owner, Globex). The dev profile seeds the matching orgs.
- **Issuer pitfall.** `KC_HOSTNAME=http://localhost:8180` makes every token's `iss`
  `http://localhost:8180/realms/pulsewatch`. Containers fetch keys from
  `http://keycloak:8080/.../certs`. So services configure `jwk-set-uri` (internal) plus an
  explicit `JwtIssuerValidator` (external) and an audience validator. They do not use
  `issuer-uri` discovery.
- **Onboarding.** A user without `org_id` calls `POST /api/orgs`:
  1. The org is created as `PENDING_OWNER` with `created_by_sub`.
  2. The Keycloak Admin API sets the `org_id` attribute and the `owner` role.
  3. The org becomes `ACTIVE`.
  The call is resumable: a retry finds the pending org for the same `sub`. The SPA then calls
  `signinSilent()` to get a token with the new claims.
- **Roles.**

  | | viewer | admin | owner |
  |---|---|---|---|
  | Read everything in the org | ✓ | ✓ | ✓ |
  | Manage monitors, status pages, channels, alert rules; ack incidents | | ✓ | ✓ |
  | Manage API keys | | ✓ | ✓ |
  | Billing | | | ✓ |

  Roles are mapped by `PulsewatchJwtConverter` from `resource_access.pulsewatch-api.roles` to
  `ROLE_*` and checked with `@PreAuthorize`.
- **API keys.** Format `pw_live_<prefix8>_<secret43>` (base62, 256-bit `SecureRandom`).
  Stored as prefix + SHA-256 of the secret and shown once. telemetry verifies against its
  replica with `MessageDigest.isEqual`, using a Caffeine cache of 30 s. Revocation propagates
  through the compacted topic in seconds. Scope `ingest:metrics`.
- **SSRF.**
  - Create-time: monitor URLs must be `http`/`https` with no userinfo and no literal private
    IPs.
  - Runtime: `SafeDnsResolver` rejects loopback, private (RFC 1918), link-local (including
    `169.254.169.254`), CGNAT `100.64/10`, multicast, `0.0.0.0/8`, IPv6 ULA/link-local and
    IPv4-mapped forms. Redirects go through the same resolver.
  - The dev profile allows only the `demo-target` container (explicit allow-list, never a
    blanket flag in prod).
- **Edge.**
  - Stateless bearer auth with CSRF disabled; CORS limited to the frontend origin.
  - Gateway rate limits: per org (JWT), per key prefix (ingest), per IP (public, ping).
  - Request size limits; tenant headers stripped.
  - Actuator exposes `health`, `info`, `prometheus` only, and those are not routed by the
    gateway.
- **Public endpoints** (all `@PublicEndpoint`): status pages, ping, ingest (API key), Stripe
  webhook (signature).
- **Secrets** come from env or `.env` (git-ignored); `.env.example` has placeholders only.

## Observability

- **Metrics.** Micrometer + Prometheus on every service. Custom metrics never use orgId or
  monitorId as tags (cardinality); a test whitelists tag keys.
  - `pulsewatch_checker_scheduler_lag_seconds` (timer: start − slot, per region): the main
    scheduler SLI.
  - `pulsewatch_checker_checks_total{region,type,outcome}`,
    `pulsewatch_checker_check_duration_seconds`, `pulsewatch_checker_owned_monitors{region}`,
    `pulsewatch_checker_inflight`.
  - `pulsewatch_incidents_open`, `pulsewatch_incident_transitions_total{kind,transition}`,
    `pulsewatch_monitor_status_unknown` (all regions stale).
  - `pulsewatch_notifications_total{channel,result}`, `pulsewatch_notifications_dlt_total`.
  - `pulsewatch_ingest_samples_total{result}`, `pulsewatch_sse_connections{stream}`.
  - Kafka consumer lag through the Kafka client metrics binder.
- **Tracing.** Micrometer Tracing (OTel bridge) → OTLP → Jaeger. Kafka observation is enabled
  on templates and listeners, so a trace runs gateway → monitor-service → outbox relay →
  checker → incident → notification. Logs include `traceId` and `correlationId`.
- **Grafana.** Provisioned dashboard `infra/grafana/dashboards/pulsewatch.json` with panels for
  scheduler lag p95 by region, checks/s by region and outcome, open incidents, notification
  success/DLT, ingest rate, consumer lag, SSE connections and JVM. Compose profile
  `observability` starts Prometheus, Grafana and Jaeger.

## Tech stack

| Area | Choice |
|---|---|
| Backend | Java 21, Spring Boot 3.5.x (sibling uses 3.5.16), Spring Cloud 2025.0.x (`spring-cloud-starter-gateway-server-webflux`), Spring Kafka, Spring Data JPA (Hibernate 6.6), Flyway (+ `flyway-database-postgresql`), springdoc 2.8.x, Spring Security OAuth2 resource server, Micrometer/Actuator, Caffeine, Apache HttpClient 5, Stripe Java SDK, Spring Mail |
| Data | PostgreSQL 16 + TimescaleDB 2.x (`timescale/timescaledb:<pinned 2.x>-pg16`, the TSL/community image, not `-oss`), Redis 7, Kafka 4.0 KRaft (`apache/kafka:4.0.2`, as in the sibling) |
| Identity | Keycloak 26.x (`quay.io/keycloak/keycloak`, pin the exact tag) |
| Tests (Java) | JUnit 5, Spring Boot Test, Testcontainers (Postgres/Timescale, Kafka, Redis, `com.github.dasniko:testcontainers-keycloak`), `spring-kafka-test` EmbeddedKafka (KRaft, no Docker), WireMock 3, GreenMail, ArchUnit, Awaitility |
| Frontend | React 18+, TypeScript, Vite, React Router, TanStack Query, `react-oidc-context`/`oidc-client-ts`, `@microsoft/fetch-event-source`, Recharts, react-hook-form + zod; Vitest, React Testing Library, MSW, ESLint |
| Agent | Node 22 (`engines >=20`), TypeScript, zero runtime deps; Vitest, ESLint |
| Ops | Docker Compose (profiles `app`, `observability`), Prometheus, Grafana, Jaeger (OTLP), GitHub Actions. No Kubernetes, Helm or cloud deploy. |

## Repository layout

```
pulsewatch/
├── pom.xml                      (parent: Boot parent, Spring Cloud BOM, modules, surefire -Xmx512m)
├── mvnw, mvnw.cmd, .mvn/wrapper/maven-wrapper.properties
├── .gitignore, .dockerignore, .env.example, README.md, PLAN.md
├── Dockerfile                   (one image for every Java module: --build-arg MODULE=…)
├── docker-compose.yml           (infra by default; profiles: app, observability)
├── common-events/               (event records, enums, Topics)
├── service-support/             (tenancy, security, outbox, idempotency, errors, correlation, live SSE hub)
├── api-gateway/
├── monitor-service/             (monitors, orgs, statuspages, apikeys, billing packages)
├── checker-service/
├── telemetry-service/           (checks, hosts, ingest, heartbeat, alerts, live packages)
├── incident-service/            (evaluation, incidents, statuspage packages)
├── notification-service/
├── frontend/                    (Vite React TS; Dockerfile → nginx)
├── agent/                       (Node TS CLI; package.json bin pulsewatch-agent; Dockerfile for demo)
├── infra/
│   ├── postgres/                (Dockerfile FROM timescale, init/01-create-databases.sql)
│   ├── keycloak/realm-pulsewatch.json
│   ├── prometheus/prometheus.yml
│   └── grafana/{provisioning/…, dashboards/pulsewatch.json}
├── scripts/smoke.sh             (e2e against the compose stack)
├── docs/                        (architecture.md, adr/, screenshots/)
└── .github/workflows/ci.yml     (jobs: backend, frontend, agent, compose/e2e)
```

Java packages: `io.github.besko2002.pulsewatch.<service>.<feature>`.

## Local ports

These avoid the ports already used on this machine (5433, 6379, 5000, 7000) and the
`order-processing-microservices` ports (8080–8085, 5435, 9094, 6380, 1025/8025, 3000, 9090,
16686, 4318), so both stacks can run side by side.

| What | Port |
|---|---|
| gateway | 8100 |
| monitor-service | 8101 |
| checker eu-central / us-east / ap-south | 8102 / 8103 / 8104 |
| telemetry-service | 8105 |
| incident-service | 8106 |
| notification-service | 8107 |
| Postgres | 5436 |
| Kafka (external) | 9095 |
| Redis | 6381 |
| Keycloak | 8180 |
| MailHog SMTP / UI | 1026 / 8026 |
| Vite dev | 5173 |
| frontend container | 8090 |
| demo-target (go-httpbin) | 8188 |
| Prometheus | 9091 |
| Grafana | 3001 |
| Jaeger UI / OTLP | 16687 / 4319 |

## Testing strategy (no Docker on the dev machine)

- **Local gate for every phase:** `./mvnw -B verify` (and `npm test`/`npm run build` for
  frontend and agent) must pass **without Docker**.
- **Every test that needs Postgres, Timescale, Redis or Keycloak:**
  - is a Testcontainers test annotated `@Testcontainers(disabledWithoutDocker = true)` and
    named `*IntegrationTest`;
  - is skipped locally and runs in GitHub Actions.
  - Containers are `@Container static` fields with `@ServiceConnection`. Never start
    containers in static initializers or base-class constructors, because that fails even
    when the test is disabled.
- **Local tests without Docker cover:**
  - pure domain logic without Spring: slots, evaluator, rule engine, plan limits, downgrade,
    uptime math, SSRF classifier, API key codec, crypto;
  - `@WebMvcTest` and `@JsonTest` slices;
  - `@EmbeddedKafka(kraft = true)` for Kafka behaviour, only in modules or test contexts
    without a DataSource (the checker and the notification retry flow with a stubbed store);
  - WireMock (Stripe, Slack, Discord, Telegram, Keycloak admin), GreenMail (SMTP), the JDK
    `HttpServer`/`HttpsServer` and `ServerSocket` (check targets), and MockMvc async (SSE).
- **Timescale in CI:** the same pinned image via
  `DockerImageName.parse("timescale/timescaledb:<tag>").asCompatibleSubstituteFor("postgres")`.
- **Integration test risk.** Integration tests are written blind locally, so keep them few and
  focused per phase. **Push early so CI gives feedback each phase** (see Open questions).
- **Definition of done (every phase):**
  - the local gate is green;
  - the counts of skipped (Docker) tests are reported honestly;
  - README "Status" is updated;
  - there is one commit, `phase N: <title>`;
  - CI is green after push. If not pushed yet, say "CI not verified".

## Phases

Each phase takes about 1–2 hours of focused agent work and ends green. Commands run from the
repo root unless they start with `cd`. `CI:` lines need Docker, so they run in GitHub
Actions or on any Docker host.

### Phase 1: Walking skeleton
*The largest phase. If it runs long, split it into 1a (infra + monitor-service + CI) and 1b
(checker + telemetry).*
- **Deliverables:**
  - Parent `pom.xml`: Boot 3.5.x parent, Spring Cloud 2025.0.x BOM, springdoc, surefire
    `-Xmx512m`, modules below. Maven wrapper copied (read-only) from
    `../order-processing-microservices`.
  - `.gitignore`: the sibling's, plus `node_modules/`, `dist/`, `coverage/`, `*.tsbuildinfo`,
    `.vite/`, `.env`.
  - `.env.example`, `README.md` stub (pitch, status, quick start, ports), `git init`.
  - `common-events`: `EventEnvelope`, `Topics`, `MonitorConfig` (HTTP settings only for now),
    `CheckResult`.
  - `service-support` (minimal): JWT → `TenantContext` and `@CurrentTenant`, Hibernate
    `TenantIdentifierResolver`, `ApiError` handler.
  - `monitor-service`:
    - Flyway V1 `organizations`, `monitors`; `Monitor` entity with `@TenantId`.
    - `POST/GET/GET {id}/PUT/DELETE /api/monitors` (HTTP type: url, expectedStatus,
      intervalSeconds ≥ 30).
    - A *temporary* after-commit publisher of `MonitorConfig` and tombstones to
      `pw.config.monitors` (replaced by the outbox in Phase 2).
    - Dev seed for orgs Acme and Globex; springdoc.
  - `checker-service`, region `local`:
    - consumes `pw.config.monitors` (seek to beginning on assignment) into `MonitorRegistry`;
    - a simple 1 s tick scheduler;
    - `HttpCheckExecutor` (Apache HttpClient 5, virtual threads, timeout) publishing
      `CheckResult` to `pw.checks.results`.
  - `telemetry-service`:
    - Flyway V1 `CREATE EXTENSION IF NOT EXISTS timescaledb`, `check_results` hypertable with
      `UNIQUE (monitor_id, region, time)`;
    - Kafka consumer → `INSERT … ON CONFLICT DO NOTHING`;
    - `GET /api/checks/{monitorId}/results?from&to` scoped by tenant.
  - `docker-compose.yml`, infra only, with the sibling's header comment style:
    - Postgres/Timescale built from `infra/postgres` (init creates the 4 service databases,
      `keycloak_db`, owners, and the timescaledb extension in `telemetry_db`);
    - Kafka KRaft;
    - Redis;
    - Keycloak `start-dev --import-realm` on `keycloak_db`;
    - MailHog.
  - Minimal `infra/keycloak/realm-pulsewatch.json` (realm, `pulsewatch-web`, `pulsewatch-cli`,
    audience mapper, `org_id` mapper and user-profile attribute, alice and bob).
  - `.github/workflows/ci.yml`: `backend` job (temurin 21, `./mvnw -B verify`, test-report
    step as in the sibling) and `compose` job (`docker compose config -q`).
- **Files/modules:** root files above, `common-events/`, `service-support/`,
  `monitor-service/`, `checker-service/`, `telemetry-service/`, `infra/postgres/`,
  `infra/keycloak/`, `.github/workflows/ci.yml`.
- **Tests:**
  - Local:
    - `MonitorControllerTest` (`@WebMvcTest`): 401 without a token, validation 400, tenant
      taken from the `org_id` claim through `jwt()`.
    - `HttpCheckExecutorTest` (JDK `HttpServer`): 200 → UP, 500 → DOWN `STATUS_MISMATCH`,
      slow → `TIMEOUT`, closed port → `CONNECT_REFUSED`.
    - `CheckResult`/`MonitorConfig` JSON round-trip.
    - `SkeletonPipelineTest` (`@EmbeddedKafka`, checker only): publish a config, then get a
      result on `pw.checks.results` within 10 s.
  - CI:
    - `MonitorTenantIsolationIntegrationTest` (Postgres).
    - `CheckResultsIntegrationTest` (Timescale: hypertable exists, duplicate insert ignored,
      other org sees nothing).
- **Acceptance:**
  - `./mvnw -B verify` shows BUILD SUCCESS on a machine without Docker, with integration tests
    reported as skipped.
  - The realm JSON is valid.
  - A CI workflow exists with both jobs.
  - README documents the manual Docker run: `docker compose up -d`, run services with
    `./mvnw -pl <module> spring-boot:run`, get a token with the `pulsewatch-cli` password grant,
    `curl` CRUD, then see results.
- **Verify:**
  ```
  ./mvnw -B verify
  ./mvnw -B -pl checker-service -am verify
  python3 -m json.tool infra/keycloak/realm-pulsewatch.json > /dev/null && echo realm-ok
  # CI: docker compose config -q ; ./mvnw -B verify (Testcontainers run)
  ```

### Phase 2: Monitor types and Transactional Outbox
- **Deliverables:**
  - Sealed `MonitorSettings` (HTTP with method, expected statuses, keyword PRESENT/ABSENT,
    redirects, headers; TCP; SSL with warnDays; HEARTBEAT with generated token, period, grace),
    Jackson polymorphism on `type`, `settings jsonb`.
  - Validators per type; create-time URL validation (scheme, no userinfo, no literal private
    IP).
  - `POST /api/monitors/{id}/pause|resume`.
  - Outbox (`OutboxWriter`/`OutboxRelay`/scheduler, ported pattern) in `service-support`. The
    snapshot is written in the same transaction; delete → tombstone.
  - `NewTopic` beans (compact, 12 partitions). The Phase 1 publisher is removed.
  - The checker logs and skips non-HTTP types until Phase 6.
- **Files/modules:** `common-events`, `service-support/outbox`,
  `monitor-service/{monitors,db/migration/V2__…}`, `checker-service` (type filter).
- **Tests:**
  - Local: JSON round-trip per type; validator matrix (bad port, interval below 30, keyword
    without HTTP, private IP literal); `OutboxRelay` unit test with a mocked `KafkaTemplate`
    (ordering, tombstone, retry on send failure); WebMvc for pause/resume.
  - CI: monitor-service + Kafka. Create → keyed record on `pw.config.monitors`; delete →
    tombstone; a rolled-back transaction → no record.
- **Acceptance:** every monitor change reaches Kafka exactly through the outbox, and
  deletions produce tombstones.
- **Verify:** `./mvnw -B -pl monitor-service,checker-service -am verify`

### Phase 3: Organizations, roles and tenant isolation
- **Deliverables:**
  - Full realm: `pulsewatch-api` roles, `pulsewatch-admin` service account, carol (viewer),
    token lifetimes.
  - `PulsewatchJwtConverter`, audience and issuer validators (`jwk-set-uri` internal plus
    external issuer), `@PublicEndpoint`, `TenantContext.system()` with `isRoot`.
  - `POST /api/orgs` onboarding (resumable, Keycloak Admin API via `RestClient` with client
    credentials), `GET /api/orgs/current`.
  - `@PreAuthorize` role rules.
  - `OrgSnapshot` with FREE limits → `pw.config.orgs` via the outbox.
  - Isolation guard tests.
- **Files/modules:** `service-support/{security,tenancy}`, `monitor-service/orgs`,
  `infra/keycloak/realm-pulsewatch.json`, `common-events` (`OrgSnapshot`, `PlanLimits`).
- **Tests:**
  - Local:
    - `EndpointInventoryTest`; ArchUnit/reflection rules (no orgId parameters, `JdbcTemplate`
      only in `TenantJdbc`);
    - role matrix in WebMvc (viewer 403 on writes);
    - onboarding against WireMock Keycloak admin (happy path, Keycloak failure → org stays
      `PENDING_OWNER`, retry resumes, 409 if the user already has an org).
  - CI: `TenantIsolationIntegrationTest`; `KeycloakRealmIntegrationTest`
    (testcontainers-keycloak imports the realm, alice's password-grant token has `org_id`,
    `aud=pulsewatch-api` and the `owner` role).
- **Acceptance:** the tenant comes only from the token, cross-tenant access gives 404, roles
  are enforced, and the realm export imports cleanly.
- **Verify:**
  ```
  ./mvnw -B -pl service-support,monitor-service -am verify
  python3 -m json.tool infra/keycloak/realm-pulsewatch.json > /dev/null && echo realm-ok
  ```

### Phase 4: API gateway
- **Deliverables:**
  - `api-gateway` module (gateway WebFlux, reactive resource server, reactive Redis).
  - Routes: `/api/monitors|orgs|status-pages|api-keys|billing/**` → monitor;
    `/api/checks|hosts|alert-rules|live|ingest|ping/**` → telemetry;
    `/api/incidents|status/**`, `/api/public/**` → incident;
    `/api/channels|notifications/**` → notification.
  - Public route allow-list.
  - `TenantKeyResolver` (`org:{org_id}` | `key:{prefix}` | `ip:{addr}`) with per-route
    `RedisRateLimiter`.
  - Correlation id filter; strip `X-Org-Id`; CORS for `http://localhost:5173`.
  - Streaming routes with the response timeout disabled.
- **Files/modules:** `api-gateway/`, root `pom.xml`, `.env.example`.
- **Tests:**
  - Local: KeyResolver unit tests; WebTestClient + WireMock backends (routing, 401 on
    protected routes, public routes pass without a token, `X-Org-Id` stripped, correlation id
    added), with the rate limiter disabled in this profile.
  - CI: Redis rate-limit test (burst → 429 for org A while org B is unaffected).
- **Acceptance:** every planned route resolves, auth happens at the edge, and the per-tenant
  limit is independent between tenants.
- **Verify:** `./mvnw -B -pl api-gateway -am verify`

### Phase 5: Distributed check scheduling across regions
- **Deliverables:**
  - `SlotCalculator` (hashed phase plus region stagger).
  - `MonitorOwnershipListener` (`ConsumerSeekAware`: replay on assign, drop on revoke).
  - `CheckScheduler` (timer queue plus virtual-thread executor, `Clock` injected; catch-up
    only for the latest slot).
  - Consumer config: group `checker-{region}`, cooperative-sticky, `group.instance.id`, no
    commits.
  - `PULSEWATCH_REGION` and region list config.
  - Metrics: scheduler lag timer, owned monitors gauge.
  - README section on running 3 regions on the host.
- **Files/modules:** `checker-service/scheduling/**`, `common-events` (region enum/list).
- **Tests (all local):**
  - Slot math: deterministic, in range, different per monitor, staggered per region.
  - Scheduler with a fake clock: no backlog after downtime, a removed monitor stops.
  - `@EmbeddedKafka` with 12 partitions: two workers in `checker-eu` plus one in `checker-us`
    over 3 intervals give every `(monitor, region, slot)` exactly once and full coverage.
  - Stop one eu worker: its monitors continue on the other within the rebalance time, and any
    duplicates share the same slot key.
- **Acceptance:** horizontal scale-out and failover within a region, every region covers
  every monitor, and the double-execution analysis holds in tests.
- **Verify:** `./mvnw -B -pl checker-service -am verify`

### Phase 6: Check executors and SSRF guard
- **Deliverables:**
  - `HttpCheckExecutor` complete: expected statuses, keyword present/absent on body capped at
    1 MB, redirects re-validated, custom headers, User-Agent.
  - `TcpCheckExecutor`.
  - `SslCheckExecutor` (SNI handshake, chain validation, `notAfter` → days left, DEGRADED when
    ≤ warnDays, DOWN on expired or invalid chain).
  - `SafeDnsResolver` plus dev allow-list; `ErrorCode` taxonomy.
  - Global and per-host semaphores.
  - Region simulation: `pulsewatch.checker.simulation.{extra-latency-ms,failure-rate}` →
    `SIMULATED_FAULT`.
- **Files/modules:** `checker-service/executors/**`, `checker-service/net/**`.
- **Tests (local):**
  - `SafeDnsResolver` table (every blocked range, IPv6, IPv4-mapped, the metadata IP; redirect
    to 127.0.0.1 gives `BLOCKED_TARGET`).
  - Keyword and status matrix with `HttpServer`.
  - TLS against an `HttpsServer` using certs generated in the test (BouncyCastle `bcpkix`
    test dependency): valid, expiring in 5 days → DEGRADED, expired → DOWN.
  - TCP open, closed and timeout.
  - Concurrency limiter.
- **Acceptance:** all four active check types work (heartbeat is passive); internal targets
  are impossible outside the dev allow-list.
- **Verify:** `./mvnw -B -pl checker-service -am verify`

### Phase 7: Telemetry storage, rollups and query API
- **Deliverables:**
  - Flyway V2: compression and retention policies, `check_results_1h` and hierarchical
    `check_results_1d` continuous aggregates with refresh policies and
    `materialized_only=false`. The `.conf` side file sets `executeInTransaction=false` for
    the aggregate migration.
  - `TenantJdbc`.
  - Batch listener with JDBC batch insert.
  - `monitor_replica` from `pw.config.monitors` (tombstones handled, versions compared).
  - Endpoints:
    - `GET /api/checks/{monitorId}/series?from&to&resolution=auto` (raw ≤ 6 h, 1h ≤ 30 d,
      otherwise 1d; per region: up %, avg/max latency);
    - `GET /api/checks/summary` (last result, 24 h uptime, p95 latency per monitor).
  - Unknown or foreign monitor → 404.
- **Files/modules:** `telemetry-service/{checks,replica,db/migration}`, `service-support`
  (`TenantJdbc`, idempotency).
- **Tests:**
  - Local: resolution picker; WebMvc with mocked query service; replica version and tombstone
    logic.
  - CI: Timescale migrations apply; `timescaledb_information.hypertables` and
    `continuous_aggregates` contain the expected objects;
    `CALL refresh_continuous_aggregate` gives correct bucket counts; batch with duplicates
    stores each once; isolation.
- **Acceptance:** results are stored idempotently, rollups are queryable, and resolution
  switches automatically.
- **Verify:** `./mvnw -B -pl telemetry-service -am verify`

### Phase 8: Incident engine
- **Deliverables:**
  - `incident-service` module: Flyway (replica, region state, monitor status, incidents with
    the partial unique index, timeline, outbox, processed_events).
  - Pure-domain `IncidentEvaluator` (per-region hysteresis, freshness, quorum, status
    transitions, flap reopen and throttle, SSL warning, pause/delete).
  - Consumers for `pw.config.monitors` and `pw.checks.results`; outbox → `pw.incidents.events`.
  - API: `GET /api/incidents?state&from&to`, `GET /api/incidents/{id}` (with timeline),
    `POST /api/incidents/{id}/ack` (admin+), `GET /api/status/monitors`.
  - Gateway routes checked.
- **Files/modules:** `incident-service/`, `common-events` (`IncidentEvent`), root `pom.xml`,
  `infra/postgres/init` (already has `incident_db`).
- **Tests:**
  - Local: table-driven evaluator suite:
    - 1 region blip → no incident;
    - 2 regions → OPEN;
    - stale region ignored;
    - degraded quorum;
    - recovery needs `recoveryThreshold`;
    - reopen within 10 min → REOPENED with the same id;
    - 4th flap throttled;
    - duplicate or older slot → no-op;
    - pause resolves.
    Plus WebMvc and ack role checks.
  - CI: Postgres + Kafka. A result stream gives an incident row and `OPENED` then `RESOLVED`
    events; a concurrent duplicate gives one open incident.
- **Acceptance:** confirmed multi-region failures, and only those, open incidents; events are
  published through the outbox.
- **Verify:** `./mvnw -B -pl incident-service,api-gateway -am verify`

### Phase 9: Heartbeat monitors
- **Deliverables:**
  - telemetry: `heartbeat_state` maintained from the replica;
    `GET|POST /api/ping/{token}` and `/fail` (`@PublicEndpoint`).
  - `DeadlineSweeper`, a reusable component: `SELECT … WHERE next_deadline < now() ORDER BY
    next_deadline LIMIT 100 FOR UPDATE SKIP LOCKED`, which emits `CheckResult(region=
    "heartbeat", DOWN, HEARTBEAT_MISSED)` through the outbox to `pw.checks.results` and
    advances the deadline. A ping emits UP.
  - incident-service treats region `heartbeat` with quorum 1.
- **Files/modules:** `telemetry-service/heartbeat/**`, `service-support/outbox` (reused),
  `incident-service` (quorum rule).
- **Tests:**
  - Local: deadline math (period + grace, first ping, late ping, `/fail`), controller (unknown
    token 404, paused monitor ignored).
  - CI: two sweepers in parallel each emit a missed heartbeat once; an end-to-end missed
    ping gives an incident.
- **Acceptance:** a cron that stops pinging opens an incident after period + grace, and
  resumes resolve it.
- **Verify:** `./mvnw -B -pl telemetry-service,incident-service -am verify`

### Phase 10: Notifications I (channels, fan-out, email)
- **Deliverables:**
  - `notification-service` module: channels CRUD `/api/channels` (admin+), `SecretBox`
    (AES-256-GCM, key id), masked targets, `POST /api/channels/{id}/test`.
  - Fan-out consumer (`min_severity`, enabled filter; deterministic ids; deliveries plus
    outbox).
  - Request consumer → `EmailSender` (Spring Mail → MailHog, HTML and text templates with a
    dashboard link); delivery status.
- **Files/modules:** `notification-service/`, `common-events` (`NotificationRequested`),
  root `pom.xml`, `.env.example` (`PULSEWATCH_SECRETS_KEY`, `MAIL_*`).
- **Tests:**
  - Local: `SecretBox` round-trip, tamper → fail, wrong key id; fan-out unit; template
    rendering; `EmailSender` against GreenMail.
  - CI: an incident event gives a `SENT` delivery and a mail in GreenMail; a redelivered event
    sends no duplicate.
- **Acceptance:** each incident transition emails each enabled email channel exactly once
  per transition, and secrets are never stored or returned in plaintext.
- **Verify:** `./mvnw -B -pl notification-service -am verify`

### Phase 11: Notifications II (webhooks, retries, DLT)
- **Deliverables:**
  - `SlackSender`, `DiscordSender`, `TelegramSender` (`RestClient`, allow-listed hosts,
    provider payload formats).
  - Error classification; `@RetryableTopic` plus `@DltHandler`.
  - `GET /api/notifications/deliveries`, `POST /api/notifications/deliveries/{id}/retry`.
  - Delivery metrics.
- **Files/modules:** `notification-service/{senders,delivery}`.
- **Tests (local):**
  - WireMock per provider (payload shape, 400 → straight to DLT, 503 → retries, Telegram 429
    `retry_after`).
  - `@EmbeddedKafka` with a stubbed `DeliveryStore` and millisecond backoffs: a failing
    endpoint passes through the retry topics to `-dlt` and becomes `FAILED`; replay after
    the fix gives `SENT`.
  - Allow-list rejects `http://169.254.169.254`.
- **Acceptance:** a failing channel never blocks the others; a poison message ends in the DLT
  and can be replayed.
- **Verify:** `./mvnw -B -pl notification-service -am verify`

### Phase 12: Live update hub (Redis pub/sub + SSE)
- **Deliverables:**
  - `service-support/live`: `LiveBus` interface with Redis and in-memory implementations,
    `SseHub` (per-key registry, 15 s heartbeat, 30 min timeout, cleanup, per-org connection
    cap), `LiveEvent`.
  - telemetry publishes `check.result` after storing and serves `GET /api/live/stream`.
  - incident-service publishes `monitor.status` and `incident` to `pw:live:{orgId}`.
  - Gateway streaming verified (no buffering).
- **Files/modules:** `service-support/live/**`, `telemetry-service/live`,
  `incident-service` (publisher), `api-gateway` (route config).
- **Tests:**
  - Local: `SseHub` unit (org isolation, cleanup on error or timeout, heartbeat, cap → 429);
    MockMvc async SSE with the in-memory bus; gateway streaming test against a WireMock
    chunked backend.
  - CI: two hubs on one Redis both receive an event published once.
- **Acceptance:** a check result reaches a connected dashboard client in under a second on
  any instance, and never reaches another org.
- **Verify:** `./mvnw -B -pl service-support,telemetry-service,incident-service,api-gateway -am verify`

### Phase 13: Status pages
- **Deliverables:**
  - monitor-service: `/api/status-pages` CRUD (slug rules, reserved words, `maxStatusPages`
    from the org limits) → outbox `pw.config.statuspages`.
  - incident-service:
    - `status_page_replica`;
    - `StatusPageAssembler` (overall status rules, 90-day uptime from incidents in UTC days,
      data minimization);
    - `GET /api/public/status/{slug}` (Redis cache-aside, ETag/304, Cache-Control);
    - `GET /api/public/status/{slug}/events` (debounced SSE);
    - invalidation on change.
  - Gateway public route with per-IP limit.
- **Files/modules:** `monitor-service/statuspages`, `incident-service/statuspage/**`,
  `common-events` (`StatusPageConfig`), `api-gateway`.
- **Tests:**
  - Local: assembler (all overall-status cases; uptime with overlapping incidents, open
    incident, day boundaries); payload has no url, host or error fields; ETag → 304; slug
    validation; limit 403.
  - CI: cache hit, miss and invalidation with Redis; unpublished page → 404.
- **Acceptance:** an outage appears on the public page live and from cache, and the page
  leaks nothing internal.
- **Verify:** `./mvnw -B -pl monitor-service,incident-service,api-gateway -am verify`

### Phase 14: API keys and agent metrics ingest
- **Deliverables:**
  - monitor-service: `/api/api-keys` (create shows the key once, list, revoke), `ApiKeyCodec`
    → outbox `pw.config.apikeys`.
  - telemetry:
    - `api_key_replica`, `org_entitlements`;
    - API-key security filter chain for `/api/ingest/**`;
    - `POST /api/ingest/v1/metrics` (gzip, validation, limits) → `pw.host.metrics`;
    - `GET /api/ingest/v1/whoami`;
    - consumer → hosts upsert plus `host_metrics`/`host_disk_metrics` hypertables;
    - `host_metrics_1m` and `host_metrics_1h` aggregates and policies;
    - `GET /api/hosts`, `GET /api/hosts/{id}/series?metric&from&to&resolution=auto`;
    - host limit plus push-interval enforcement.
- **Files/modules:** `monitor-service/apikeys`, `telemetry-service/{ingest,hosts,db/migration}`,
  `common-events`.
- **Tests:**
  - Local: codec (format, entropy length, hash, constant-time verify); filter (missing,
    malformed, unknown, revoked, wrong scope → 401); payload validation (future `ts`, > 120
    samples → 400, > 256 KB → 413, gzip); 403 at the host limit; 429 on too-frequent pushes.
  - CI: ingest → Kafka → stored → series query; aggregates refresh; isolation.
- **Acceptance:** an agent with a valid key can push and query, revocation takes effect within
  seconds, and limits hold.
- **Verify:** `./mvnw -B -pl monitor-service,telemetry-service -am verify`

### Phase 15: Agent CLI (Node + TypeScript)
- **Deliverables:**
  - `agent/package.json` (name `pulsewatch-agent`, `bin`, `files: ["dist"]`, `engines`), plus
    `tsconfig.json`, `eslint.config.js`, `vitest.config.ts`.
  - `src/cli.ts` (`run | once --json | check | --version`), `config.ts` (precedence),
    `collectors/{cpu,memory,load,disk}.ts` (injectable `os`/`fs`), `buffer.ts`, `backoff.ts`,
    `sender.ts` (fetch, gzip, response handling table), `hostId.ts`.
  - `agent/README.md` (install, systemd unit example).
  - CI job `agent` (Node 22: `npm ci`, lint, test, build, `npm pack --dry-run`).
- **Files/modules:** `agent/**`, `.github/workflows/ci.yml`.
- **Tests (Vitest):**
  - collectors with fake `/proc/meminfo` and cpu ticks;
  - config precedence;
  - ring buffer overflow;
  - backoff bounds with jitter;
  - sender against a local `http.createServer` (auth header, gzip body, 429 `Retry-After`,
    401 slow retry, 413 split);
  - SIGTERM flush.
- **Acceptance:** `once --json` prints a valid sample on macOS and Linux, and the package
  contains only `dist`, `README` and `package.json`.
- **Verify:**
  ```
  cd agent && npm install && npm run lint && npm test && npm run build \
    && node dist/cli.js once --json && npm pack --dry-run
  ```
  Use `npm install` the first time to create `package-lock.json`, then `npm ci`.

### Phase 16: Host alert rules and offline detection
- **Deliverables:**
  - telemetry: `/api/alert-rules` CRUD (admin+), `alert_rule_state`, `RuleEvaluator` running
    in the host-metrics consumer (serial per host thanks to the hostId key; OK → PENDING →
    FIRING after `for_seconds`, then RESOLVED), `HostOfflineSweeper` (reuses `DeadlineSweeper`,
    3 × push interval) → outbox `pw.host.alerts`.
  - incident-service: consumer → incidents `HOST_THRESHOLD`/`HOST_OFFLINE` (subject keys
    `host:{id}:rule:{ruleId}`, `host:{id}:offline`, no quorum).
- **Files/modules:** `telemetry-service/alerts/**`, `incident-service/hostalerts`,
  `common-events` (`HostAlert`).
- **Tests:**
  - Local: evaluator table (brief spike → no alert, sustained → FIRING once, recovery →
    RESOLVED, missing disk mount, rule targeting all hosts); sweeper deadline math.
  - CI: metrics stream → `HostAlert` → `IncidentEvent`; offline host → incident, back online
    → resolved.
- **Acceptance:** "CPU > 90 % for 5 min" and "host silent" each produce exactly one incident
  and notification chain.
- **Verify:** `./mvnw -B -pl telemetry-service,incident-service -am verify`

### Phase 17: Billing and plan limits (Stripe subscriptions)
- **Deliverables:**
  - monitor-service `billing/`: `PlanCatalog`, `PlanLimitEnforcer` (org-row lock) used by
    monitors, status pages and API keys; checkout, portal, subscription and webhook endpoints;
    `stripe_events`; `PlanChangeService` (upgrade, downgrade, grace period); `OrgSnapshot`
    publishing.
  - Disabled mode (503) when keys are missing.
  - Gateway public webhook route; `.env.example` `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`,
    `STRIPE_PRICE_PRO`, `APP_BASE_URL`.
- **Files/modules:** `monitor-service/billing/**`, `monitor-service/{monitors,statuspages,apikeys}`
  (enforcer calls), `api-gateway`.
- **Tests:**
  - Local:
    - signature verification with a test secret and computed `Stripe-Signature` (valid,
      tampered, stale timestamp);
    - status → plan mapping table;
    - duplicate event no-op;
    - older event ignored;
    - downgrade algorithm (pause newest beyond limit, clamp intervals, unpublish pages,
      upgrade un-pauses only `PLAN_LIMIT`);
    - checkout session creation against WireMock (`Stripe.overrideApiBase`).
  - CI: two parallel creates at 4/5 give exactly one 201 and one `PLAN_LIMIT_REACHED`; a
    downgrade publishes monitor and org snapshots.
- **Acceptance:** limits cannot be bypassed under concurrency, and the plan follows Stripe
  state even with duplicate or out-of-order webhooks.
- **Verify:** `./mvnw -B -pl monitor-service,api-gateway -am verify`

### Phase 18: Frontend I (shell, login, monitors)
- **Deliverables:**
  - `frontend/` Vite React TS; scripts `dev`, `build`, `lint`, `typecheck`, `test`.
  - OIDC with `react-oidc-context` (PKCE, silent renew), `apiClient` (bearer, 401 → login,
    `ApiError` mapping), typed API modules.
  - Onboarding screen when there is no `org_id`, then `signinSilent`.
  - App shell and navigation.
  - Monitors list, create and edit (type-specific forms with zod), pause, resume and delete.
  - Role-aware UI.
  - `.env.example` `VITE_*`; CI job `frontend`.
- **Files/modules:** `frontend/**`, `.github/workflows/ci.yml`.
- **Tests (Vitest + RTL + MSW):** form validation per type; list rendering; viewer sees no
  write actions; onboarding flow; 401 handling; plan-limit error shown.
- **Acceptance:** log in as alice, create a monitor, and see it listed; a viewer is read-only.
- **Verify:** `cd frontend && npm install && npm run lint && npm run typecheck && npm test && npm run build`

### Phase 19: Frontend II (live dashboard, charts, incidents, hosts)
- **Deliverables:**
  - `useLiveStream` (fetch-event-source, bearer, reconnect backoff, snapshot then deltas into
    the TanStack Query cache).
  - Dashboard tiles (live status, last latency).
  - Monitor detail (latency and uptime per region, resolution switch, Recharts).
  - Incidents list and detail (timeline, ack).
  - Hosts list and detail (CPU, memory, disk, load charts, live).
- **Files/modules:** `frontend/src/{live,dashboard,monitors,incidents,hosts}/**`.
- **Tests:** stream hook (reconnect, duplicate events ignored, snapshot reconciliation) with a
  mocked fetch stream; event reducer; series transforms; ack flow with MSW.
- **Acceptance:** with mocked APIs, live events update tiles without a refresh, and charts
  render every resolution.
- **Verify:** `cd frontend && npm ci && npm run lint && npm run typecheck && npm test && npm run build`

### Phase 20: Frontend III (public status page, settings, billing)
- **Deliverables:**
  - `/status/:slug` route, lazy-loaded with no OIDC: native `EventSource`, 90-day uptime bars,
    incident history.
  - Settings: notification channels (with test send), API keys (show-once modal, copy,
    revoke), alert rules, status page editor.
  - Billing page (usage meters, upgrade → Checkout redirect, Manage → Portal, past_due
    banner).
- **Files/modules:** `frontend/src/{statuspage,settings,billing}/**`.
- **Tests:** uptime bar rendering and tooltip; status page live update from a mocked
  `EventSource`; the API key appears exactly once; billing states FREE, PRO and past_due;
  channel form masks secrets.
- **Acceptance:** every user-facing feature in the description is reachable in the UI.
- **Verify:** `cd frontend && npm ci && npm run lint && npm run typecheck && npm test && npm run build`

### Phase 21: Observability
- **Deliverables:**
  - Prometheus registry, OTel bridge and OTLP exporter on every service; Kafka observation
    on; log pattern with traceId and correlationId.
  - The custom metrics listed above.
  - `infra/prometheus/prometheus.yml` (scrapes `host.docker.internal:8100-8107`),
    `infra/grafana` provisioning and `dashboards/pulsewatch.json`.
  - Compose `observability` profile (Jaeger, Prometheus, Grafana on the ports above).
- **Files/modules:** every service `pom.xml` and `application.yml`, checker, incident and
  notification metrics classes, `infra/{prometheus,grafana}`, `docker-compose.yml`.
- **Tests (local):** `SimpleMeterRegistry` asserts metrics are registered and incremented
  (checker, incident, notification); tag-key whitelist (no orgId or monitorId); dashboard
  JSON is valid.
- **Acceptance:** one trace spans HTTP → Kafka → three services; the dashboard shows scheduler
  lag per region.
- **Verify:**
  ```
  ./mvnw -B verify
  python3 -m json.tool infra/grafana/dashboards/pulsewatch.json > /dev/null && echo dash-ok
  # CI/Docker host: docker compose --profile observability config -q
  ```

### Phase 22: Containerised stack and end-to-end smoke test
- **Deliverables:**
  - Root `Dockerfile` (`MODULE` arg, non-root, healthcheck; mirrors the sibling) and
    `.dockerignore`.
  - `frontend/Dockerfile` (build → nginx, runtime `config.js` for OIDC and API URLs);
    `agent/Dockerfile`.
  - Compose `app` profile: gateway, monitor, `checker-eu-central`/`checker-us-east`/
    `checker-ap-south` (ap with `failure-rate=0.3` to demo suppression), telemetry, incident,
    notification, frontend, `demo-target` (`mccutchen/go-httpbin`), `demo-agent`; memory
    limits and healthchecks.
  - `scripts/smoke.sh`:
    1. alice's token via `pulsewatch-cli`;
    2. create an HTTP monitor on `demo-target /status/200`;
    3. results arrive from 3 regions;
    4. switch to `/status/503` → incident OPEN plus a MailHog message (`/api/v2/messages`)
       plus the public page shows an outage;
    5. switch back → RESOLVED;
    6. bob gets 404 on alice's monitor;
    7. the demo agent's host appears.
  - CI job `e2e`: `docker compose --profile app up -d --build --wait`, then `smoke.sh`, with
    logs printed on failure.
- **Files/modules:** `Dockerfile`, `.dockerignore`, `frontend/Dockerfile`, `agent/Dockerfile`,
  `docker-compose.yml`, `scripts/smoke.sh`, `.github/workflows/ci.yml`.
- **Acceptance:** the CI e2e job is green (it cannot run locally without Docker), and the
  local gates are still green.
- **Verify:**
  ```
  ./mvnw -B verify
  bash -n scripts/smoke.sh && (command -v shellcheck >/dev/null && shellcheck scripts/smoke.sh || true)
  # CI: docker compose --profile app up -d --build --wait && ./scripts/smoke.sh
  ```

### Phase 23: Documentation and release polish
- **Deliverables:**
  - README: pitch, architecture diagram, feature tour, quick start (infra-only and full
    stack), ports, demo users, Stripe test-mode setup, agent install, testing strategy
    (local vs CI), design decisions summary, "skills demonstrated", screenshot slots in
    `docs/screenshots/`.
  - `docs/architecture.md` (scheduling, state machine, tenancy).
  - `docs/adr/` (4 short ADRs: scheduling, tenancy, SSE, Timescale).
  - CI polish (npm cache, test summary).
  - License (ask the user).
  - Final pass on `.env.example`.
- **Files/modules:** `README.md`, `docs/**`, `.github/workflows/ci.yml`, `LICENSE`.
- **Acceptance:** a reviewer can understand the system and run it from the README alone, and
  all gates are green.
- **Verify:**
  ```
  ./mvnw -B verify
  cd agent && npm ci && npm test && npm run build
  cd frontend && npm ci && npm test && npm run build
  ```

## Out of scope (v1)

Maintenance windows, team invites and multiple orgs per user, SMS/PagerDuty, per-tenant data
retention, Postgres RLS, Avro/Schema Registry, real geographic regions, Kubernetes, Helm and
cloud deployment (the user handles deployment).

## Skills this project adds beyond the sibling projects

- **Keycloak / OIDC.** SPA authorization code with PKCE, resource servers with audience and
  issuer validation, custom claim mappers and user profile, realm-as-code, Admin API
  onboarding. The siblings hand-roll JWTs.
- **Multi-tenant SaaS.** Hibernate `@TenantId` discriminator tenancy, tenant-from-token only,
  404-on-foreign, endpoint-inventory guard tests, per-tenant rate limiting and caches.
- **TimescaleDB.** Hypertables, compression, continuous and hierarchical aggregates,
  retention policies, resolution-aware queries, non-transactional Flyway migrations.
- **Distributed scheduling** through consumer-group partition ownership, deterministic slots,
  and idempotent execution effects, with a written trade-off analysis.
- **Event-carried state transfer** over compacted topics, with tombstones and local read
  replicas (resilience: checks run while the control plane is down).
- **Multi-region quorum, hysteresis and flap handling**: a real false-positive suppression
  design with an exhaustively tested state machine.
- **Non-blocking Kafka retries** (`@RetryableTopic`, retry topics), DLT handling and
  user-triggered replay.
- **Live updates.** SSE with Redis pub/sub fan-out across instances, snapshot-then-deltas
  clients, debouncing.
- **HTTP caching**: Redis cache-aside plus ETag/304 plus `Cache-Control` on a public endpoint.
- **Stripe subscriptions.** Checkout in subscription mode, Customer Portal, lifecycle webhooks,
  out-of-order and duplicate handling, grace periods, downgrade enforcement. The siblings only
  do one-off payments.
- **Application security for an outbound-calling product**: SSRF-safe DNS resolution,
  outbound allow-lists, AES-GCM secrets at rest, prefix-plus-hash API keys.
- **DB-backed deadline sweepers** with `FOR UPDATE SKIP LOCKED` (heartbeats, host offline).
- **Node.js/TypeScript CLI**: zero-dependency system metrics, resilient delivery (buffer,
  backoff, gzip), npm packaging, Vitest.
- **React live dashboards**: OIDC in an SPA, streaming state reconciliation, time-series
  charts, public lazy-loaded route.
- **Testing without local Docker**: EmbeddedKafka (KRaft), WireMock, GreenMail, ArchUnit and
  guard tests, with Testcontainers isolated to CI, plus a compose-based e2e job in CI.

## Open questions for the implementer

Push back on these before or while implementing them.

1. **Pushing early.** Testcontainers and e2e tests cannot run on this machine, so every
   integration test is written blind until CI runs it. Ask the user to authorize creating and
   pushing `besko2002/pulsewatch` **at the end of Phase 1** so each later phase gets CI
   feedback. Until then, report "CI not verified".
2. **Keycloak org model.** The plan uses a user attribute `org_id`, the Admin API and one org
   per user. Keycloak 26 has a built-in *Organizations* feature (`organization` claim,
   invitations) that could replace the custom onboarding but changes the claim shape. Decide
   in Phase 3. Either way, the Admin API service account is a privileged credential and must
   stay dev-scoped.
3. **Phase 1 size.** It builds three services, infra and CI. If it runs past about 2 hours,
   split it into 1a and 1b as noted instead of cutting tests.
4. **Rebalance behaviour of the scheduler.** Static membership plus cooperative-sticky should
   keep rebalances rare. Verify in Phase 5 that the EmbeddedKafka failover test is stable,
   not flaky. If ownership turns out fragile, the fallback is a ShedLock leader that
   dispatches slots into a work topic.
5. **Hibernate `@TenantId` details.** `UUID` tenant ids and `isRoot()` system context need
   Hibernate 6.4+ (Boot 3.5 ships 6.6). Confirm early. The fallback is explicit
   `…AndOrgId` repository methods guarded by the inventory tests.
6. **Timescale specifics.**
   - Continuous aggregates and compression need the TSL (community) image, not `-oss`.
     Testcontainers must use the same pinned image.
   - Aggregate DDL must run outside a transaction (Flyway `.conf` with
     `executeInTransaction=false`).
   - Retention is global, not per plan.
   - The `timescaledb` extension needs superuser to create, so it is created by the init
     script in compose and by the migration in tests.
7. **Memory.** The full `app` profile (6 services, 3 checkers, Keycloak, Kafka, Timescale,
   Redis, frontend) needs about 5–6 GB. A ~4 GB Docker Desktop will struggle, so keep memory
   limits tight (checkers 256 MB) or document running a subset. GitHub runners (7 GB) are
   fine for e2e.
8. **SSE through Spring Cloud Gateway.** Response buffering and timeouts on the WebFlux
   gateway can break streams. If Phase 12's streaming test is unreliable, route `/api/live/**`
   and `/api/public/**/events` with dedicated settings, or let the SPA reach those services
   directly in dev.
9. **Notification duplicates.** At-least-once delivery to Slack, Discord and Telegram is
   accepted and documented. Push back if exactly-once is expected; providers don't support it.
10. **Stripe.** Needs the user's test-mode keys and a Price id for PRO, and the Stripe CLI to
    forward webhooks. Without them billing runs in 503 mode, which is fine for CI.
11. **SSRF allow-list in dev.** The demo target must be reachable only through an explicit
    host allow-list in the `dev`/`compose` profile. Never ship a boolean "allow private"
    flag enabled by default.
12. **Status page uptime source.** Incident-based uptime differs from the dashboard's
    raw-check uptime. Confirm the user is fine showing two numbers, with labels explaining
    each.
13. **Charts.** Recharts is simple but slows down above a few thousand points. Server-side
    resolution keeps series small; switch to uPlot only if profiling shows a problem.
14. **Versions to pin at implementation time:** Spring Boot 3.5.x (sibling 3.5.16), Spring
    Cloud 2025.0.x, Keycloak 26.x, the TimescaleDB `2.x-pg16` tag, `apache/kafka` 4.0.x. Use
    the newest patch releases available and record them in the README.
15. **Naming and licensing.** The groupId `io.github.besko2002.pulsewatch`, the npm package
    name `pulsewatch-agent` (whether to publish to npm is the user's call), and the license
    for the repo all need user confirmation.
