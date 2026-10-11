# Pulsewatch

Multi-tenant uptime monitoring: Phase 1 connects monitor CRUD through Kafka to a local HTTP checker and a TimescaleDB result API. Future phases add region quorum, billing, status pages and agents.

## Status

Phase 1 walking skeleton. Docker-dependent integration tests run in CI (Docker is unavailable on the development machine). CI not verified until published.

## Pinned versions

Java 21; Spring Boot 3.5.16; Spring Cloud 2025.0.3; springdoc 2.8.16; TimescaleDB 2.20.3-pg16; Kafka 4.0.2; Keycloak 26.4.7; Redis 7.4.5.

## Quick start (manual Docker run)

1. `docker compose up -d` (infrastructure only). Wait for Keycloak to finish importing the realm.
2. Build shared modules with `./mvnw -DskipTests install`, then in separate terminals run `./mvnw -pl monitor-service spring-boot:run -Dspring-boot.run.profiles=dev`, `./mvnw -pl checker-service spring-boot:run`, and `./mvnw -pl telemetry-service spring-boot:run`.
3. Get a token (dev-only `pulsewatch-cli` password grant):
   `TOKEN=$(curl -sf -d 'grant_type=password&client_id=pulsewatch-cli&username=alice@acme.test&password=alice' http://localhost:8180/realms/pulsewatch/protocol/openid-connect/token | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')`
4. Create a public HTTP monitor: `curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"name":"example","url":"https://example.org/","expectedStatus":200,"intervalSeconds":30}' http://localhost:8101/api/monitors`. Use `GET /api/monitors`, `GET /api/monitors/{id}`, `PUT /api/monitors/{id}`, `DELETE /api/monitors/{id}` for CRUD.
5. See results: `curl -H "Authorization: Bearer $TOKEN" 'http://localhost:8105/api/checks/{monitorId}/results?from=2020-01-01T00:00:00Z&to=2030-01-01T00:00:00Z'`.

Ports: monitor 8101, checker 8102 (non-web), telemetry 8105, Postgres 5436, Kafka 9095, Redis 6381, Keycloak 8180, MailHog 1026/8026. The gateway (8100) arrives in Phase 4.

Run `./mvnw -B verify` locally; Docker-backed integration tests skip locally and run on the CI runner. The temporary after-commit publisher is replaced by an outbox in Phase 2; do not use it as a durable delivery guarantee.
