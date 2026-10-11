package io.github.besko2002.pulsewatch.events;
import java.time.Instant;
import java.util.UUID;
public record EventEnvelope<T>(UUID eventId, String eventType, int schemaVersion, Instant occurredAt, UUID orgId, String correlationId, T payload) {}
