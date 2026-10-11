package io.github.besko2002.pulsewatch.events;
import java.time.Instant;
import java.util.UUID;
public record CheckResult(UUID monitorId, UUID orgId, String region, Instant slot, Instant startedAt,
                          int durationMs, Outcome outcome, Integer httpStatus, ErrorCode errorCode,
                          String errorMessage, Instant tlsExpiresAt, String checkerInstance) {
    public enum Outcome { UP, DOWN }
    public enum ErrorCode { TIMEOUT, DNS, CONNECT_REFUSED, TLS_INVALID, STATUS_MISMATCH, INTERNAL }
}
