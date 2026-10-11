package io.github.besko2002.pulsewatch.events;
import java.net.URI;
import java.util.UUID;
public record MonitorConfig(UUID monitorId, UUID orgId, String name, MonitorType type, int intervalSeconds, int timeoutMs, HttpSettings settings, long version) {
    public enum MonitorType { HTTP }
    public record HttpSettings(URI url, int expectedStatus) {}
}
