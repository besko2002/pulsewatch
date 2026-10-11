package io.github.besko2002.pulsewatch.monitor.monitors;
import jakarta.validation.constraints.*;
public record MonitorRequest(@NotBlank String name, @NotBlank @Pattern(regexp="https?://.+") String url,
                             @Min(100) @Max(599) int expectedStatus, @Min(30) int intervalSeconds) {}
