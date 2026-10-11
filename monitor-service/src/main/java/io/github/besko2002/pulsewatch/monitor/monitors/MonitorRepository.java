package io.github.besko2002.pulsewatch.monitor.monitors;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;
public interface MonitorRepository extends JpaRepository<Monitor, UUID> {}
