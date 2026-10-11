package io.github.besko2002.pulsewatch.monitor.monitors;
import io.github.besko2002.pulsewatch.events.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.*;
import java.net.URI;
import java.util.UUID;
@Component
public class MonitorPublisher {
    private final KafkaTemplate<String, MonitorConfig> kafka;
    public MonitorPublisher(KafkaTemplate<String, MonitorConfig> kafka) { this.kafka=kafka; }
    // Temporary after-commit publisher; Phase 2 replaces it with the transactional outbox.
    public void publish(Monitor m) { sendAfterCommit(m.getId(), new MonitorConfig(m.getId(), m.getOrgId(), m.getName(), MonitorConfig.MonitorType.HTTP,
        m.getIntervalSeconds(), 5000, new MonitorConfig.HttpSettings(URI.create(m.getUrl()), m.getExpectedStatus()), m.getVersion())); }
    public void delete(UUID id) { sendAfterCommit(id, null); }
    private void sendAfterCommit(UUID id, MonitorConfig config) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() { kafka.send(Topics.MONITORS, id.toString(), config); }
        });
    }
}
