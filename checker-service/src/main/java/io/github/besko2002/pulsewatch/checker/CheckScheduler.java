package io.github.besko2002.pulsewatch.checker;
import io.github.besko2002.pulsewatch.events.*;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.*;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
@Component @EnableScheduling
public class CheckScheduler {
    private final MonitorRegistry registry; private final HttpCheckExecutor executor;
    private final KafkaTemplate<String,CheckResult> kafka; private final String region;
    private final ExecutorService workers=Executors.newVirtualThreadPerTaskExecutor();
    private final Map<UUID,Long> lastSlot=new ConcurrentHashMap<>();
    public CheckScheduler(MonitorRegistry registry, HttpCheckExecutor executor, KafkaTemplate<String,CheckResult> kafka,
                          @Value("${pulsewatch.region:local}") String region) {
        this.registry=registry; this.executor=executor; this.kafka=kafka; this.region=region;
    }
    @Scheduled(fixedRate=1000)
    public void tick() {
        long now=Instant.now().getEpochSecond();
        for (MonitorConfig m: registry.snapshot()) {
            long slot=now-now % m.intervalSeconds();
            // Submit once for each newly observed slot.
            Long previous=lastSlot.put(m.monitorId(),slot);
            if (previous==null || previous!=slot) submit(m,slot);
        }
        lastSlot.keySet().retainAll(registry.snapshot().stream().map(MonitorConfig::monitorId).toList());
    }
    private void submit(MonitorConfig m,long slot) {
        workers.submit(() -> kafka.send(Topics.RESULTS,m.monitorId().toString(),executor.execute(m,region,Instant.ofEpochSecond(slot))));
    }
    @PreDestroy public void close() { workers.shutdownNow(); }
}
