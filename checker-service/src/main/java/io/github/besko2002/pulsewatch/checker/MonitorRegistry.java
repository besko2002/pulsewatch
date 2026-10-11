package io.github.besko2002.pulsewatch.checker;
import io.github.besko2002.pulsewatch.events.MonitorConfig;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
@Component
public class MonitorRegistry implements ConsumerSeekAware {
    private final Map<UUID, MonitorConfig> monitors=new ConcurrentHashMap<>();
    private final Map<UUID, Integer> partitions=new ConcurrentHashMap<>();
    @KafkaListener(topics="pw.config.monitors", groupId="checker-${pulsewatch.region:local}")
    public void accept(org.apache.kafka.clients.consumer.ConsumerRecord<String,MonitorConfig> record) {
        UUID id=UUID.fromString(record.key());
        if (record.value()==null) { monitors.remove(id); partitions.remove(id); }
        else { monitors.put(id,record.value()); partitions.put(id,record.partition()); }
    }
    public Collection<MonitorConfig> snapshot() { return List.copyOf(monitors.values()); }
    @Override public void onPartitionsAssigned(Map<TopicPartition,Long> assignments, ConsumerSeekCallback callback) {
        assignments.keySet().forEach(p -> callback.seekToBeginning(p.topic(), p.partition()));
    }
    @Override public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
        Set<Integer> ids=new HashSet<>(); revoked.forEach(p -> ids.add(p.partition()));
        partitions.forEach((id,partition) -> { if (ids.contains(partition)) { monitors.remove(id); partitions.remove(id); } });
    }
}
