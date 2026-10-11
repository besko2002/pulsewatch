package io.github.besko2002.pulsewatch.monitor;
import io.github.besko2002.pulsewatch.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.TopicBuilder;
@Configuration
public class MonitorTopics {
    @Bean NewTopic monitorConfigs() {
        return TopicBuilder.name(Topics.MONITORS).partitions(12).replicas(1)
            .config("cleanup.policy", "compact").build();
    }
}
