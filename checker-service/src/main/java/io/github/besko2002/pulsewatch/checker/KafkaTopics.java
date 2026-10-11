package io.github.besko2002.pulsewatch.checker;
import io.github.besko2002.pulsewatch.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.TopicBuilder;
@Configuration
public class KafkaTopics {
    @Bean NewTopic checkResults() { return TopicBuilder.name(Topics.RESULTS).partitions(12).replicas(1).build(); }
}
