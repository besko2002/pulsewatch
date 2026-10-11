package io.github.besko2002.pulsewatch.checker;
import io.github.besko2002.pulsewatch.events.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import static org.assertj.core.api.Assertions.assertThat;
@SpringBootTest @EmbeddedKafka(kraft=true,partitions=12,topics={Topics.MONITORS,Topics.RESULTS},bootstrapServersProperty="spring.kafka.bootstrap-servers")
class SkeletonPipelineTest {
    @Autowired KafkaTemplate<String,MonitorConfig> kafka;
    @Autowired EmbeddedKafkaBroker broker;
    @Test void configTriggersResult() throws Exception {
        Properties props=new Properties(); props.putAll(KafkaTestUtils.consumerProps(UUID.randomUUID().toString(),"false",broker));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES,"io.github.besko2002.pulsewatch.events");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE,CheckResult.class.getName());
        try (Consumer<String,CheckResult> consumer=new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(Topics.RESULTS));
            UUID id=UUID.randomUUID();
            MonitorConfig config=new MonitorConfig(id,UUID.randomUUID(),"local",MonitorConfig.MonitorType.HTTP,30,500,
                new MonitorConfig.HttpSettings(URI.create("http://127.0.0.1:1/"),200),1);
            kafka.send(Topics.MONITORS,id.toString(),config).get();
            long deadline=System.nanoTime()+Duration.ofSeconds(10).toNanos();
            while (System.nanoTime()<deadline) {
                for (ConsumerRecord<String,CheckResult> record:consumer.poll(Duration.ofMillis(200)))
                    if (record.value().monitorId().equals(id)) { assertThat(record.value().outcome()).isEqualTo(CheckResult.Outcome.DOWN);return; }
            }
            org.junit.jupiter.api.Assertions.fail("No check result received within 10 seconds");
        }
    }
}
