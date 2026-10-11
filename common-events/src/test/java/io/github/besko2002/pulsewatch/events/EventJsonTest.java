package io.github.besko2002.pulsewatch.events;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;
class EventJsonTest {
    private final ObjectMapper mapper=new ObjectMapper().registerModule(new JavaTimeModule());
    @Test void configRoundTrip() throws Exception {
        MonitorConfig config=new MonitorConfig(UUID.randomUUID(),UUID.randomUUID(),"site",MonitorConfig.MonitorType.HTTP,30,1000,
            new MonitorConfig.HttpSettings(URI.create("https://example.org"),200),1);
        assertThat(mapper.readValue(mapper.writeValueAsString(config),MonitorConfig.class)).isEqualTo(config);
    }
    @Test void resultRoundTrip() throws Exception {
        CheckResult result=new CheckResult(UUID.randomUUID(),UUID.randomUUID(),"local",Instant.now(),Instant.now(),23,
            CheckResult.Outcome.UP,200,null,null,null,"checker-local");
        assertThat(mapper.readValue(mapper.writeValueAsString(result),CheckResult.class)).isEqualTo(result);
    }
}
