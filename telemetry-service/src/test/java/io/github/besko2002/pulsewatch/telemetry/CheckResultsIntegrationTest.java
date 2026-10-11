package io.github.besko2002.pulsewatch.telemetry;
import io.github.besko2002.pulsewatch.telemetry.checks.*;
import io.github.besko2002.pulsewatch.events.CheckResult;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.utility.DockerImageName;
import java.time.Instant;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;
@SpringBootTest(properties="spring.kafka.listener.auto-startup=false") @Testcontainers(disabledWithoutDocker=true)
class CheckResultsIntegrationTest {
    @Container @ServiceConnection static PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>(DockerImageName.parse("timescale/timescaledb:2.20.3-pg16").asCompatibleSubstituteFor("postgres"));
    @Autowired JdbcTemplate jdbc;
    @Autowired CheckResultsStore store;
    @Autowired CheckResultsController controller;
    @Test void hypertableDedupeAndTenantIsolation() {
        assertThat(jdbc.queryForObject("SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name='check_results'",Integer.class)).isEqualTo(1);
        UUID org=UUID.randomUUID(), monitor=UUID.randomUUID(); Instant slot=Instant.now();
        CheckResult result=new CheckResult(monitor,org,"local",slot,slot,10,CheckResult.Outcome.UP,200,null,null,null,"local");
        store.save(result); store.save(result);
        assertThat(controller.results(org,monitor,slot.minusSeconds(1),slot.plusSeconds(1))).hasSize(1);
        assertThat(controller.results(UUID.randomUUID(),monitor,slot.minusSeconds(1),slot.plusSeconds(1))).isEmpty();
    }
}
