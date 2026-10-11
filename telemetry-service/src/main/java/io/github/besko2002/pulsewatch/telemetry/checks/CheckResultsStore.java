package io.github.besko2002.pulsewatch.telemetry.checks;
import io.github.besko2002.pulsewatch.events.CheckResult;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.sql.Timestamp;
@Component
public class CheckResultsStore {
    private final JdbcTemplate jdbc;
    public CheckResultsStore(JdbcTemplate jdbc) { this.jdbc=jdbc; }
    @KafkaListener(topics="pw.checks.results",groupId="telemetry-results")
    public void save(CheckResult result) {
        jdbc.update("INSERT INTO check_results(time,monitor_id,org_id,region,outcome,latency_ms,http_status,error_code,error_message,checker_instance) VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT (monitor_id,region,time) DO NOTHING",
            Timestamp.from(result.slot()),result.monitorId(),result.orgId(),result.region(),result.outcome().name(),result.durationMs(),result.httpStatus(),
            result.errorCode()==null?null:result.errorCode().name(),result.errorMessage(),result.checkerInstance());
    }
}
