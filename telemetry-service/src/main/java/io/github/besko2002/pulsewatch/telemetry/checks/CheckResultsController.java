package io.github.besko2002.pulsewatch.telemetry.checks;
import io.github.besko2002.pulsewatch.support.tenancy.CurrentTenant;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.*;
@RestController @RequestMapping("/api/checks")
public class CheckResultsController {
    private final JdbcTemplate jdbc;
    public CheckResultsController(JdbcTemplate jdbc) { this.jdbc=jdbc; }
    @GetMapping("/{monitorId}/results")
    public List<Map<String,Object>> results(@CurrentTenant UUID tenant, @PathVariable UUID monitorId,
        @RequestParam Instant from, @RequestParam Instant to) {
        return jdbc.queryForList("SELECT time,monitor_id,region,outcome,latency_ms,http_status,error_code FROM check_results WHERE org_id=? AND monitor_id=? AND time>=? AND time<=? ORDER BY time DESC",
            tenant,monitorId,Timestamp.from(from),Timestamp.from(to));
    }
}
