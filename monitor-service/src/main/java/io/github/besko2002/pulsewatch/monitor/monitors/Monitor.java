package io.github.besko2002.pulsewatch.monitor.monitors;
import jakarta.persistence.*;
import org.hibernate.annotations.TenantId;
import java.util.UUID;
@Entity @Table(name = "monitors")
public class Monitor {
    @Id private UUID id;
    @TenantId @Column(name="org_id", nullable=false) private UUID orgId;
    private String name;
    private String url;
    @Column(name="expected_status") private int expectedStatus;
    @Column(name="interval_seconds") private int intervalSeconds;
    @Version private long version;
    protected Monitor() {}
    public Monitor(UUID id, UUID orgId, String name, String url, int expectedStatus, int intervalSeconds) {
        this.id=id; this.orgId=orgId; update(name,url,expectedStatus,intervalSeconds);
    }
    public void update(String name, String url, int status, int interval) { this.name=name; this.url=url; this.expectedStatus=status; this.intervalSeconds=interval; }
    public UUID getId() { return id; } public UUID getOrgId() { return orgId; }
    public String getName() { return name; } public String getUrl() { return url; }
    public int getExpectedStatus() { return expectedStatus; } public int getIntervalSeconds() { return intervalSeconds; }
    public long getVersion() { return version; }
}
