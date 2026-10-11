package io.github.besko2002.pulsewatch.monitor.monitors;
import io.github.besko2002.pulsewatch.support.tenancy.CurrentTenant;
import jakarta.validation.Valid;
import org.springframework.http.*;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;
@RestController @RequestMapping("/api/monitors")
public class MonitorController {
    private final MonitorRepository repository; private final MonitorPublisher publisher;
    public MonitorController(MonitorRepository repository, MonitorPublisher publisher) { this.repository=repository; this.publisher=publisher; }
    private void assertTenant(UUID orgId, UUID tenant) { if (!tenant.equals(orgId)) throw new ResponseStatusException(HttpStatus.NOT_FOUND); }
    private Monitor existing(UUID id, UUID tenant) {
        Monitor m=repository.findById(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
        assertTenant(m.getOrgId(),tenant); return m;
    }
    @PostMapping @Transactional
    public ResponseEntity<Monitor> create(@CurrentTenant UUID tenant, @Valid @RequestBody MonitorRequest request) {
        Monitor m=repository.saveAndFlush(new Monitor(UUID.randomUUID(),tenant,request.name(),request.url(),request.expectedStatus(),request.intervalSeconds()));
        publisher.publish(m); return ResponseEntity.status(HttpStatus.CREATED).body(m);
    }
    @GetMapping public List<Monitor> list(@CurrentTenant UUID tenant) { return repository.findAll().stream().filter(m -> m.getOrgId().equals(tenant)).toList(); }
    @GetMapping("/{id}") public Monitor get(@CurrentTenant UUID tenant, @PathVariable UUID id) { return existing(id,tenant); }
    @PutMapping("/{id}") @Transactional
    public Monitor update(@CurrentTenant UUID tenant, @PathVariable UUID id, @Valid @RequestBody MonitorRequest request) {
        Monitor m=existing(id,tenant); m.update(request.name(),request.url(),request.expectedStatus(),request.intervalSeconds());
        repository.flush(); publisher.publish(m); return m;
    }
    @DeleteMapping("/{id}") @ResponseStatus(HttpStatus.NO_CONTENT) @Transactional
    public void delete(@CurrentTenant UUID tenant, @PathVariable UUID id) { Monitor m=existing(id,tenant); repository.delete(m); publisher.delete(id); }
}
