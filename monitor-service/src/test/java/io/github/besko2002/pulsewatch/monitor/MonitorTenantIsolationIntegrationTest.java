package io.github.besko2002.pulsewatch.monitor;
import io.github.besko2002.pulsewatch.monitor.monitors.*;
import io.github.besko2002.pulsewatch.support.tenancy.TenantContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;
@SpringBootTest(properties="spring.kafka.admin.auto-create=false") @AutoConfigureMockMvc @Testcontainers(disabledWithoutDocker=true)
class MonitorTenantIsolationIntegrationTest {
    @Container @ServiceConnection static PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:16.9");
    @Autowired MockMvc mvc;
    @Autowired org.springframework.jdbc.core.JdbcTemplate jdbc;
    @MockitoBean MonitorPublisher publisher;
    @Test void otherTenantCannotReadMonitor() throws Exception {
        UUID a=UUID.randomUUID(), b=UUID.randomUUID();
        jdbc.update("INSERT INTO organizations(id,name,slug) VALUES (?,?,?)", a,"Tenant A",a.toString());
        jdbc.update("INSERT INTO organizations(id,name,slug) VALUES (?,?,?)", b,"Tenant B",b.toString());
        mvc.perform(post("/api/monitors").with(jwt().jwt(j -> j.claim("org_id",a.toString())))
            .contentType("application/json").content("""
                {"name":"site","url":"https://example.org","expectedStatus":200,"intervalSeconds":30}
                """)).andExpect(status().isCreated());
        UUID id=jdbc.queryForObject("SELECT id FROM monitors WHERE org_id=?",UUID.class,a);
        mvc.perform(get("/api/monitors/"+id).with(jwt().jwt(j -> j.claim("org_id",a.toString()))))
            .andExpect(status().isOk());
        mvc.perform(get("/api/monitors/"+id).with(jwt().jwt(j -> j.claim("org_id",b.toString()))))
            .andExpect(status().isNotFound());
        mvc.perform(get("/api/monitors").with(jwt().jwt(j -> j.claim("org_id",b.toString()))))
            .andExpect(status().isOk()).andExpect(content().json("[]"));
        mvc.perform(delete("/api/monitors/"+id).with(jwt().jwt(j -> j.claim("org_id",b.toString()))))
            .andExpect(status().isNotFound());
    }
}
