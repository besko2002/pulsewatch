package io.github.besko2002.pulsewatch.monitor.monitors;
import io.github.besko2002.pulsewatch.monitor.SecurityConfig;
import io.github.besko2002.pulsewatch.support.security.TenantFilter;
import io.github.besko2002.pulsewatch.support.tenancy.TenantContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.util.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
@WebMvcTest(MonitorController.class) @Import({SecurityConfig.class, TenantFilter.class})
class MonitorControllerTest {
    @Autowired MockMvc mvc;
    @MockitoBean MonitorRepository repository;
    @MockitoBean MonitorPublisher publisher;
    private static final UUID ORG=UUID.fromString("11111111-1111-1111-1111-111111111111");
    @Test void requiresJwt() throws Exception { mvc.perform(get("/api/monitors")).andExpect(status().isUnauthorized()); }
    @Test void validatesBody() throws Exception {
        mvc.perform(post("/api/monitors").with(jwt().jwt(j -> j.claim("org_id",ORG.toString())))
            .contentType("application/json").content("""
                {"name":"site","url":"https://example.org","expectedStatus":200,"intervalSeconds":5}
                """))
            .andExpect(status().isBadRequest());
    }
    @Test void readsTenantFromJwtNotRequestHeader() throws Exception {
        when(repository.findAll()).thenAnswer(i -> { org.junit.jupiter.api.Assertions.assertEquals(ORG,TenantContext.require()); return List.of(); });
        mvc.perform(get("/api/monitors").header("X-Org-Id",UUID.randomUUID()).with(jwt().jwt(j -> j.claim("org_id",ORG.toString()))))
            .andExpect(status().isOk()).andExpect(content().json("[]"));
        verify(repository).findAll();
    }
}
