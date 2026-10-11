package io.github.besko2002.pulsewatch.monitor;
import org.springframework.boot.*;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.UUID;
@Configuration @Profile("dev")
public class DevSeed {
    @Bean ApplicationRunner seed(JdbcTemplate jdbc) { return args -> {
        jdbc.update("INSERT INTO organizations(id,name,slug) VALUES (?::uuid,?,?) ON CONFLICT DO NOTHING", "11111111-1111-1111-1111-111111111111", "Acme", "acme");
        jdbc.update("INSERT INTO organizations(id,name,slug) VALUES (?::uuid,?,?) ON CONFLICT DO NOTHING", "22222222-2222-2222-2222-222222222222", "Globex", "globex");
    }; }
}
