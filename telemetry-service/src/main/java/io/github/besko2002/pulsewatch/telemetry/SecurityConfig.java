package io.github.besko2002.pulsewatch.telemetry;
import io.github.besko2002.pulsewatch.support.security.TenantFilter;
import org.springframework.context.annotation.*;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
@Configuration
public class SecurityConfig {
    @Bean SecurityFilterChain chain(HttpSecurity http,TenantFilter filter) throws Exception {
        return http.csrf(csrf -> csrf.disable()).authorizeHttpRequests(a -> a.anyRequest().authenticated())
            .oauth2ResourceServer(o -> o.jwt(j -> {})).addFilterAfter(filter,BasicAuthenticationFilter.class).build();
    }
}
