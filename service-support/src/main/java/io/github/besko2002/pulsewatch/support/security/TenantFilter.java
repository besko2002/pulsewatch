package io.github.besko2002.pulsewatch.support.security;
import io.github.besko2002.pulsewatch.support.tenancy.TenantContext;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.filter.OncePerRequestFilter;
import java.io.IOException;
import java.util.UUID;
public class TenantFilter extends OncePerRequestFilter {
    @Override protected void doFilterInternal(HttpServletRequest request, jakarta.servlet.http.HttpServletResponse response, FilterChain chain) throws ServletException, IOException {
        try {
            if (SecurityContextHolder.getContext().getAuthentication() instanceof JwtAuthenticationToken token) {
                String org = token.getToken().getClaimAsString("org_id");
                if (org != null) TenantContext.set(UUID.fromString(org));
            }
            chain.doFilter(request, response);
        } finally { TenantContext.clear(); }
    }
}
