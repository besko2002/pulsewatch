package io.github.besko2002.pulsewatch.support.tenancy;
import org.hibernate.context.spi.CurrentTenantIdentifierResolver;
import java.util.UUID;
public class TenantResolver implements CurrentTenantIdentifierResolver<UUID> {
    @Override public UUID resolveCurrentTenantIdentifier() { return TenantContext.require(); }
    @Override public boolean validateExistingCurrentSessions() { return true; }
}
