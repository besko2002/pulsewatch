package io.github.besko2002.pulsewatch.support.tenancy;
import java.util.UUID;
public final class TenantContext {
    private static final ThreadLocal<UUID> CURRENT = new ThreadLocal<>();
    private TenantContext() {}
    public static UUID require() {
        UUID id = CURRENT.get();
        if (id == null) throw new IllegalStateException("Tenant required");
        return id;
    }
    public static UUID get() { return CURRENT.get(); }
    public static void set(UUID id) { CURRENT.set(id); }
    public static void clear() { CURRENT.remove(); }
}
