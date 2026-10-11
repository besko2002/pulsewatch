package io.github.besko2002.pulsewatch.support.tenancy;
import java.lang.annotation.*;
@Target(ElementType.PARAMETER) @Retention(RetentionPolicy.RUNTIME)
public @interface CurrentTenant {}
