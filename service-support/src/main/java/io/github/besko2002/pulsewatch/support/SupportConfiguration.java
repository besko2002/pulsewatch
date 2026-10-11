package io.github.besko2002.pulsewatch.support;
import io.github.besko2002.pulsewatch.support.tenancy.*;
import io.github.besko2002.pulsewatch.support.security.TenantFilter;
import io.github.besko2002.pulsewatch.support.errors.ApiErrorHandler;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.autoconfigure.orm.jpa.HibernatePropertiesCustomizer;
import org.springframework.web.method.support.*;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.core.MethodParameter;
import org.springframework.web.context.request.NativeWebRequest;
import java.util.*;
@AutoConfiguration
public class SupportConfiguration {
    @Bean TenantFilter tenantFilter() { return new TenantFilter(); }
    @Bean ApiErrorHandler apiErrorHandler() { return new ApiErrorHandler(); }
    @Bean WebMvcConfigurer tenantArgumentResolver() {
        return new WebMvcConfigurer() {
            @Override public void addArgumentResolvers(List<HandlerMethodArgumentResolver> resolvers) {
                resolvers.add(new HandlerMethodArgumentResolver() {
                    @Override public boolean supportsParameter(MethodParameter p) { return p.hasParameterAnnotation(CurrentTenant.class) && p.getParameterType() == UUID.class; }
                    @Override public Object resolveArgument(MethodParameter p, ModelAndViewContainer m, NativeWebRequest r, org.springframework.web.bind.support.WebDataBinderFactory b) {
                        return TenantContext.require();
                    }
                });
            }
        };
    }
    @Bean @ConditionalOnClass(HibernatePropertiesCustomizer.class)
    HibernatePropertiesCustomizer tenantHibernateCustomizer() {
        return properties -> properties.put("hibernate.tenant_identifier_resolver", new TenantResolver());
    }
}
