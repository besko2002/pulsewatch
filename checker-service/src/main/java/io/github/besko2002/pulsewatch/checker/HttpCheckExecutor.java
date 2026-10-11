package io.github.besko2002.pulsewatch.checker;
import io.github.besko2002.pulsewatch.events.*;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.*;
import org.apache.hc.core5.util.Timeout;
import org.apache.hc.client5.http.config.RequestConfig;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.net.*;
import java.time.*;
@Component
public class HttpCheckExecutor {
    public CheckResult execute(MonitorConfig monitor, String region, Instant slot) {
        Instant start=Instant.now(); Integer status=null; CheckResult.ErrorCode error=null; String message=null;
        int timeout=Math.min(monitor.timeoutMs(),monitor.intervalSeconds()*500);
        RequestConfig config=RequestConfig.custom().setConnectTimeout(Timeout.ofMilliseconds(timeout))
            .setResponseTimeout(Timeout.ofMilliseconds(timeout)).build();
        try (CloseableHttpClient client=HttpClients.custom().setDefaultRequestConfig(config).disableRedirectHandling().build()) {
            HttpGet request=new HttpGet(monitor.settings().url());
            status=client.execute(request, response -> response.getCode());
            if (status!=monitor.settings().expectedStatus()) error=CheckResult.ErrorCode.STATUS_MISMATCH;
        } catch (Exception ex) {
            message=ex.getMessage();
            Throwable cause=ex;
            while (cause.getCause()!=null) cause=cause.getCause();
            if (cause instanceof java.net.SocketTimeoutException || cause instanceof java.net.http.HttpTimeoutException)
                error=CheckResult.ErrorCode.TIMEOUT;
            else if (cause instanceof java.net.ConnectException) error=CheckResult.ErrorCode.CONNECT_REFUSED;
            else if (cause instanceof UnknownHostException) error=CheckResult.ErrorCode.DNS;
            else error=CheckResult.ErrorCode.INTERNAL;
        }
        int duration=(int)Duration.between(start,Instant.now()).toMillis();
        return new CheckResult(monitor.monitorId(),monitor.orgId(),region,slot,start,duration,
            error==null ? CheckResult.Outcome.UP : CheckResult.Outcome.DOWN,status,error,message,null,"checker-"+region);
    }
}
