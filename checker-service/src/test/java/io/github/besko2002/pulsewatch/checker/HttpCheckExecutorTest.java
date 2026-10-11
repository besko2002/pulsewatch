package io.github.besko2002.pulsewatch.checker;
import io.github.besko2002.pulsewatch.events.*;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.*;
import java.net.*;
import java.time.Instant;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;
class HttpCheckExecutorTest {
    HttpServer server;
    @BeforeEach void start() throws Exception {
        server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/ok",x -> {x.sendResponseHeaders(200,-1);x.close();});
        server.createContext("/bad",x -> {x.sendResponseHeaders(500,-1);x.close();});
        server.createContext("/slow",x -> {try {Thread.sleep(1500);} catch(InterruptedException e) {Thread.currentThread().interrupt();} x.sendResponseHeaders(200,-1);x.close();});
        server.start();
    }
    @AfterEach void stop() {server.stop(0);}
    CheckResult check(String path,int timeout) {
        MonitorConfig m=new MonitorConfig(UUID.randomUUID(),UUID.randomUUID(),"test",MonitorConfig.MonitorType.HTTP,30,timeout,
            new MonitorConfig.HttpSettings(URI.create("http://127.0.0.1:"+server.getAddress().getPort()+path),200),1);
        return new HttpCheckExecutor().execute(m,"local",Instant.now());
    }
    @Test void success() {assertThat(check("/ok",500).outcome()).isEqualTo(CheckResult.Outcome.UP);}
    @Test void badStatus() {assertThat(check("/bad",500).errorCode()).isEqualTo(CheckResult.ErrorCode.STATUS_MISMATCH);}
    @Test void timeout() {assertThat(check("/slow",200).errorCode()).isEqualTo(CheckResult.ErrorCode.TIMEOUT);}
    @Test void closedPort() {
        server.stop(0);
        assertThat(check("/ok",500).errorCode()).isEqualTo(CheckResult.ErrorCode.CONNECT_REFUSED);
    }
}
