package io.muninn.ingestion.adapter;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Builds the JDK {@link HttpClient} an exchange adapter opens its WebSocket with.
 *
 * <p>The client runs every listener callback ({@code onText} and the rest) on its own
 * executor, and the JDK's default executor gives those threads the <em>system</em> class
 * loader as their context class loader. Each callback hands the event straight to the
 * Kafka producer, and Kafka resolves class-typed configuration through the context class
 * loader. In the Spring Boot executable jar the system class loader cannot see the
 * libraries nested in {@code BOOT-INF/lib}, so the first send from a callback failed
 * inside {@code ProducerConfig}'s static initialiser ({@code DefaultJwtRetriever} "could
 * not be found"). A class whose initialiser fails is unusable for the life of the JVM,
 * so from then on every producer in the process failed: ingestion published nothing,
 * and each {@code NoClassDefFoundError} escaped the message handler and dropped the
 * socket, which reconnected and failed again every two seconds. The tests never saw it
 * because they run on a flat class path, where the system class loader sees everything.</p>
 *
 * <p>The fix is to run callbacks on threads whose context class loader is the one that
 * loaded the application, as every Spring-managed thread already has.</p>
 */
final class AdapterHttpClients {

    private AdapterHttpClients() {
    }

    /**
     * An HTTP/1.1 client whose callback threads carry the application class loader.
     *
     * @param source the adapter's source id, used to name its callback threads
     */
    static HttpClient newWebSocketClient(String source) {
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .executor(callbackExecutor(source))
                .build();
    }

    /** Package-private so the class loader guarantee can be tested without a socket. */
    static ExecutorService callbackExecutor(String source) {
        ClassLoader applicationLoader = AdapterHttpClients.class.getClassLoader();
        AtomicInteger count = new AtomicInteger();
        ThreadFactory factory = task -> {
            Thread thread = new Thread(task, source + "-ws-" + count.incrementAndGet());
            thread.setDaemon(true);
            thread.setContextClassLoader(applicationLoader);
            return thread;
        };
        return Executors.newCachedThreadPool(factory);
    }
}
