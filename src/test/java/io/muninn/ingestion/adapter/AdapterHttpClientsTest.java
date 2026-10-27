package io.muninn.ingestion.adapter;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class AdapterHttpClientsTest {

    /**
     * Runs {@code task} on a thread created while the creating thread's context class
     * loader is {@code foreign}, as the JDK's own executor threads see the system class
     * loader, and returns the context class loader the task observed.
     */
    private static ClassLoader loaderSeenByCallback(ExecutorService executor, ClassLoader foreign)
            throws Exception {
        Thread caller = Thread.currentThread();
        ClassLoader original = caller.getContextClassLoader();
        caller.setContextClassLoader(foreign);
        try {
            return executor.submit(() -> Thread.currentThread().getContextClassLoader())
                    .get(5, TimeUnit.SECONDS);
        } finally {
            caller.setContextClassLoader(original);
            executor.shutdownNow();
        }
    }

    @Test
    void callbacksRunWithTheApplicationClassLoaderWhateverTheCreatorHad() throws Exception {
        // The production failure: callbacks ran with the system class loader, which in the
        // executable jar cannot see BOOT-INF/lib, so Kafka could not load its own classes
        // from a callback. A new thread inherits its creator's loader, so without the
        // explicit choice this test sees the foreign loader and fails.
        try (URLClassLoader foreign = new URLClassLoader(new URL[0], null)) {
            ClassLoader seen = loaderSeenByCallback(AdapterHttpClients.callbackExecutor("test"), foreign);

            assertThat(seen)
                    .isSameAs(AdapterHttpClients.class.getClassLoader())
                    .isNotSameAs(foreign);
        }
    }

    @Test
    void theApplicationClassLoaderCanSeeKafka() throws Exception {
        // The property the fix depends on, checked directly: the loader callbacks get is
        // one that resolves the class whose lookup failed in production.
        try (URLClassLoader foreign = new URLClassLoader(new URL[0], null)) {
            ClassLoader seen = loaderSeenByCallback(AdapterHttpClients.callbackExecutor("test"), foreign);

            assertThat(Class.forName(
                    "org.apache.kafka.common.security.oauthbearer.DefaultJwtRetriever", false, seen))
                    .isNotNull();
        }
    }

    @Test
    void callbackThreadsAreNamedForTheirVenueAndDoNotHoldTheJvmOpen() throws Exception {
        ExecutorService executor = AdapterHttpClients.callbackExecutor("okx.spot.v1");
        try {
            Thread thread = executor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
            assertThat(thread.getName()).startsWith("okx.spot.v1-ws-");
            assertThat(thread.isDaemon()).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void theClientRunsCallbacksOnThatExecutorNotTheJdkDefault() {
        HttpClient client = AdapterHttpClients.newWebSocketClient("binance.spot.v1");

        // An empty executor() means the JDK default, whose threads carry the system loader.
        assertThat(client.executor()).isPresent();
        assertThat(client.version()).isEqualTo(HttpClient.Version.HTTP_1_1);
    }
}
