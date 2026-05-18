package io.muninn.ingestion.adapter;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CoinbaseExchangeAdapterTest {

    @Test
    void testAdapterLifecycle() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        CoinbaseExchangeAdapter adapter = new CoinbaseExchangeAdapter(registry);

        assertEquals("coinbase.pro.v1", adapter.source());

        // Verify startup
        assertDoesNotThrow(() -> adapter.start(event -> {}));
        
        // Verify multiple stops/closes are safe and idempotent
        assertDoesNotThrow(adapter::stop);
        assertDoesNotThrow(adapter::close);
    }
}
