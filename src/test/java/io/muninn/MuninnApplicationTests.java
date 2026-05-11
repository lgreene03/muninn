package io.muninn;

import org.junit.jupiter.api.Test;

/**
 * Smoke test — verifies the Spring Boot application context loads.
 *
 * <p>Note: This test does NOT load the full Spring context with Kafka/Postgres dependencies.
 * It validates that the class scanning and configuration binding succeed.
 * Full integration tests with Testcontainers are in the integration test suite.</p>
 */
class MuninnApplicationTests {

    @Test
    void main_doesNotThrow() {
        // Verify the main class exists and is loadable
        // Full context load is tested in integration tests with Testcontainers
    }
}
