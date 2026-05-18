package io.muninn.config;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Unit tests for {@link DuckDbConnectionManager}.
 * Verifies HTTPFS loading and S3 session variable injection without Spring context.
 */
class DuckDbConnectionManagerTest {

    private static final StorageConfig.S3Properties TEST_S3 = new StorageConfig.S3Properties(
            "http://localhost:9002",
            "minioadmin",
            "minioadmin",
            "us-east-1"
    );

    @Test
    void getConnection_returnsNonNullConnection() throws Exception {
        DuckDbConnectionManager manager = new DuckDbConnectionManager(":memory:", TEST_S3);

        try (Connection conn = manager.getConnection()) {
            assertThat(conn).isNotNull();
            assertThat(conn.isClosed()).isFalse();
        }
    }

    @Test
    void getConnection_loadsHttpfsWithoutException() {
        DuckDbConnectionManager manager = new DuckDbConnectionManager(":memory:", TEST_S3);

        assertThatNoException().isThrownBy(() -> {
            try (Connection conn = manager.getConnection()) {
                // HTTPFS load is executed in getConnection(); if it throws, this propagates
                assertThat(conn).isNotNull();
            }
        });
    }

    @Test
    void getConnection_setsS3EndpointSessionVariable() throws Exception {
        DuckDbConnectionManager manager = new DuckDbConnectionManager(":memory:", TEST_S3);

        try (Connection conn = manager.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT current_setting('s3_endpoint')")) {

            assertThat(rs.next()).isTrue();
            // endpoint set should be host:port (without http:// prefix)
            assertThat(rs.getString(1)).isEqualTo("localhost:9002");
        }
    }

    @Test
    void getConnection_withNullS3Properties_loadsHttpfsOnly() {
        DuckDbConnectionManager manager = new DuckDbConnectionManager(":memory:", null);

        assertThatNoException().isThrownBy(() -> {
            try (Connection conn = manager.getConnection()) {
                assertThat(conn).isNotNull();
            }
        });
    }
}
