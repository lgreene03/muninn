package io.muninn.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

/**
 * DuckDB embedded analytics database configuration.
 *
 * <p>DuckDB runs in-process with no separate server. The default configuration
 * uses an in-memory database for development; the {@code local-full} profile
 * can point to a persistent file.</p>
 */
@Configuration
public class DuckDbConfig {

    @ConfigurationProperties(prefix = "muninn.duckdb")
    public record DuckDbProperties(String path) {
        public DuckDbProperties {
            if (path == null || path.isBlank()) path = ":memory:";
        }
    }

    @Bean
    public DuckDbProperties duckDbProperties() {
        return new DuckDbProperties(":memory:");
    }

    @Bean("duckDbConnectionManager")
    public DuckDbConnectionManager duckDbConnectionManager(DuckDbProperties duckDbProperties) {
        return new DuckDbConnectionManager(duckDbProperties.path());
    }
}
