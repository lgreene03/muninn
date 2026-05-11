package io.muninn.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
public class DuckDbConfig {

    @Value("${muninn.duckdb.path::memory:}")
    private String duckDbPath;

    @Bean("duckDbDataSource")
    public DataSource duckDbDataSource() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.duckdb.DuckDBDriver");
        ds.setUrl("jdbc:duckdb:" + duckDbPath);
        return ds;
    }
}
