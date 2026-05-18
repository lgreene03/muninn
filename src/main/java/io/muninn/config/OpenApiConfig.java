package io.muninn.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenAPI / Swagger UI configuration for the Muninn Query API.
 *
 * <p>Exposes documentation at {@code /swagger-ui.html} and the spec at
 * {@code /api-docs} as configured in {@code application.yml}.</p>
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI muninnOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Muninn Query API")
                        .version("v1")
                        .description("Read-only query API for feature time-series and replay jobs."))
                .addServersItem(new Server()
                        .url("http://localhost:8080")
                        .description("Local development"));
    }
}
