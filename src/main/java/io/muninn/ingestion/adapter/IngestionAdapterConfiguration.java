package io.muninn.ingestion.adapter;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the set of active {@link ExchangeAdapter}s based on per-exchange
 * {@code muninn.ingestion.<exchange>.enabled} flags.
 *
 * <p>Each adapter is conditionally instantiated — the cost of pulling in a
 * disabled exchange is one bean-definition evaluation, with no runtime
 * footprint. {@link io.muninn.ingestion.IngestionPipeline} injects the full
 * list and drives each.</p>
 *
 * <p>To add a third exchange:</p>
 * <ol>
 *   <li>Add a {@code <Name>Config} record with {@code @ConfigurationProperties}.</li>
 *   <li>Add a {@code <Name>ExchangeAdapter} implementing {@link ExchangeAdapter}.</li>
 *   <li>Add a {@code @Bean @ConditionalOnProperty} method here.</li>
 *   <li>Document the source identifier in
 *       {@code docs/steering/DOMAIN_MODEL.md §Exchange} and add a Flyway
 *       migration row to {@code V003__seed_reference_data.sql}.</li>
 * </ol>
 *
 * <p>See ADR-0008 for the framework rationale.</p>
 */
@Configuration
@EnableConfigurationProperties({BinanceConfig.class, CoinbaseConfig.class})
public class IngestionAdapterConfiguration {

    private static final Logger log = LoggerFactory.getLogger(IngestionAdapterConfiguration.class);

    @Bean
    @ConditionalOnProperty(name = "muninn.ingestion.binance.enabled", havingValue = "true")
    public ExchangeAdapter binanceAdapter(BinanceConfig config, MeterRegistry meterRegistry) {
        log.atInfo()
                .addKeyValue("source", "binance.spot.v1")
                .addKeyValue("instruments", config.instruments())
                .log("Registering Binance exchange adapter");
        return new BinanceWebSocketAdapter(config, meterRegistry);
    }

    @Bean
    @ConditionalOnProperty(name = "muninn.ingestion.coinbase.enabled", havingValue = "true")
    public ExchangeAdapter coinbaseAdapter(CoinbaseConfig config, MeterRegistry meterRegistry) {
        log.atInfo()
                .addKeyValue("source", "coinbase.pro.v1")
                .addKeyValue("instruments", config.instruments())
                .log("Registering Coinbase exchange adapter");
        return new CoinbaseExchangeAdapter(config, meterRegistry);
    }
}
