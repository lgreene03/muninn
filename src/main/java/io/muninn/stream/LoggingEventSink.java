package io.muninn.stream;

import io.muninn.shared.event.MarketEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * No-op event sink for Phase 1. Logs received events at DEBUG level.
 *
 * <p>Replaced in Phase 3 with a feature-engine-connected implementation.</p>
 */
@Component
public class LoggingEventSink implements EventSink {

    private static final Logger log = LoggerFactory.getLogger(LoggingEventSink.class);

    @Override
    public void accept(MarketEvent event) {
        log.atDebug()
                .addKeyValue("eventId", event.eventId())
                .addKeyValue("type", event.getClass().getSimpleName())
                .addKeyValue("instrument", event.instrument().symbol())
                .log("Event received by sink");
    }
}
