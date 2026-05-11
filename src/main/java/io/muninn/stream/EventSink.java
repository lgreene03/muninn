package io.muninn.stream;

import io.muninn.shared.event.MarketEvent;

/**
 * Accepts processed events from the stream processor.
 *
 * <p>In Phase 1, this is a no-op sink. In Phase 3, it will route events
 * to the feature engine. In Phase 5, it will also feed the Parquet writer.</p>
 */
public interface EventSink {

    void accept(MarketEvent event);
}
