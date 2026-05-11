package io.muninn.stream;

import io.muninn.event.EventEnvelope;

public interface EventSink {

    void accept(EventEnvelope envelope);
}
