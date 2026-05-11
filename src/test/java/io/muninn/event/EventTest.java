package io.muninn.event;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EventTest {

    @Test
    void createEvent() {
        Event event = Event.create("trade.executed", "exchange-adapter", "AAPL", Map.of("price", 150.25));
        assertNotNull(event.eventId());
        assertNotNull(event.eventTime());
        assertEquals("trade.executed", event.eventType());
        assertEquals("exchange-adapter", event.source());
        assertEquals("AAPL", event.partitionKey());
    }

    @Test
    void rejectsNullEventType() {
        assertThrows(IllegalArgumentException.class, () ->
                Event.create(null, "source", "key", Map.of()));
    }

    @Test
    void rejectsNullPayload() {
        assertThrows(IllegalArgumentException.class, () ->
                Event.create("type", "source", "key", null));
    }

    @Test
    void payloadIsImmutable() {
        var mutable = new java.util.HashMap<String, Object>();
        mutable.put("key", "value");
        Event event = Event.create("type", "source", "key", mutable);
        assertThrows(UnsupportedOperationException.class, () ->
                event.payload().put("new", "value"));
    }
}
