package io.muninn.feature.engine.fastpath;

import io.muninn.shared.event.MarketEvent;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single-Producer Single-Consumer (SPSC) Ring Buffer.
 *
 * <p>Demonstrates mechanical sympathy:
 * 1. Zero allocation on the hot path (object pooling).
 * 2. Cache-line padding to prevent false sharing between the producer and consumer cursors.
 * 3. Power-of-two capacity for fast bitwise modulo operations.</p>
 */
public class SPSCRingBuffer {

    private final EventSlot[] buffer;
    private final int mask;

    // Cache line padding (typically 64 bytes or 128 bytes depending on CPU)
    // Prevents the producer and consumer sequences from sitting on the same cache line
    // and invalidating each other's L1 cache.

    private long p01, p02, p03, p04, p05, p06, p07, p08;
    private long p09, p10, p11, p12, p13, p14, p15, p16;
    private final AtomicLong tail = new AtomicLong(0); // Producer cursor

    private long q01, q02, q03, q04, q05, q06, q07, q08;
    private long q09, q10, q11, q12, q13, q14, q15, q16;
    private final AtomicLong head = new AtomicLong(0); // Consumer cursor

    public SPSCRingBuffer(int capacity) {
        if (Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("Capacity must be a power of 2");
        }
        this.mask = capacity - 1;
        this.buffer = new EventSlot[capacity];
        for (int i = 0; i < capacity; i++) {
            this.buffer[i] = new EventSlot();
        }
    }

    /**
     * Producer writes an event. Zero allocation.
     */
    public boolean offer(MarketEvent event) {
        long currentTail = tail.get();
        long currentHead = head.get();

        if (currentTail - currentHead == buffer.length) {
            return false; // Queue is full
        }

        EventSlot slot = buffer[(int) (currentTail & mask)];
        slot.event = event;
        
        // Publish to consumer
        tail.lazySet(currentTail + 1);
        return true;
    }

    /**
     * Consumer reads an event. Zero allocation.
     */
    public MarketEvent poll() {
        long currentHead = head.get();
        
        if (currentHead == tail.get()) {
            return null; // Queue is empty
        }

        EventSlot slot = buffer[(int) (currentHead & mask)];
        MarketEvent event = slot.event;
        slot.event = null; // Help GC
        
        // Publish to producer
        head.lazySet(currentHead + 1);
        return event;
    }

    /**
     * Pre-allocated mutable slot to avoid object churn.
     */
    private static class EventSlot {
        MarketEvent event;
    }
}
