package io.muninn.feature.microstructure;

import io.muninn.shared.event.OrderDeltaEvent;
import io.muninn.shared.event.Side;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class OrderBookL3Test {

    private OrderBookL3 orderBook;
    private final Instrument btcUsdt = new Instrument("BTC-USDT", "BTC", "USDT", new Exchange("coinbase", "Coinbase", ZoneId.of("UTC")));

    @BeforeEach
    void setUp() {
        orderBook = new OrderBookL3();
    }

    private OrderDeltaEvent createDelta(String orderId, Side side, double price, double quantity, OrderDeltaEvent.Action action) {
        return new OrderDeltaEvent(
                UUID.randomUUID(),
                Instant.now(),
                Instant.now(),
                "coinbase.v1",
                btcUsdt,
                1,
                1,
                orderId,
                side,
                price,
                quantity,
                action
        );
    }

    @Test
    void testAddOrderMaintainsBBO() {
        orderBook.applyDelta(createDelta("O1", Side.BUY, 60000.0, 1.0, OrderDeltaEvent.Action.ADD));
        orderBook.applyDelta(createDelta("O2", Side.BUY, 60010.0, 1.0, OrderDeltaEvent.Action.ADD));
        orderBook.applyDelta(createDelta("O3", Side.SELL, 60020.0, 1.0, OrderDeltaEvent.Action.ADD));
        orderBook.applyDelta(createDelta("O4", Side.SELL, 60015.0, 1.0, OrderDeltaEvent.Action.ADD));

        assertEquals(60010.0, orderBook.getBestBidPrice(), 0.001);
        assertEquals(60015.0, orderBook.getBestAskPrice(), 0.001);
    }

    @Test
    void testModifyOrder() {
        orderBook.applyDelta(createDelta("O1", Side.BUY, 60000.0, 2.0, OrderDeltaEvent.Action.ADD));
        // Modify implies updating the quantity (L3 definition varies, but here modify keeps price)
        orderBook.applyDelta(createDelta("O1", Side.BUY, 60000.0, 1.0, OrderDeltaEvent.Action.MODIFY));
        
        assertEquals(60000.0, orderBook.getBestBidPrice(), 0.001);
    }

    @Test
    void testDeleteOrderUpdatesBBO() {
        orderBook.applyDelta(createDelta("O1", Side.BUY, 60010.0, 1.0, OrderDeltaEvent.Action.ADD));
        orderBook.applyDelta(createDelta("O2", Side.BUY, 60000.0, 1.0, OrderDeltaEvent.Action.ADD));
        
        assertEquals(60010.0, orderBook.getBestBidPrice(), 0.001);

        orderBook.applyDelta(createDelta("O1", Side.BUY, 60010.0, 0.0, OrderDeltaEvent.Action.DELETE));
        
        assertEquals(60000.0, orderBook.getBestBidPrice(), 0.001);
    }
}
