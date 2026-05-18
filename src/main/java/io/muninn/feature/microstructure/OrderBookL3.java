package io.muninn.feature.microstructure;

import io.muninn.shared.event.OrderDeltaEvent;
import io.muninn.shared.event.Side;

import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeMap;

/**
 * Deterministic L3 (Market-by-Order) Order Book.
 *
 * <p>Maintains full depth-of-market down to individual order precision.
 * Designed with mechanical sympathy in mind: avoids object allocation on the hot path
 * by pooling order nodes and using primitive data types where possible.</p>
 */
public class OrderBookL3 {

    private static final int MAX_ORDERS = 100_000;

    // Simple primitive arrays for zero-allocation open addressing (keys are string hashes for now, or longs if orderIds are numeric)
    // To keep this deterministic and simple for Phase 9, we use a hybrid approach:
    // A pre-allocated pool of OrderNodes.
    private final OrderNode[] nodePool = new OrderNode[MAX_ORDERS];
    private int nextFreeNode = 0;

    // Fast lookups by String ID (using standard Map for this iteration, but pool nodes)
    private final java.util.HashMap<String, OrderNode> orderMap = new java.util.HashMap<>(MAX_ORDERS);

    // Price levels for BBO extraction
    private final TreeMap<Double, PriceLevelL3> bids = new TreeMap<>(Comparator.reverseOrder());
    private final TreeMap<Double, PriceLevelL3> asks = new TreeMap<>();

    public OrderBookL3() {
        for (int i = 0; i < MAX_ORDERS; i++) {
            nodePool[i] = new OrderNode();
        }
    }

    public void applyDelta(OrderDeltaEvent delta) {
        switch (delta.action()) {
            case ADD -> addOrder(delta.orderId(), delta.side(), delta.price(), delta.quantity());
            case MODIFY -> modifyOrder(delta.orderId(), delta.quantity());
            case DELETE -> deleteOrder(delta.orderId());
        }
    }

    private void addOrder(String orderId, Side side, double price, double quantity) {
        if (orderMap.containsKey(orderId)) {
            return; // Idempotency for deterministic replay gaps
        }

        OrderNode node = allocateNode();
        node.orderId = orderId;
        node.side = side;
        node.price = price;
        node.quantity = quantity;

        orderMap.put(orderId, node);

        TreeMap<Double, PriceLevelL3> levels = (side == Side.BUY) ? bids : asks;
        PriceLevelL3 level = levels.computeIfAbsent(price, k -> new PriceLevelL3(price));
        level.addNode(node);
    }

    private void modifyOrder(String orderId, double newQuantity) {
        OrderNode node = orderMap.get(orderId);
        if (node != null) {
            TreeMap<Double, PriceLevelL3> levels = (node.side == Side.BUY) ? bids : asks;
            PriceLevelL3 level = levels.get(node.price);
            if (level != null) {
                level.totalQuantity -= node.quantity;
                level.totalQuantity += newQuantity;
                node.quantity = newQuantity;
            }
        }
    }

    private void deleteOrder(String orderId) {
        OrderNode node = orderMap.remove(orderId);
        if (node != null) {
            TreeMap<Double, PriceLevelL3> levels = (node.side == Side.BUY) ? bids : asks;
            PriceLevelL3 level = levels.get(node.price);
            if (level != null) {
                level.removeNode(node);
                if (level.isEmpty()) {
                    levels.remove(node.price);
                }
            }
            freeNode(node);
        }
    }

    public double getBestBidPrice() {
        return bids.isEmpty() ? Double.NaN : bids.firstKey();
    }

    public double getBestAskPrice() {
        return asks.isEmpty() ? Double.NaN : asks.firstKey();
    }

    private OrderNode allocateNode() {
        if (nextFreeNode < MAX_ORDERS) {
            return nodePool[nextFreeNode++];
        }
        // Fallback if pool exhausted
        return new OrderNode();
    }

    private void freeNode(OrderNode node) {
        // Simple pool recycling logic
        if (nextFreeNode > 0) {
            nodePool[--nextFreeNode] = node;
            node.reset();
        }
    }

    static class OrderNode {
        String orderId;
        Side side;
        double price;
        double quantity;
        OrderNode prev;
        OrderNode next;

        void reset() {
            orderId = null;
            prev = null;
            next = null;
        }
    }

    static class PriceLevelL3 {
        final double price;
        double totalQuantity;
        OrderNode head;
        OrderNode tail;

        PriceLevelL3(double price) {
            this.price = price;
        }

        void addNode(OrderNode node) {
            if (tail == null) {
                head = tail = node;
            } else {
                tail.next = node;
                node.prev = tail;
                tail = node;
            }
            totalQuantity += node.quantity;
        }

        void removeNode(OrderNode node) {
            if (node.prev != null) node.prev.next = node.next;
            if (node.next != null) node.next.prev = node.prev;
            if (head == node) head = node.next;
            if (tail == node) tail = node.prev;
            totalQuantity -= node.quantity;
            node.prev = null;
            node.next = null;
        }

        boolean isEmpty() {
            return head == null;
        }
    }
}
