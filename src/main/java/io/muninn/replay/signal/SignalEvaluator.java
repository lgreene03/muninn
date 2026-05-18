package io.muninn.replay.signal;

import io.muninn.feature.FeatureComputer;
import io.muninn.feature.FeatureDefinition;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.TradeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedList;
import java.util.Map;

/**
 * Backtesting Harness for Signal Evaluation.
 *
 * <p>Strictly adheres to Muninn's NON_GOALS.md (No trading bot, no execution).
 * This class simply evaluates the statistical predictive power (Information Coefficient)
 * of a computed feature against actual forward returns observed in the replay stream.</p>
 */
public class SignalEvaluator {

    private static final Logger log = LoggerFactory.getLogger(SignalEvaluator.class);

    private final FeatureComputer featureComputer;
    private final FeatureDefinition definition;
    private final Duration forwardHorizon;
    private final String signalKey;

    private final PearsonCorrelationMetric icMetric = new PearsonCorrelationMetric();
    
    // Naive look-forward buffer for computing forward returns
    private final LinkedList<TradeEvent> priceBuffer = new LinkedList<>();
    private final LinkedList<SignalObservation> signalBuffer = new LinkedList<>();

    private record SignalObservation(MarketEvent event, double signalValue) {}

    public SignalEvaluator(FeatureComputer featureComputer, FeatureDefinition definition, Duration forwardHorizon, String signalKey) {
        this.featureComputer = featureComputer;
        this.definition = definition;
        this.forwardHorizon = forwardHorizon;
        this.signalKey = signalKey;
    }

    /**
     * Evaluates a stream of historical events, matching signals to their future returns.
     */
    public void evaluate(Iterable<MarketEvent> events) {
        for (MarketEvent event : events) {
            if (event instanceof TradeEvent trade) {
                priceBuffer.addLast(trade);
            }

            // Compute feature at this exact moment
            Map<String, Object> output = featureComputer.compute(definition, java.util.List.of(event));
            if (output.containsKey(signalKey)) {
                Object val = output.get(signalKey);
                if (val instanceof Number n) {
                    signalBuffer.addLast(new SignalObservation(event, n.doubleValue()));
                }
            }

            // Match old signals with current price if horizon has elapsed
            drainMatchedSignals(event);
        }
    }

    private void drainMatchedSignals(MarketEvent currentEvent) {
        while (!signalBuffer.isEmpty()) {
            SignalObservation obs = signalBuffer.peekFirst();
            Duration elapsed = java.time.Duration.between(obs.event().eventTime(), currentEvent.eventTime());
            
            if (elapsed.compareTo(forwardHorizon) >= 0) {
                signalBuffer.pollFirst();
                
                // Find execution price at signal time and resolution price at current time
                double entryPrice = findPriceAt(obs.event());
                double exitPrice = findPriceAt(currentEvent);
                
                if (!Double.isNaN(entryPrice) && !Double.isNaN(exitPrice)) {
                    double forwardReturn = (exitPrice - entryPrice) / entryPrice;
                    icMetric.add(obs.signalValue(), forwardReturn);
                }
            } else {
                break; // Not enough time has passed for this signal
            }
        }
    }

    private double findPriceAt(MarketEvent event) {
        // Find the closest trade price leading up to or at this event
        for (int i = priceBuffer.size() - 1; i >= 0; i--) {
            TradeEvent trade = priceBuffer.get(i);
            if (!trade.eventTime().isAfter(event.eventTime())) {
                return trade.price().doubleValue();
            }
        }
        return Double.NaN;
    }

    public double getInformationCoefficient() {
        return icMetric.correlation();
    }

    public int getObservationsCount() {
        return icMetric.count();
    }
}
