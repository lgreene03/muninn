package io.muninn.replay.signal;

/**
 * Computes the Pearson Correlation Coefficient incrementally (Welford's algorithm).
 *
 * <p>Used in quantitative finance to measure the Information Coefficient (IC)
 * of a signal. IC is the correlation between the forecasted signal value and the
 * realized forward return.</p>
 */
public class PearsonCorrelationMetric {

    private int count = 0;
    private double meanX = 0;
    private double meanY = 0;
    private double m2X = 0;
    private double m2Y = 0;
    private double sumXY = 0;

    /**
     * Records a pair of (Signal Value, Forward Return).
     */
    public void add(double x, double y) {
        if (Double.isNaN(x) || Double.isNaN(y)) return;

        count++;
        double dx = x - meanX;
        double dy = y - meanY;
        
        meanX += dx / count;
        meanY += dy / count;
        
        m2X += dx * (x - meanX);
        m2Y += dy * (y - meanY);
        
        sumXY += dx * (y - meanY);
    }

    /**
     * Returns the current Pearson Correlation Coefficient [-1.0, 1.0].
     * If variance is zero or sample size is < 2, returns NaN.
     */
    public double correlation() {
        if (count < 2 || m2X == 0.0 || m2Y == 0.0) {
            return Double.NaN;
        }
        return sumXY / Math.sqrt(m2X * m2Y);
    }

    public int count() {
        return count;
    }
}
