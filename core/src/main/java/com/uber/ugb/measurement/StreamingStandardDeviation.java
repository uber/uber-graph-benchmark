package com.uber.ugb.measurement;

public class StreamingStandardDeviation {

    // this piece of code is adapted from
    // http://obscuredclarity.blogspot.com/2012/08/running-average-and-running-standard.html

    private int count = 0;
    private double average = 0.0;
    private double pwrSumAvg = 0.0;

    /**
     * Incoming new values used to calculate the running statistics
     *
     * @param value
     */
    public void put(double value) {

        count++;
        average += (value - average) / count;
        pwrSumAvg += (value * value - pwrSumAvg) / count;

    }

    public double getAverage() {
        return average;
    }

    public double getStandardDeviation() {

        double stdDev = Math.sqrt((pwrSumAvg * count - count * average * average) / (count - 1));
        return Double.isNaN(stdDev) ? 0.0 : stdDev;
    }
}
