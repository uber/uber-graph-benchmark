package com.uber.ugb.measurement;

import java.io.Serializable;

public class StreamingStandardDeviation implements Serializable {

    // this piece of code is adapted from
    // http://obscuredclarity.blogspot.com/2012/08/running-average-and-running-standard.html

    private long count = 0;
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

    public StreamingStandardDeviation merge(StreamingStandardDeviation that) {
        long total = this.count + that.count;
        if (total > 0) {
            this.average = (this.average * this.count + that.average * that.count) / total;
            this.pwrSumAvg = (this.pwrSumAvg * this.count + that.pwrSumAvg * that.count) / total;
        }
        this.count = total;
        return this;
    }

    public double getAverage() {
        return average;
    }

    public double getStandardDeviation() {

        double stdDev = Math.sqrt((pwrSumAvg * count - count * average * average) / (count - 1));
        return Double.isNaN(stdDev) ? 0.0 : stdDev;
    }
}
