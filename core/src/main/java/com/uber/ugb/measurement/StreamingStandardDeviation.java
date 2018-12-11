/*
 *
 *  * Copyright 2018 Uber Technologies Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

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
