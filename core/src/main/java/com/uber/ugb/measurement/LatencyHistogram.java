package com.uber.ugb.measurement;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class LatencyHistogram implements Measurement {
    private final long[] usBuckets;
    private final long[] msBuckets;
    private final long[] secBuckets;
    private final String name;
    private long overflowCount;
    private AtomicLong operations;
    private AtomicLong totalLatencyNs;
    private long minNs;
    private long maxNs;
    private StreamingStandardDeviation std;

    public LatencyHistogram(String name) {
        this.name = name;
        usBuckets = new long[2000];
        msBuckets = new long[2000];
        secBuckets = new long[3600];
        minNs = -1;
        maxNs = -1;
        this.operations = new AtomicLong();
        this.totalLatencyNs = new AtomicLong();
        this.std = new StreamingStandardDeviation();
    }

    @Override
    public boolean hasData() {
        return this.operations.get() > 0;
    }

    /**
     * measure collects latency in nano seconds
     *
     * @param latencyNs
     */
    @Override
    public void measure(long latencyNs) {
        long usBucket = latencyNs / 1000;
        if (usBucket < usBuckets.length) {
            usBuckets[(int) usBucket]++;
        } else {
            long msBucket = usBucket / 1000;
            if (msBucket < msBuckets.length) {
                msBuckets[(int) msBucket]++;
            } else {
                long secBucket = msBucket / 1000;
                if (secBucket < secBuckets.length) {
                    secBuckets[(int) secBucket]++;
                } else {
                    overflowCount++;
                }
            }
        }

        operations.incrementAndGet();
        totalLatencyNs.addAndGet(latencyNs);

        if ((minNs < 0) || (latencyNs < minNs)) {
            minNs = latencyNs;
        }

        if ((maxNs < 0) || (latencyNs > maxNs)) {
            maxNs = latencyNs;
        }
        std.put(latencyNs);
    }

    @Override
    public void measure(Runnable runnable) {
        long start = System.nanoTime();
        try {
            runnable.run();
        } finally {
            this.measure(System.nanoTime() - start);
        }
    }

    @Override
    public void printout(MetricsOutput out) throws IOException {
        double operationCount = (double) operations.get();
        double meanNs = totalLatencyNs.get() / operationCount;
        double variance = std.getStandardDeviation();
        out.write(name, "Operations", operations.get());
        out.write(name, "Average(us)", meanNs / 1000d);
        out.write(name, "Variance(us)", variance / 1000d);
        out.write(name, "Min(us)", minNs / 1000d);
        out.write(name, "Max(us)", maxNs / 1000d);

        long opcounter = 0;
        boolean done95th = false;
        boolean done99th = false;
        for (int i = 0; i < usBuckets.length; i++) {
            opcounter += usBuckets[i];
            if ((!done95th) && (((double) opcounter) / operationCount >= 0.95)) {
                out.write(name, "95thPercentile(us)", i);
                done95th = true;
            }
            if ((!done99th) && ((double) opcounter) / operationCount >= 0.99) {
                out.write(name, "99thPercentile(us)", i);
                done99th = true;
                break;
            }
        }
        for (int i = 0; i < msBuckets.length; i++) {
            opcounter += msBuckets[i];
            if ((!done95th) && (((double) opcounter) / operationCount >= 0.95)) {
                out.write(name, "95thPercentile(ms)", i);
                done95th = true;
            }
            if ((!done99th) && ((double) opcounter) / operationCount >= 0.99) {
                out.write(name, "99thPercentile(ms)", i);
                done99th = true;
                break;
            }
        }
        for (int i = 0; i < secBuckets.length; i++) {
            opcounter += secBuckets[i];
            if ((!done95th) && (((double) opcounter) / operationCount >= 0.95)) {
                out.write(name, "95thPercentile(second)", i);
                done95th = true;
            }
            if ((!done99th) && ((double) opcounter) / operationCount >= 0.99) {
                out.write(name, "99thPercentile(second)", i);
                break;
            }
        }

    }

}
