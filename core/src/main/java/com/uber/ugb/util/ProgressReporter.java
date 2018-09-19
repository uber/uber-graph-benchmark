package com.uber.ugb.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class ProgressReporter {

    private static Logger logger = Logger.getLogger(ProgressReporter.class.getName());

    String prefix;
    AtomicLong totalCount;
    boolean verbose;
    AtomicLong lastLogTime;
    AtomicLong lastCount;
    long countInterval;

    public ProgressReporter(String prefix, long totalCount, long countInterval) {
        this.prefix = prefix;
        this.totalCount = new AtomicLong(totalCount);
        this.verbose = true;
        this.lastLogTime = new AtomicLong(System.currentTimeMillis());
        this.lastCount = new AtomicLong();
        this.countInterval = countInterval;
    }

    public void maybeReport(long count) {
        if (verbose) {
            this.report(count);
        }
    }

    public void report(long count) {
        long now = System.currentTimeMillis();
        long deltaTime = now - lastLogTime.get();
        if (deltaTime > 2000L) {
            logger.info(String.format("%s:%d/%d %.1fk/s %.2f%%",
                prefix,
                count, totalCount.get(),
                (count - lastCount.get()) / (float) (deltaTime),
                (count * 100.0f / totalCount.get())
            ));
            lastLogTime.set(now);
            lastCount.set(count);
        }
    }
}
