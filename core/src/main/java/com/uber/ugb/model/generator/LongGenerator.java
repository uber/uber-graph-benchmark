package com.uber.ugb.model.generator;

public class LongGenerator extends Generator<Long> {

    long min;
    long max;
    long range;

    public LongGenerator(long min, long max) {
        this.min = min;
        this.max = max;
        this.range = max - min;
    }

    @Override
    protected Long genValue() {
        return Math.abs(random.nextLong() % this.range) + this.min;
    }
}
