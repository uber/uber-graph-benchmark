package com.uber.ugb.model.generator;

public class YearGenerator extends Generator<Long> {

    long min;
    long range;

    public YearGenerator(long min, long max) {
        this.min = min;
        this.range = max - min;
    }

    @Override
    protected Long genValue() {
        return Math.abs(random.nextLong()) / range + min;
    }
}
