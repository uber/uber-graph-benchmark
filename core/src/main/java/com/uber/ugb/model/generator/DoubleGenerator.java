package com.uber.ugb.model.generator;

public class DoubleGenerator extends Generator<Double> {

    double min;
    double max;
    double range;

    public DoubleGenerator(double min, double max) {
        this.min = min;
        this.max = max;
        this.range = max - min;
    }

    @Override
    protected Double genValue() {
        return Math.abs(random.nextDouble() * this.range) + this.min;
    }
}
