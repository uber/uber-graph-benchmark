package com.uber.ugb.model.generator;

import java.math.BigDecimal;

public class DecimalGenerator extends Generator<BigDecimal> {

    double min;
    double max;
    double range;

    public DecimalGenerator(double min, double max){
        this.min = min;
        this.max = max;
        this.range = max - min;
    }

    @Override
    protected BigDecimal genValue() {
        return new BigDecimal(Math.abs(random.nextDouble()*this.range)+this.min);
    }
}
