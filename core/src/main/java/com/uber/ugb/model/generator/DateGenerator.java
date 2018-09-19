package com.uber.ugb.model.generator;

import java.util.Date;

public class DateGenerator extends Generator<Date> {

    int min;
    int range;

    public DateGenerator(int minYear, int maxYear) {
        this.min = minYear;
        this.range = maxYear - min;
    }

    @Override
    protected Date genValue() {
        int year = random.nextInt(range);
        int month = random.nextInt(12);
        int day = random.nextInt(30) + 1;
        return new Date(year + min - 1900, month, day);
    }
}
