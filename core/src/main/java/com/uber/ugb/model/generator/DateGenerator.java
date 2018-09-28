package com.uber.ugb.model.generator;

public class DateGenerator extends Generator<String> {

    int min;
    int range;

    public DateGenerator(int minYear, int maxYear) {
        this.min = minYear;
        this.range = maxYear - min;
    }

    @Override
    protected String genValue() {
        int year = random.nextInt(range);
        int month = random.nextInt(12);
        int day = random.nextInt(30);
        return String.format("%4d-%2d-%2d", year + min, month + 1, day + 1);
    }
}
