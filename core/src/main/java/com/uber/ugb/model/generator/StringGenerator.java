package com.uber.ugb.model.generator;

public class StringGenerator extends Generator<String> {

    private static String text = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    int minLength;
    int maxLength;

    public StringGenerator(int minLength, int maxLength) {
        this.minLength = minLength;
        this.maxLength = maxLength;
    }

    @Override
    protected String genValue() {
        int lengh = maxLength - minLength > 0 ? random.nextInt(maxLength - minLength) + minLength : minLength;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lengh; i++) {
            int x = random.nextInt(text.length());
            sb.append(text.charAt(x));
        }

        return sb.toString();
    }
}
