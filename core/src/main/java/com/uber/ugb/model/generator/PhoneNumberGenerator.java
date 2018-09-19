package com.uber.ugb.model.generator;

public class PhoneNumberGenerator extends Generator<String> {

    private static String text = "0123456789";

    @Override
    protected String genValue() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 3; i++) {
            int x = random.nextInt(text.length());
            sb.append(text.charAt(x));
        }
        sb.append("-");
        for (int i = 0; i < 3; i++) {
            int x = random.nextInt(text.length());
            sb.append(text.charAt(x));
        }
        sb.append("-");
        for (int i = 0; i < 4; i++) {
            int x = random.nextInt(text.length());
            sb.append(text.charAt(x));
        }

        return sb.toString();
    }
}
