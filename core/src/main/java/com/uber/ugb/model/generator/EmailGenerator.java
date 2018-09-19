package com.uber.ugb.model.generator;

public class EmailGenerator extends Generator<String> {

    private static String randomText = "abcdefghijklmnopqrstuvwxyz";
    private static String[] domains = {"gmail.com", "yahoo.com", "outlook.com", "inbox.com", "icloud.com", "mail.com"};

    @Override
    protected String genValue() {
        int lengh = random.nextInt(8) + 3;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lengh; i++) {
            int x = random.nextInt(randomText.length());
            sb.append(randomText.charAt(x));
        }
        sb.append("@");
        sb.append(domains[random.nextInt(domains.length)]);

        return sb.toString();
    }
}
