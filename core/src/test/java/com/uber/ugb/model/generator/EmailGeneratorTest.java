package com.uber.ugb.model.generator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class EmailGeneratorTest {

    @Test
    public void testEmailGen() {
        EmailGenerator emailGenerator = new EmailGenerator();

        String x = (String) emailGenerator.generate(1, "User", 1, "email");
        String y = (String) emailGenerator.generate(1, "User", 1, "email");
        String z = (String) emailGenerator.generate(1, "User", 1, "emailAddress");

        assertEquals(x, y);
        assertNotEquals(x, z);

        String xx = (String) emailGenerator.generate(2, "User", 1, "email");
        assertNotEquals(x, xx);

        System.out.println("email:"+x);
        System.out.println("email:"+z);
        System.out.println("email:"+xx);

    }
}
