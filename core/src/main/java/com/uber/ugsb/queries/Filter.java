package com.uber.ugsb.queries;

public class Filter {
    public final String field;
    public final Operator operator;
    public final String value;

    public Filter(String field, Operator operator, String value) {
        this.field = field;
        this.operator = operator;
        this.value = value;
    }

    public Object getValueObject() {
        Object valueObject = null;
        // support either single quoted string or integers
        if (this.value.startsWith("'") && this.value.endsWith("'")) {
            valueObject = this.value.substring(1, this.value.length() - 1);
        } else {
            valueObject = Integer.parseInt(this.value);
        }
        return valueObject;
    }

    public enum Operator {
        GreaterThan, Equal, LessThan
    }
}
