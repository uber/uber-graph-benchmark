package com.uber.ugb.schema;

import com.uber.ugb.schema.model.Schema;

/**
 * An exception thrown when a schema, or a vocabulary of schemas, is found to be invalid
 */
@SuppressWarnings("serial")
public class InvalidSchemaException extends RuntimeException {

    public InvalidSchemaException(final String message) {
        super(message);
    }

    public InvalidSchemaException(final Throwable cause) {
        super(cause);
    }

    public InvalidSchemaException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public InvalidSchemaException(final Schema schema, final String message) {
        super(errorMessage(schema, message));
    }

    public InvalidSchemaException(final Schema schema, final String message, final Throwable cause) {
        super(errorMessage(schema, message), cause);
    }

    private static String errorMessage(final Schema schema, final String message) {
        return schema.getName() + " is invalid: " + message;
    }
}
