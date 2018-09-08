package com.uber.ugsb.db;

/**
 * Something wrong when interacting with the database.
 */
public class DBException extends Exception {

    private static final long serialVersionUID = -633374420266238044L;

    public DBException(String message) {
        super(message);
    }

    public DBException(String message, Throwable cause) {
        super(message, cause);
    }

}
