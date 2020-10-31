package com.github.opaluchlukasz.keyperdb.api;

public class DbException extends RuntimeException {
    public DbException(String message, Exception cause) {
        super(message, cause);
    }
}
