package com.github.opaluchlukasz.keyperdb.api;

import com.github.opaluchlukasz.keyperdb.infrastructure.DbManager;

import java.util.Optional;

public class Keyper {
    private final DbManager dbManager;

    Keyper(String name) {
        this.dbManager = new DbManager(name);
    }

    public void put(String key, String value) {
        dbManager.put(key, value);
    }

    public void delete(String key) {
        dbManager.delete(key);
    }

    public Optional<String> get(String key) {
        return dbManager.get(key);
    }
}
