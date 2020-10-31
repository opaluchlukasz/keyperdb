package com.github.opaluchlukasz.keyperdb.api;

import com.github.opaluchlukasz.keyperdb.infrastructure.DbManager;

import java.util.Optional;

public class Keyper {
    private final String name;
    private final DbManager dbManager;

    Keyper(String name) {
        this.name = name;
        this.dbManager = new DbManager(name);
    }

    public void put(String key, String value) {
        dbManager.put(key, value);
    }

    public Optional<String> get(String key) {
        return dbManager.get(key);
    }
}
