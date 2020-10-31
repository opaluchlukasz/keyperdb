package com.github.opaluchlukasz.keyperdb.infrastructure;

import com.github.opaluchlukasz.keyperdb.api.DbException;
import com.github.opaluchlukasz.keyperdb.infrastructure.index.DbIndex;
import com.github.opaluchlukasz.keyperdb.infrastructure.io.DbStorage;

import java.util.Optional;

public class DbManager {
    private final DbIndex dbIndex;

    public DbManager(String name) {
        try {
            DbStorage dbStorage = new DbStorage(name);
            this.dbIndex = new DbIndex(dbStorage);
        } catch (Exception ex) {
            throw new DbException("Fatal exception", ex);
        }
    }

    public void put(String key, String value) {
        dbIndex.put(key, value);
    }

    public Optional<String> get(String key) {
        return dbIndex.getIndexed(key);
    }
}
