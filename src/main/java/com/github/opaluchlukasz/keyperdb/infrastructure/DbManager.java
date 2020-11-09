package com.github.opaluchlukasz.keyperdb.infrastructure;

import com.github.opaluchlukasz.keyperdb.api.DbException;
import com.github.opaluchlukasz.keyperdb.infrastructure.index.DbIndex;
import com.github.opaluchlukasz.keyperdb.infrastructure.io.DbStorage;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.SECONDS;

public class DbManager {
    private static final int COMPACTING_INTERVAL_IN_SECONDS = 60;

    private final DbIndex dbIndex;
    private final ScheduledExecutorService executor;

    public DbManager(String name) {
        try {
            DbStorage dbStorage = new DbStorage(name);
            this.dbIndex = new DbIndex(dbStorage);

            executor = Executors.newSingleThreadScheduledExecutor();
            CompactingJob compactingJob = new CompactingJob(dbIndex, dbStorage);
            executor.scheduleAtFixedRate(compactingJob, COMPACTING_INTERVAL_IN_SECONDS,
                    COMPACTING_INTERVAL_IN_SECONDS, SECONDS);
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

    public void delete(String key) {
        dbIndex.delete(key);
    }
}
