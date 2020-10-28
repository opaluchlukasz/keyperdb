package com.github.opaluchlukasz.keyperdb.api;

import com.github.opaluchlukasz.keyperdb.infrastructure.FileManager;

import java.io.IOException;
import java.util.Optional;

public class Keyper {
    private final String name;
    private final FileManager fileManager;

    Keyper(String name) {
        //TODO start threads
        this.name = name;
        this.fileManager = new FileManager(name);
    }

    public void put(String key, String value) throws IOException {
        fileManager.put(key, value);
    }


    public Optional<String> get(String key) throws IOException {
        return fileManager.get(key);
    }
}
