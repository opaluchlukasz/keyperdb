package com.github.opaluchlukasz.keyperdb.api;

public final class KeyperFactory {

    private KeyperFactory() {
        //NOOP
    }

    //TODO keep instances in concurrenthashmap
    public static Keyper create(String name) {
        return new Keyper(name);
    }
}
