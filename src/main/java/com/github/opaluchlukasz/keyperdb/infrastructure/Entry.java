package com.github.opaluchlukasz.keyperdb.infrastructure;

import java.util.Objects;

import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.regex.Pattern.quote;

public final class Entry {
    private static final String ENTRY_SEPARATOR = "|:|";
    private static final String ENTRY_SEPARATOR_PATTERN = quote(ENTRY_SEPARATOR);

    private final String key;
    private final String value;

    private Entry(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public static Entry fromLine(String line) {
        String[] split = line.split(ENTRY_SEPARATOR_PATTERN);
        return new Entry(split[0], split[1]);
    }

    public static Entry of(String key, String value) {
        return new Entry(key, value);
    }

    public String key() {
        return key;
    }

    public String value() {
        return value;
    }

    public byte[] asBytes() {
        return (key + ENTRY_SEPARATOR + value + lineSeparator()).getBytes(UTF_8);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Entry entry = (Entry) o;
        return Objects.equals(key, entry.key) &&
                Objects.equals(value, entry.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}
