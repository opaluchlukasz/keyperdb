package com.github.opaluchlukasz.keyperdb.infrastructure

import spock.lang.Specification

import static java.lang.System.lineSeparator

class EntryTest extends Specification {
    private static final String KEY = 'foo'
    private static final String VALUE = 'bar'

    def 'should create deleted entry'() {
        when:
        Entry entry = Entry.deleted(KEY)

        then:
        entry.isDeleted()
    }

    def 'should return byte representation of entry'() {
        when:
        Entry entry = Entry.of(KEY, VALUE)

        then:
        entry.asBytes() == "$KEY|:|$VALUE${lineSeparator()}".bytes
    }
}
