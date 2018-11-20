package com.uber.ugb.storage;

import java.util.List;

public interface PrefixKeyValueStore extends KeyValueStore {

    List<PrefixQueriedRow> scan(byte[] prefix, int limit);

    void put(byte[] keyPrefix, byte[] keySuffix, byte[] value);

    class PrefixQueriedRow {
        public final byte[] keySuffix;
        public final byte[] value;

        public PrefixQueriedRow(byte[] keySuffix, byte[] value) {
            this.keySuffix = keySuffix;
            this.value = value;
        }
    }

}
