package com.uber.ugb.storage;

import java.util.List;

public interface PrefixKeyValueStore extends KeyValueStore {

    List<Row> scan(byte[] prefix, int limit);

    class Row {
        public final byte[] key;
        public final byte[] value;

        public Row(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

}
