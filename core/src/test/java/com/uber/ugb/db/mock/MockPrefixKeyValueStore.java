package com.uber.ugb.db.mock;

import com.uber.ugb.storage.PrefixKeyValueStore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MockPrefixKeyValueStore extends MockKeyValueStore implements PrefixKeyValueStore {

    public MockPrefixKeyValueStore() {
        super();
    }

    @Override
    public List<Row> scan(byte[] prefix, int limit) {

        List<Row> out = new ArrayList<>();

        for (Map.Entry<ByteBuffer, byte[]> entry : super.kvs.entrySet()) {
            byte[] key = entry.getKey().array();
            if (hasPrefix(key, prefix)) {
                out.add(new Row(key, entry.getValue()));
            }
        }

        return out;

    }

    private boolean hasPrefix(byte[] text, byte[] prefix) {
        if (prefix == null) {
            return true;
        }
        if (text == null) {
            return false;
        }
        if (text.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (prefix[i] != text[i]) {
                return false;
            }
        }
        return true;
    }

}
