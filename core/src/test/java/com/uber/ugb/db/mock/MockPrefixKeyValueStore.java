package com.uber.ugb.db.mock;

import com.uber.ugb.storage.PrefixKeyValueStore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MockPrefixKeyValueStore extends MockKeyValueStore implements PrefixKeyValueStore {

    public MockPrefixKeyValueStore() {
        super();
    }

    @Override
    public List<PrefixQueriedRow> scan(byte[] prefix, int limit) {

        List<PrefixQueriedRow> out = new ArrayList<>();

        for (Map.Entry<ByteBuffer, byte[]> entry : super.kvs.entrySet()) {
            byte[] key = entry.getKey().array();
            if (hasPrefix(key, prefix)) {
                out.add(new PrefixQueriedRow(Arrays.copyOfRange(key, prefix.length, key.length), entry.getValue()));
            }
        }

        return out;

    }

    @Override
    public void put(byte[] keyPrefix, byte[] keySuffix, byte[] value) {
        int len = keyPrefix.length + keySuffix.length;
        ByteBuffer key = ByteBuffer.allocate(len).put(keyPrefix).put(keySuffix);
        key.rewind();
        super.kvs.put(key, value);
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
