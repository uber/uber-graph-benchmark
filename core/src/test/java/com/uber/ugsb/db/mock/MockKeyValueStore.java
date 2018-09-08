package com.uber.ugsb.db.mock;

import com.uber.ugsb.storage.KeyValueStore;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MockKeyValueStore implements KeyValueStore {

    protected Map<ByteBuffer, byte[]> kvs;

    public MockKeyValueStore() {
        kvs = new HashMap<ByteBuffer, byte[]>();
    }

    @Override
    public byte[] get(byte[] key) {
        return kvs.get(ByteBuffer.wrap(key));
    }

    @Override
    public void put(byte[] key, byte[] value) {
        kvs.put(ByteBuffer.wrap(key), value);
    }

}
