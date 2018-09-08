package com.uber.ugsb.storage;

public interface KeyValueStore {

    byte[] get(byte[] key);

    void put(byte[] key, byte[] value);

}
