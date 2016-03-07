package org.ethereum.datasource;

import org.apache.commons.collections4.map.LRUMap;
import org.ethereum.util.ByteUtil;
import org.mapdb.DataIO;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Anton Nashatyrev on 04.03.2016.
 */
public class ObjectArray<V> {
    DatasourceArray src;
    LRUMap<Integer, V> cache = new LRUMap<>(256);

    Serializer<V> serializer;

    public ObjectArray(DatasourceArray src, Serializer<V> serializer) {
        this.src = src;
        this.serializer = serializer;
    }

    public void flush() {
        src.flush();
    }

    public synchronized int size() {
        return src.size();
    }

    public synchronized V get(int idx) {
        if (idx >= size()) return null;

        V v = cache.get(idx);
        if (v == null) {
            byte[] bytes = src.get(idx);
            try {
                v = serializer.deserialize(new DataIO.DataInputByteArray(bytes), bytes.length);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            cache.put(idx, v);
        }
        return v;
    }

    public synchronized V put(int idx, V value) {
        DataIO.DataOutputByteArray dout = new DataIO.DataOutputByteArray();
        try {
            serializer.serialize(dout, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        src.put(idx, dout.buf);
        cache.put(idx, value);
        return value;
    }
}
