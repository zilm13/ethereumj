package org.ethereum.datasource;

import org.ethereum.util.ByteUtil;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by Anton Nashatyrev on 04.03.2016.
 */
public class DatasourceArray  {
    KeyValueDataSource db;
    static final byte[] sizeKey = {-1, -1, -1, -1, -1, -1, -1, -1, -1};
    int size = -1;

    public DatasourceArray(KeyValueDataSource db) {
        this.db = db;
    }

    public synchronized int size() {
        if (size < 0) {
            byte[] sizeBB = db.get(sizeKey);
            size = sizeBB == null ? 0 : ByteUtil.byteArrayToInt(sizeBB);
        }
        return size;
    }

    public synchronized byte[] get(int idx) {
        if (idx < 0 || idx >= size()) throw new IndexOutOfBoundsException(idx + " > " + size);
        return db.get(ByteUtil.intToBytes(idx));
    }

    public synchronized byte[] put(int idx, byte[] value) {
        if (idx >= size()) {
            setSize(idx + 1);
        }
        db.put(ByteUtil.intToBytes(idx), value);
        return value;
    }

    private synchronized void setSize(int newSize) {
        size = newSize;
        db.put(sizeKey, ByteUtil.intToBytes(newSize));
    }

    public void flush() {
        if (db instanceof CachingDataSource) {
            ((CachingDataSource) db).flush();
        }
    }
}
