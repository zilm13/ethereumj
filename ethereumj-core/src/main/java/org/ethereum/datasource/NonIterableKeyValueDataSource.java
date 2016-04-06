package org.ethereum.datasource;

import java.util.Map;

/**
 * Created by Anton Nashatyrev on 10.03.2016.
 */
public interface NonIterableKeyValueDataSource extends DataSource {
    byte[] get(byte[] key);

    byte[] put(byte[] key, byte[] value);

    void delete(byte[] key);

    void updateBatch(Map<byte[], byte[]> rows);
}
