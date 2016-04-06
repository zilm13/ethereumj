package org.ethereum.datasource;

import org.ethereum.crypto.SHA3Helper;
import org.ethereum.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Anton Nashatyrev on 18.02.2016.
 */
public class XorDataSource implements KeyValueDataSource {
    private static final Logger logger = LoggerFactory.getLogger("db");

    KeyValueDataSource source;
    byte[] subKey;

    public XorDataSource(KeyValueDataSource source) {
        this.source = source;
    }
    public XorDataSource(KeyValueDataSource source, byte[] subKey) {
        this.source = source;
        this.subKey = subKey;
    }

    private byte[] convertKey(byte[] key) {
        return ByteUtil.xorAlignRight(key, subKey);
    }

    @Override
    public byte[] get(byte[] key) {
        return source.get(convertKey(key));
    }

    @Override
    public byte[] put(byte[] key, byte[] value) {
        return source.put(convertKey(key), value);
    }

    @Override
    public void delete(byte[] key) {
        source.delete(convertKey(key));
    }

    @Override
    public Set<byte[]> keys() {
        return Collections.emptySet();
//        throw new RuntimeException("Not implemented");
//        Set<byte[]> keys = source.keys();
//        HashSet<byte[]> ret = new HashSet<>(keys.size());
//        for (byte[] key : keys) {
//            ret.add(convertKey(key));
//        }
//        return ret;
    }

    @Override
    public void updateBatch(Map<byte[], byte[]> rows) {
        Map<byte[], byte[]> converted = new HashMap<>(rows.size());
        for (Map.Entry<byte[], byte[]> entry : rows.entrySet()) {
            converted.put(convertKey(entry.getKey()), entry.getValue());
        }
        source.updateBatch(converted);
    }

    @Override
    public void setName(String name) {
        source.setName(name);
    }

    @Override
    public String getName() {
        return source.getName();
    }

    @Override
    public void init() {
//        source.init();
        if (subKey == null) {
            subKey = SHA3Helper.sha3(getName().getBytes());
            logger.info("XorDataSource inited: " + getName());
        }
    }

    @Override
    public boolean isAlive() {
        return source.isAlive();
    }

    @Override
    public void close() {
        //source.close();
    }
}
