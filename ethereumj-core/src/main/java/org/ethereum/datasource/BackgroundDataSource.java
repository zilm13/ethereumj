package org.ethereum.datasource;

import org.ethereum.db.ByteArrayWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Anton Nashatyrev on 18.02.2016.
 */
public class BackgroundDataSource implements KeyValueDataSource, Flushable {
    private static Logger logger = LoggerFactory.getLogger("db");

    KeyValueDataSource source;

    Map<ByteArrayWrapper, byte[]> cache = new HashMap<>();
    volatile Map<ByteArrayWrapper, byte[]> flushCache = new HashMap<>();

    ExecutorService flusher = Executors.newSingleThreadExecutor();

    public BackgroundDataSource(KeyValueDataSource source) {
        this.source = source;
    }

    public synchronized void flush() {
        if (!flushCache.isEmpty()) {
            logger.debug("Previous flush is in progress, skipping this flush");
            return;
        }
        flushCache = cache;
        cache = new HashMap<>();

        final Map<byte[], byte[]> records = new HashMap<>();
        for (Map.Entry<ByteArrayWrapper, byte[]> entry : flushCache.entrySet()) {
            records.put(entry.getKey().getData(), entry.getValue());
        }

        logger.debug("Submitting flush of " + flushCache.size() + " entries");
        flusher.submit(new Runnable() {
            @Override
            public void run() {
                try {

                    logger.debug("Executing flush of " + flushCache.size() + " entries");
                    source.updateBatch(records);
                    logger.debug("Complete flush of " + flushCache.size() + " entries");
                    flushCache = new HashMap<>();
                } catch (Exception e) {
                    logger.error("Error while doing flush task: ", e);
                }
            }
        });
    }

    @Override
    public synchronized byte[] get(byte[] key) {
        ByteArrayWrapper keyW = new ByteArrayWrapper(key);
        byte[] bb = cache.get(keyW);
        if (bb == null) {
            bb = flushCache.get(keyW);
            if (bb == null) {
                return source.get(key);
            } else {
                return bb;
            }
        } else {
            return bb;
        }
    }

    @Override
    public synchronized byte[] put(byte[] key, byte[] value) {
        return cache.put(new ByteArrayWrapper(key), value);
    }

    @Override
    public synchronized void delete(byte[] key) {
//        cache.remove(new ByteArrayWrapper(key));
        cache.put(new ByteArrayWrapper(key), null);
//        source.delete(key);
    }

    @Override
    public synchronized Set<byte[]> keys() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public synchronized void updateBatch(Map<byte[], byte[]> rows) {
        for (Map.Entry<byte[], byte[]> entry : rows.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
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
    }

    @Override
    public boolean isAlive() {
        return source.isAlive();
    }

    @Override
    public void close() {
//        source.close();
    }
}
