package org.ethereum.db;

import org.apache.commons.collections.map.LRUMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;
import static org.ethereum.util.ByteUtil.wrap;

@Component
public class DetailsDataStore {

    @Autowired
    ApplicationContext ctx;

    private static final Logger gLogger = LoggerFactory.getLogger("general");

    private DatabaseImpl db = null;
    private Map<ByteArrayWrapper, ContractDetails> cache = new ConcurrentHashMap<>();
    private Map<ByteArrayWrapper, ContractDetails> readCache = new LRUMap(10000);
    private Set<ByteArrayWrapper> removes = new HashSet<>();

    public DetailsDataStore() {
    }

    public void setDB(DatabaseImpl db) {
        this.db = db;
    }

    public ContractDetails get(byte[] key) {
        return get(key, false);
    }
    public void fetch(byte[] key) {
        get(key, true);
    }

    private static final ContractDetailsImpl NULL = new ContractDetailsImpl();

    private synchronized ContractDetails get(byte[] key, boolean fetch) {

        ByteArrayWrapper wrappedKey = wrap(key);
        ContractDetails details = readCache.get(wrappedKey);

        if (details == null) {

            if (removes.contains(wrappedKey)) return null;
            byte[] data = db.get(key);
            if (data == null) {
                readCache.put(wrappedKey, NULL);
                return null;
            }

            details = ctx.getBean(ContractDetailsImpl.class);
            details.decode(data);

            readCache.put(wrappedKey, details);

            float out = ((float) data.length) / 1048576;
            if (out > 10) {
                String sizeFmt = format("%02.2f", out);
                gLogger.debug("loaded: key: " + Hex.toHexString(key) + " size: " + sizeFmt + "MB");
            }
        }


        return details == NULL ? null  : details;
    }

    public synchronized void update(byte[] key, ContractDetails contractDetails) {
        contractDetails.setAddress(key);

        ByteArrayWrapper wrappedKey = wrap(key);
        cache.put(wrappedKey, contractDetails);
        readCache.put(wrappedKey, contractDetails);
        removes.remove(wrappedKey);
    }

    public synchronized void remove(byte[] key) {
        ByteArrayWrapper wrappedKey = wrap(key);
        cache.remove(wrappedKey);
        readCache.remove(wrappedKey);
        removes.add(wrappedKey);
    }

    public synchronized void flush() {
        long keys = cache.size();

        long start = System.nanoTime();
        long totalSize = flushInternal();
        long finish = System.nanoTime();

        float flushSize = (float) totalSize / 1_048_576;
        float flushTime = (float) (finish - start) / 1_000_000;
        gLogger.info(format("Flush details in: %02.2f ms, %d keys, %02.2fMB", flushTime, keys, flushSize));
    }

    private long flushInternal() {
        long totalSize = 0;

        Map<byte[], byte[]> batch = new HashMap<>();
        for (Map.Entry<ByteArrayWrapper, ContractDetails> entry : cache.entrySet()) {
            ContractDetails details = entry.getValue();
            details.syncStorage();

            byte[] key = entry.getKey().getData();
            byte[] value = details.getEncoded();

            batch.put(key, value);
            totalSize += value.length;
        }

        db.getDb().updateBatch(batch);

        for (ByteArrayWrapper key : removes) {
            db.delete(key.getData());
        }

        cache.clear();
        removes.clear();

        return totalSize;
    }


    public synchronized Set<ByteArrayWrapper> keys() {
        Set<ByteArrayWrapper> keys = new HashSet<>();
        keys.addAll(cache.keySet());
        keys.addAll(db.dumpKeys());

        return keys;
    }


    private void temporarySave(String addr, byte[] data){
        try {
            FileOutputStream fos = new FileOutputStream(addr);
            fos.write(data);
            fos.close();
            System.out.println("drafted: " + addr);
        } catch (IOException e) {e.printStackTrace();}
    }
}
