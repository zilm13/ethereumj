package org.ethereum.datasource;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import org.ethereum.db.ByteArrayWrapper;
import org.spongycastle.util.encoders.Hex;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Anton Nashatyrev on 07.03.2016.
 */
@Component
@Scope("prototype")
public class GemFireDataSource implements KeyValueDataSource {

    Region<String, byte[]> region;
    String name;

    public GemFireDataSource() {
    }

    public GemFireDataSource(String name) {
        this.name = name;
    }

    @Override
    public void init() {
        ClientCache cache = new ClientCacheFactory()
                .addPoolLocator("localhost", 10334)
                .create();
        ClientRegionFactory<String, byte[]> clientRegionFactory = cache.<String, byte[]>createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
        region = clientRegionFactory.create(name);
    }

    @Override
    public byte[] get(byte[] key) {
        return region.get(Hex.toHexString(key));
    }

    @Override
    public byte[] put(byte[] key, byte[] value) {
        region.put(Hex.toHexString(key), value);
        return value;
    }

    @Override
    public void delete(byte[] key) {
        region.remove(key);
    }

    @Override
    public Set<byte[]> keys() {
        Set<String> set = region.keySet();
        Set<byte[]> ret = new HashSet<>();
        for (String k : set) {
            ret.add(Hex.decode(k));
        }
        return ret;
    }

    @Override
    public void updateBatch(Map<byte[], byte[]> rows) {
        Map<String, byte[]> batch = new HashMap<>();
        for (Map.Entry<byte[], byte[]> entry : rows.entrySet()) {
            batch.put(Hex.toHexString(entry.getKey()), entry.getValue());
        }
        region.putAll(batch);
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isAlive() {
        return region != null;
    }

    @Override
    public void close() {
        region.close();
        region = null;
    }
}
