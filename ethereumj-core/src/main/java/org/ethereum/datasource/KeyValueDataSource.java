package org.ethereum.datasource;

import java.util.Map;
import java.util.Set;

/**
 * @author Roman Mandeleil
 * @since 18.01.2015
 */
public interface KeyValueDataSource extends NonIterableKeyValueDataSource {

    Set<byte[]> keys();
}
