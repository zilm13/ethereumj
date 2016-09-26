package org.ethereum.sync;

import org.ethereum.net.server.ChannelManager;

/**
 *
 */
public interface SyncStrategy {

    void init(ChannelManager channelManager, SyncQueueIfc syncQueue, SyncPool pool, SyncManager syncManager);

    // TODO: Need something to push event when we ended

    void close();
}
