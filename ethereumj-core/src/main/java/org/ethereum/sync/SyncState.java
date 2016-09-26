package org.ethereum.sync;

/**
 * @author Mikhail Kalinin
 * @since 14.07.2015
 */
public enum SyncState {

    // Common
    IDLE,
    HASH_RETRIEVING,
    BLOCK_RETRIEVING,

    // TODO: More flexibility?
    NODE_DATA_RETRIEVING,
    RECEIPTS_RETRIEVING,

    // Peer
    DONE_HASH_RETRIEVING
}
