package org.ethereum.sync;

import org.ethereum.core.Block;
import org.ethereum.core.BlockHeaderWrapper;
import org.ethereum.db.ByteArrayWrapper;

import java.util.Collection;
import java.util.List;

/**
 * Created by Anton Nashatyrev on 27.05.2016.
 */
public interface SyncQueueIfc {

    /**
     * Wanted headers
     */
    interface HeadersRequest {

        long getStart();

        int getCount();

        boolean isReverse();
    }

    /**
     * Wanted blocks
     */
    interface BlocksRequest {
        List<BlocksRequest> split(int count);

        List<BlockHeaderWrapper> getBlockHeaders();
    }

    /**
     * Returns wanted headers request
     */
    HeadersRequest requestHeaders();


    // **** TODO: Refactor to detach versions
    /**
     * Wanted receipts
     */
    interface ReceiptsRequest {
        List<ReceiptsRequest> split(int count);

        List<byte[]> getReceiptsHashes();
    }

    /**
     * Returns wanted receipts request
     */
    ReceiptsRequest requestReceipts(int maxSize);
    /**
     * Wanted node data
     */
    interface NodeDataRequest {
        List<NodeDataRequest> split(int count);

        List<byte[]> getHashes();
    }

    /**
     * Returns wanted receipts request
     */
    NodeDataRequest requestNodeData(int maxSize);
    // **** TODO: Refactor to detach versions

    /**
     * Adds received headers.
     * Headers need to verified.
     * The list can be in any order and shouldn't correspond to prior headers request
     */
    void addHeaders(Collection<BlockHeaderWrapper> headers);

    /**
     * Returns wanted blocks hashes
     */
    BlocksRequest requestBlocks(int maxSize);

    /**
     * Adds new received blocks to the queue
     * The blocks need to be verified but can be passed in any order and need not correspond
     * to prior returned block request
     * @return  blocks ready to be imported in the valid import order.
     */
    List<Block> addBlocks(Collection<Block> blocks);

    /**
     * Returns approximate header count waiting for their blocks
     */
    int getHeadersCount();

    int getReceiptsCount();

    int getNodeDataCount();
}
