package org.ethereum.sync;

import org.ethereum.core.Block;
import org.ethereum.core.BlockWrapper;
import org.ethereum.core.Blockchain;
import org.ethereum.core.FastBlockWrapper;
import org.ethereum.core.TransactionReceipt;
import org.ethereum.db.ByteArrayWrapper;
import org.ethereum.listener.CompositeEthereumListener;
import org.ethereum.net.eth.EthVersion;
import org.ethereum.net.eth.handler.Eth63;
import org.ethereum.net.eth.handler.ReceiptsWrapper;
import org.ethereum.net.eth.handler.StateWrapper;
import org.ethereum.net.server.Channel;
import org.ethereum.net.server.ChannelManager;
import org.ethereum.validator.BlockHeaderValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Scope("prototype")
@Component
public class FastSyncStrategy implements SyncStrategy {

    private static final int RECEIPTS_QUEUE_LIMIT = 20000;
    private static final int NODE_DATA_QUEUE_LIMIT = 20000;

    private static final EthVersion MIN_VERSION_SUPPORTED = EthVersion.V63;


    private final static Logger logger = LoggerFactory.getLogger("sync");

    private SyncPool pool;
    private ChannelManager channelManager;
    private SyncQueueIfc syncQueue;
    private SyncManager syncManager;

    private CountDownLatch receivedReceiptsLatch = new CountDownLatch(0);
    private Thread getReceiptsThread;

    private CountDownLatch receivedNodeDataLatch = new CountDownLatch(0);
    private Thread getNodeDataThread;

    private Map<ByteArrayWrapper, FastBlockWrapper> partialBlocks = Collections.synchronizedMap(new LinkedHashMap<ByteArrayWrapper, FastBlockWrapper> ());

    @Autowired
    private CompositeEthereumListener compositeEthereumListener;

    @Autowired
    private Blockchain blockchain;

    @Autowired
    private BlockHeaderValidator headerValidator;

    @Override
    public void init(final ChannelManager channelManager, SyncQueueIfc syncQueue, final SyncPool pool, final SyncManager syncManager) {
        this.pool = pool;
        this.channelManager = channelManager;
        this.syncQueue = syncQueue;
        this.syncManager = syncManager;

        getReceiptsThread = new Thread(new Runnable() {
            @Override
            public void run() {
                receiptsRetrieveLoop();
            }
        }, "NewSyncThreadReceipts");
        getReceiptsThread.start();

        getNodeDataThread = new Thread(new Runnable() {
            @Override
            public void run() {
                nodeDataRetrieveLoop();
            }
        }, "NewSyncThreadNodeData");
        getNodeDataThread.start();
    }

    private void receiptsRetrieveLoop() {
        while(!Thread.currentThread().isInterrupted()) {
            try {

                if (syncQueue.getReceiptsCount() < RECEIPTS_QUEUE_LIMIT) {
                    SyncQueueIfc.BlocksRequest hReq = syncQueue.requestBlocks(1000);
                    for (SyncQueueIfc.BlocksRequest receiptsRequest : hReq.split(100)) {

                        Channel any = pool.getAnyIdle(MIN_VERSION_SUPPORTED);
                        if (any != null) {
                            Eth63 ethHandler = (Eth63) any.getEthHandler();
                            logger.debug("receiptsRetrieveLoop: request receipts (" + receiptsRequest.getBlockHeaders().size() + ") from " + any.getNode());
                            ethHandler.sendGetReceipts(receiptsRequest.getBlockHeaders());
                        } else {
                            logger.debug("receiptsRetrieveLoop: No IDLE peers found");
                        }
                    }
                } else {
                    logger.debug("receiptsRetrieveLoop: ReceiptsQueue is full");
                }
                receivedReceiptsLatch = new CountDownLatch(1);
                receivedReceiptsLatch.await(syncManager.isSyncDone() ? 10000 : 2000, TimeUnit.MILLISECONDS);

            } catch (InterruptedException e) {
                break;
            } catch (Exception e) {
                logger.error("Unexpected: ", e);
            }
        }
    }

    private void nodeDataRetrieveLoop() {
        while(!Thread.currentThread().isInterrupted()) {
            try {
                if (syncQueue.getNodeDataCount() < NODE_DATA_QUEUE_LIMIT) {
                    SyncQueueIfc.BlocksRequest hReq = syncQueue.requestBlocks(1000);
                    for (SyncQueueIfc.BlocksRequest nodeDataRequest : hReq.split(100)) {

                        Channel any = pool.getAnyIdle(MIN_VERSION_SUPPORTED);

                        if (any != null) {
                            Eth63 ethHandler = (Eth63) any.getEthHandler();
                            logger.debug("nodeDataRetrieveLoop: request receipts (" + nodeDataRequest.getBlockHeaders().size() + ") from " + any.getNode());
                            ethHandler.sendGetNodeData(nodeDataRequest.getBlockHeaders());
                        } else {
                            logger.debug("nodeDataRetrieveLoop: No IDLE peers found");
                        }
                    }
                } else {
                    logger.debug("nodeDataRetrieveLoop: NodeDataQueue is full");
                }
                receivedNodeDataLatch = new CountDownLatch(1);
                receivedNodeDataLatch.await(syncManager.isSyncDone() ? 10000 : 2000, TimeUnit.MILLISECONDS);

            } catch (InterruptedException e) {
                break;
            } catch (Exception e) {
                logger.error("Unexpected: ", e);
            }
        }
    }

    public void addList(List<Block> blocks, byte[] nodeId) {
        for (Block block: blocks) {
            FastBlockWrapper blockWrapper = partialBlocks.get(new ByteArrayWrapper(block.getHash()));
            if (blockWrapper == null) {
                // TODO: ???
            } else {
                blockWrapper.setBlock(block);
                checkBlockFinished(blockWrapper);
            }
        }
    }

        // V63 consumer
    public void addReceipts(List<ReceiptsWrapper> receipts, byte[] nodeId) {
        for (ReceiptsWrapper receiptsWrapper: receipts) {
            FastBlockWrapper blockWrapper = partialBlocks.get(new ByteArrayWrapper(receiptsWrapper.getBlockHeader().getHash()));
            if (blockWrapper == null) {
                // TODO: ???
            } else {
                blockWrapper.setReceipts(receiptsWrapper.getReceipts());
                checkBlockFinished(blockWrapper);
            }
        }
    }

    public void addNodeData(List<StateWrapper> states, byte[] nodeId) {
        for (StateWrapper stateWrapper: states) {
            FastBlockWrapper blockWrapper = partialBlocks.get(new ByteArrayWrapper(stateWrapper.getBlockHeader().getHash()));
            if (blockWrapper == null) {
                // TODO: ???
            } else {
                blockWrapper.setState(stateWrapper.getState());
                checkBlockFinished(blockWrapper);
            }
        }
    }

    public void addBlockWrapper(BlockWrapper blockWrapper, byte[] nodeId) {
        FastBlockWrapper fastBlockWrapper = partialBlocks.get(new ByteArrayWrapper(blockWrapper.getHash()));
        if (fastBlockWrapper == null) {
            // TODO: ???
        } else {
            fastBlockWrapper.setBlock(blockWrapper.getBlock());
            checkBlockFinished(fastBlockWrapper);
        }
    }

    private void checkBlockFinished(FastBlockWrapper blockWrapper) {
        if (blockWrapper.isFilled()) {
            syncManager.addBlock(blockWrapper);
            partialBlocks.remove(blockWrapper.getBlockHash());
        }
    }

    @Override
    public void close() {
        try {
            if (getReceiptsThread != null) getReceiptsThread.interrupt();
            if (getNodeDataThread != null) getNodeDataThread.interrupt();
        } catch (Exception e) {
            logger.warn("Problems closing FullSync Strategy", e);
        }
    }
}
