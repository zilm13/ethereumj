package org.ethereum.sync;

import org.ethereum.config.SystemProperties;
import org.ethereum.core.*;
import org.ethereum.listener.CompositeEthereumListener;
import org.ethereum.listener.EthereumListener;
import org.ethereum.net.eth.handler.StateWrapper;
import org.ethereum.net.server.ChannelManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

import static org.ethereum.core.ImportResult.IMPORTED_BEST;
import static org.ethereum.core.ImportResult.IMPORTED_NOT_BEST;
import static org.ethereum.core.ImportResult.NO_PARENT;

/**
 * @author Mikhail Kalinin
 * @since 14.07.2015
 */
@Component
public class SyncManager {

    enum SyncType {
        FULL,
        FAST
    }

    private final static Logger logger = LoggerFactory.getLogger("sync");

    private boolean syncDone = false;

    private long lastKnownBlockNumber = 0;

    @Autowired
    private Blockchain blockchain;

    @Autowired
    EthereumListener ethereumListener;

    @Autowired
    ApplicationContext ctx;

    ChannelManager channelManager;

    private SystemProperties config;

    private SyncPool pool;

    private SyncQueueIfc syncQueue;

    private ScheduledExecutorService logExecutor = Executors.newSingleThreadScheduledExecutor();

    private Map<Class, SyncStrategy> activeStrategies = new HashMap<>();

    // TODO: Is validation the same here for both fast and full sync??
    /**
     * Queue with validated blocks to be added to the blockchain
     */
    private BlockingQueue<BlockWrapper> blockQueue = new LinkedBlockingQueue<>();

    private BlockingQueue<FastBlockWrapper> fastBlockQueue = new LinkedBlockingDeque<>();

    private Thread syncQueueThread;

    private SyncType syncType = SyncType.FAST;

    @Autowired
    private CompositeEthereumListener compositeEthereumListener;

    public SyncManager() {
    }

    @Autowired
    public SyncManager(final SystemProperties config) {
        this.config = config;
    }

    public void init(final ChannelManager channelManager, final SyncPool pool) {
        this.pool = pool;
        this.channelManager = channelManager;
        if (!config.isSyncEnabled()) {
            logger.info("Sync Manager: OFF");
            return;
        }
        logger.info("Sync Manager: ON");

        logger.info("Initializing SyncManager.");
        pool.init(channelManager);

        syncQueue = new SyncQueueImpl(blockchain);


        Runnable queueProducer = new Runnable(){

            @Override
            public void run() {
                produceQueue();
            }
        };

        syncQueueThread = new Thread (queueProducer, "SyncQueueThread");
        syncQueueThread.start();

        // TODO: Pass it from config or command line?
        SyncType syncType = SyncType.FAST;

        for (Map.Entry<Class, SyncStrategy> entry: fetchStrategies(syncType).entrySet()) {
            SyncStrategy strategy = entry.getValue();
            strategy.init(channelManager, syncQueue, pool, this);
            activeStrategies.put(entry.getKey(), strategy);
        }

        if (logger.isInfoEnabled()) {
            startLogWorker();
        }
    }

    // TODO: There will be some logic
    private Map<Class, SyncStrategy> fetchStrategies(SyncType syncType) {
        Map<Class, SyncStrategy> strategies = new HashMap<>();
        SyncStrategy fullSync = ctx.getBean(FullSyncStrategy.class);
        strategies.put(FullSyncStrategy.class, fullSync);
        if (syncType.equals(SyncType.FAST)) {
            SyncStrategy fastSync = ctx.getBean(FastSyncStrategy.class);
            strategies.put(FastSyncStrategy.class, fastSync);
        }

        return strategies;
    }

    // V62/63 consumer
    public void addList(List<Block> blocks, byte[] nodeId) {
        if (syncType.equals(SyncType.FULL)) {
            SyncStrategy fullSync = getActive(FullSyncStrategy.class);
            if (fullSync != null) ((FullSyncStrategy) fullSync).addList(blocks, nodeId);
        } else if (syncType.equals(SyncType.FAST)) {
            SyncStrategy fastSync = getActive(FastSyncStrategy.class);
            if (fastSync != null) ((FastSyncStrategy) fastSync).addList(blocks, nodeId);
        }
    }

    // V62 consumer
    public boolean validateAndAddNewBlock(Block block, byte[] nodeId) {
        SyncStrategy fullSync = getActive(FullSyncStrategy.class);
        return fullSync != null && ((FullSyncStrategy) fullSync).validateAndAddNewBlock(block, nodeId);
    }

    // V62 consumer
    public boolean validateAndAddHeaders(List<BlockHeader> headers, byte[] nodeId) {
        SyncStrategy fullSync = getActive(FullSyncStrategy.class);
        return fullSync != null && ((FullSyncStrategy) fullSync).validateAndAddHeaders(headers, nodeId);
    }

    // V63 consumer
    public void addReceipts(List<TransactionReceipt> receipts, byte[] nodeId) {
        SyncStrategy fastSync = getActive(FastSyncStrategy.class);
        if (fastSync != null) ((FastSyncStrategy) fastSync).addReceipts(receipts, nodeId);
    }

    // V63 consumer
    public void addNodeData(List<StateWrapper> states, byte[] nodeId) {
        SyncStrategy fastSync = getActive(FastSyncStrategy.class);
        if (fastSync != null) ((FastSyncStrategy) fastSync).addNodeData(states, nodeId);
    }

    /**
     * Processing the queue adding blocks to the chain.
     */
    private void produceQueue() {

        while (!Thread.currentThread().isInterrupted()) {

            BlockWrapper wrapper = null;
            try {

                // TODO: we need either pre-processing queue to store block until receipts and nodedata arrives
                // or other way to wait until all block data will be filled
                wrapper = blockQueue.take();

                logger.debug("BlockQueue size: {}, headers queue size: {}", blockQueue.size(), syncQueue.getHeadersCount());

                // TODO: Maybe this logic should be in syncType implementations?
                ImportResult importResult = null;
                switch (syncType) {
                    case FULL:
                        importResult = blockchain.tryToConnect(wrapper.getBlock());
                        break;
                    // TODO: Move to another queue?
                    case FAST:
                        fastBlockQueue.put(wrapper.getBlock());
                        return;
                }

                if (importResult == IMPORTED_BEST) {
                    logger.info("Success importing BEST: block.number: {}, block.hash: {}, tx.size: {} ",
                            wrapper.getNumber(), wrapper.getBlock().getShortHash(),
                            wrapper.getBlock().getTransactionsList().size());

                    if (wrapper.isNewBlock() && !isSyncDone()) {
                        setSyncDone(true);
                        channelManager.onSyncDone(true);
                        compositeEthereumListener.onSyncDone();
                    }
                }

                if (importResult == IMPORTED_NOT_BEST)
                    logger.info("Success importing NOT_BEST: block.number: {}, block.hash: {}, tx.size: {} ",
                            wrapper.getNumber(), wrapper.getBlock().getShortHash(),
                            wrapper.getBlock().getTransactionsList().size());

                if (isSyncDone() && (importResult == IMPORTED_BEST || importResult == IMPORTED_NOT_BEST)) {
                    if (logger.isDebugEnabled()) logger.debug("Block dump: " + Hex.toHexString(wrapper.getBlock().getEncoded()));
                }

                // In case we don't have a parent on the chain
                // return the try and wait for more blocks to come.
                if (importResult == NO_PARENT) {
                    logger.error("No parent on the chain for block.number: {} block.hash: {}",
                            wrapper.getNumber(), wrapper.getBlock().getShortHash());
                }

            } catch (InterruptedException e) {
                break;
            } catch (Throwable e) {
                logger.error("Error processing block {}: ", wrapper.getBlock().getShortDescr(), e);
                logger.error("Block dump: {}", Hex.toHexString(wrapper.getBlock().getEncoded()));
            }

        }
    }

    public void addBlockWrapper(BlockWrapper blockWrapper) {
        if (syncType.equals(SyncType.FULL)) {
            getBlockQueue().add(blockWrapper);
        } else if (syncType.equals(SyncType.FAST)) {
            SyncStrategy fastSync = getActive(FastSyncStrategy.class);
            if (fastSync != null) ((FastSyncStrategy) fastSync).addBlockWrapper(blockWrapper, nodeId);
        }
    }

    private SyncStrategy getActive(Class clazz) {
        if (activeStrategies.containsKey(clazz)) return activeStrategies.get(clazz);
        return null;
    }

    public boolean isSyncDone() {
        return syncDone;
    }

    public void setSyncDone(boolean syncDone) {
        this.syncDone = syncDone;
    }

    private void startLogWorker() {
        logExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    pool.logActivePeers();
                    logger.info("\n");
                } catch (Throwable t) {
                    t.printStackTrace();
                    logger.error("Exception in log worker", t);
                }
            }
        }, 0, 30, TimeUnit.SECONDS);
    }

    public long getLastKnownBlockNumber() {
        return lastKnownBlockNumber;
    }

    public void setLastKnownBlockNumber(long lastKnownBlockNumber) {
        this.lastKnownBlockNumber = lastKnownBlockNumber;
    }

    public BlockingQueue<BlockWrapper> getBlockQueue() {
        return blockQueue;
    }
    public BlockingQueue<FastBlockWrapper> getFastBlockQueue() {
        return fastBlockQueue;
    }

    public void close() {
        pool.close();
        try {
            for (SyncStrategy strategy: activeStrategies.values()) {
                strategy.close();
            }
            if (syncQueueThread != null) syncQueueThread.interrupt();
            logExecutor.shutdown();
        } catch (Exception e) {
            logger.warn("Problems closing SyncManager", e);
        } finally {
            activeStrategies.clear();
        }
    }
}
