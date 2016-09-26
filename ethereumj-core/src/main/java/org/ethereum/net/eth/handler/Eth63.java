package org.ethereum.net.eth.handler;

import io.netty.channel.ChannelHandlerContext;
import org.ethereum.config.SystemProperties;
import org.ethereum.core.Block;
import org.ethereum.core.BlockHeaderWrapper;
import org.ethereum.core.Blockchain;
import org.ethereum.core.Transaction;
import org.ethereum.core.TransactionInfo;
import org.ethereum.core.TransactionReceipt;
import org.ethereum.db.ByteArrayWrapper;
import org.ethereum.listener.CompositeEthereumListener;
import org.ethereum.net.eth.EthVersion;
import org.ethereum.net.eth.message.EthMessage;
import org.ethereum.net.eth.message.GetNodeDataMessage;
import org.ethereum.net.eth.message.GetReceiptsMessage;
import org.ethereum.net.eth.message.NodeDataMessage;
import org.ethereum.net.eth.message.ReceiptsMessage;

import org.ethereum.sync.SyncState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.ethereum.crypto.HashUtil.sha3;
import static org.ethereum.net.eth.EthVersion.V63;
import static org.ethereum.sync.SyncState.*;

/**
 * Eth 63
 */
@Component
@Scope("prototype")
public class Eth63 extends Eth62 {

//    protected Queue<GetReceiptsMessageWrapper> receiptsRequests = new LinkedBlockingQueue<>();
    protected final List<ReceiptsWrapper> sentGetReceipts = Collections.synchronizedList(new ArrayList<ReceiptsWrapper>());
    protected final List<StateWrapper> sentNodeDataRequests = Collections.synchronizedList(new ArrayList<StateWrapper>());

    private static final EthVersion version = V63;

    public Eth63() {
        super(version);
    }

    @Autowired
    public Eth63(final SystemProperties config, final Blockchain blockchain,
                 final CompositeEthereumListener ethereumListener) {
        super(version, config, blockchain, ethereumListener);
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, EthMessage msg) throws InterruptedException {

        super.channelRead0(ctx, msg);

        // Only commands that were added in V63, V62 are handled in child with default no action
        switch (msg.getCommand()) {
            case GET_NODE_DATA:
                processGetNodeData((GetNodeDataMessage) msg);
                break;
            case NODE_DATA:
                processNodeData((NodeDataMessage) msg);
                break;
            case GET_RECEIPTS:
                processGetReceipts((GetReceiptsMessage) msg);
                break;
            case RECEIPTS:
                processReceipts((ReceiptsMessage) msg);
                break;
            default:
                break;
        }
    }

    protected synchronized void processGetNodeData(GetNodeDataMessage msg) {

        if(logger.isTraceEnabled()) logger.trace(
                "Peer {}: processing GetNodeData, size [{}]",
                channel.getPeerIdShort(),
                msg.getDataHashes().size()
        );
//        throw new RuntimeException("Not implemented yet");
    }

    protected synchronized void processNodeData(NodeDataMessage msg) {

        if(logger.isTraceEnabled()) logger.trace(
                "Peer {}: processing NodeData, size [{}]",
                channel.getPeerIdShort(),
                msg.getDataList().size()
        );

        // TODO: Validate response
        // TODO: key = sha3(value) validate
        // TODO: add states

        Map<ByteArrayWrapper, byte[]> states = new HashMap<>();
        for (byte[] state: msg.getDataList()) {
            ByteArrayWrapper stateRoot =  new ByteArrayWrapper(sha3(state));
            states.put(stateRoot, state);
        }
        List<StateWrapper> readyStates = new ArrayList<>();
        for (StateWrapper stateWrapper : sentNodeDataRequests) {
            byte[] state = states.get(stateWrapper.getStateRoot());
            if (state != null) {
                stateWrapper.setState(state);
                readyStates.add(stateWrapper);
            } else {
                // FIXME: we have gaps
            }
        }

        syncManager.addNodeData(readyStates, channel.getNodeId());

        syncState = IDLE;
    }

    protected synchronized void processGetReceipts(GetReceiptsMessage msg) {

        if(logger.isTraceEnabled()) logger.trace(
                "Peer {}: processing GetReceipts, size [{}]",
                channel.getPeerIdShort(),
                msg.getBlockHashes().size()
        );

        // FIXME: Untested implementation
        List<TransactionReceipt> receipts = new ArrayList<>();
        for (byte[] blockHash : msg.getBlockHashes()) {
            Block block = blockchain.getBlockByHash(blockHash);
            for (Transaction transaction : block.getTransactionsList()) {
                TransactionInfo transactionInfo = blockchain.getTransactionInfo(transaction.getHash());
                receipts.add(transactionInfo.getReceipt());
            }
        };

        sendMessage(new ReceiptsMessage(receipts));
    }

    protected synchronized void processReceipts(ReceiptsMessage msg) {

        if(logger.isTraceEnabled()) logger.trace(
                "Peer {}: processing Receipts, size [{}]",
                channel.getPeerIdShort(),
                msg.getReceipts().size()
        );

        // TODO: Validate: size, response/request, we could get receiptsTrieHash from receipts and compare
        // TODO: Add to blocks
        // TODO: Save blocks?

        syncManager.addReceipts(msg.getReceipts(), channel.getNodeId());

        syncState = IDLE;
    }

    public synchronized void sendGetReceipts(List<BlockHeaderWrapper> blockHeaders) {
        syncState = RECEIPTS_RETRIEVING;

        sentGetReceipts.clear();
        List<byte[]> blockHashes =  new ArrayList<>();
        List<ReceiptsWrapper> newReceipts = new ArrayList<>();
        for (BlockHeaderWrapper blockHeaderWrapper: blockHeaders) {
            byte[] blockHash = blockHeaderWrapper.getHeader().getHash();
            blockHashes.add(blockHash);
            newReceipts.add(new ReceiptsWrapper(new ByteArrayWrapper(blockHash), blockHeaderWrapper, channel.getNodeId()));
        }
        GetReceiptsMessage getReceiptsRequest = new GetReceiptsMessage(blockHashes);
        sentGetReceipts.addAll(newReceipts);

        if(logger.isTraceEnabled()) logger.trace(
                "Peer {}: send GetReceipts, hashes.count [{}]",
                channel.getPeerIdShort(),
                sentHeaders.size()
        );

        sendMessage(getReceiptsRequest);
    }

    public synchronized void sendGetNodeData(List<BlockHeaderWrapper> blockHeaders) {
        syncState = NODE_DATA_RETRIEVING;

        sentNodeDataRequests.clear();
        List<byte[]> stateRoots =  new ArrayList<>();
        List<StateWrapper> newStates = new ArrayList<>();
        for (BlockHeaderWrapper blockHeaderWrapper: blockHeaders) {
            byte[] stateRoot = blockHeaderWrapper.getHeader().getStateRoot();
            stateRoots.add(stateRoot);
            newStates.add(new StateWrapper(new ByteArrayWrapper(stateRoot), blockHeaderWrapper, channel.getNodeId()));
        }
        GetNodeDataMessage nodeDataRequest = new GetNodeDataMessage(stateRoots);
        sentNodeDataRequests.addAll(newStates);

        if(logger.isTraceEnabled()) logger.trace(
                "Peer {}: send GetNodeData, hashes.count [{}]",
                channel.getPeerIdShort(),
                sentHeaders.size()
        );

        sendMessage(nodeDataRequest);
    }
}
