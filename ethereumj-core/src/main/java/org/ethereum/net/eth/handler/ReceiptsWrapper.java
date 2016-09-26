package org.ethereum.net.eth.handler;

import org.ethereum.core.BlockHeaderWrapper;
import org.ethereum.core.TransactionReceipt;
import org.ethereum.db.ByteArrayWrapper;
import org.ethereum.util.RLP;
import org.spongycastle.util.encoders.Hex;

import java.util.Arrays;
import java.util.List;

/**
 * TODO: Move me where I should be
 */
public class ReceiptsWrapper {

    private ByteArrayWrapper hash;
    private BlockHeaderWrapper blockHeader;
    private List<TransactionReceipt> receipts;
    private byte[] nodeId;

    public ReceiptsWrapper(ByteArrayWrapper hash, BlockHeaderWrapper blockHeader, byte[] nodeId) {
        this.hash = hash;
        this.blockHeader = blockHeader;
        this.nodeId = nodeId;
    }

    public ReceiptsWrapper(byte[] bytes) {
        parse(bytes);
    }

    public byte[] getBytes() {
        byte[] headerBytes = hash.getData();
        byte[] nodeIdBytes = RLP.encodeElement(nodeId);
        return RLP.encodeList(headerBytes, nodeIdBytes);
    }

    public BlockHeaderWrapper getBlockHeader() {
        return blockHeader;
    }

    public void setBlockHeader(BlockHeaderWrapper blockHeader) {
        this.blockHeader = blockHeader;
    }

    public List<TransactionReceipt> getReceipts() {
        return receipts;
    }

    public void setReceipts(List<TransactionReceipt> receipts) {
        this.receipts = receipts;
    }

    private void parse(byte[] bytes) {
//        List<RLPElement> params = RLP.decode2(bytes);
//        List<RLPElement> wrapper = (RLPList) params.get(0);
//
//        byte[] headerBytes = wrapper.get(0).getRLPData();
//
//        this.header= new BlockHeader(headerBytes);
//        this.nodeId = wrapper.get(1).getRLPData();
    }

    public byte[] getNodeId() {
        return nodeId;
    }

//    public byte[] getHash() {
//        return header.getHash();
//    }
//
//    public long getNumber() {
//        return header.getNumber();
//    }
//
//    public BlockHeader getHeader() {
//        return header;
//    }
//
//    public String getHexStrShort() {
//        return Hex.toHexString(header.getHash()).substring(0, 6);
//    }

    public boolean sentBy(byte[] nodeId) {
        return Arrays.equals(this.nodeId, nodeId);
    }

    @Override
    public String toString() {
        return "ReceiptsWrapper {" +
                "hash=" + hash +
                ", nodeId=" + Hex.toHexString(nodeId) +
                '}';
    }
}
