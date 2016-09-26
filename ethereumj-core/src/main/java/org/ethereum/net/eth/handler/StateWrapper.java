package org.ethereum.net.eth.handler;

import org.ethereum.core.BlockHeaderWrapper;
import org.ethereum.db.ByteArrayWrapper;
import org.ethereum.util.RLP;
import org.ethereum.util.RLPElement;
import org.ethereum.util.RLPList;
import org.spongycastle.util.encoders.Hex;

import java.util.Arrays;
import java.util.List;

/**
 * TODO: Move me where I should be
 */
public class StateWrapper {

    private ByteArrayWrapper stateRoot;
    private BlockHeaderWrapper blockHeader;
    private byte[] state;
    private byte[] nodeId;

    public StateWrapper(ByteArrayWrapper stateRoot, BlockHeaderWrapper blockHeader, byte[] nodeId) {
        this.stateRoot = stateRoot;
        this.blockHeader = blockHeader;
        this.nodeId = nodeId;
    }

    public StateWrapper(byte[] bytes) {
        parse(bytes);
    }

    public byte[] getBytes() {
        byte[] headerBytes = stateRoot.getData();
        byte[] nodeIdBytes = RLP.encodeElement(nodeId);
        return RLP.encodeList(headerBytes, nodeIdBytes);
    }

    public BlockHeaderWrapper getBlockHeader() {
        return blockHeader;
    }

    public void setBlockHeader(BlockHeaderWrapper blockHeader) {
        this.blockHeader = blockHeader;
    }

    public ByteArrayWrapper getStateRoot() {
        return stateRoot;
    }

    public void setStateRoot(ByteArrayWrapper stateRoot) {
        this.stateRoot = stateRoot;
    }

    public byte[] getState() {
        return state;
    }

    public void setState(byte[] state) {
        this.state = state;
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
        return "StateWrapper {" +
                "stateRoot=" + stateRoot +
                ", nodeId=" + Hex.toHexString(nodeId) +
                '}';
    }
}
