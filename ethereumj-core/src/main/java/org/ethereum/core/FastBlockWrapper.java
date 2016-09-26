package org.ethereum.core;

import org.ethereum.db.ByteArrayWrapper;
import org.ethereum.net.eth.handler.StateWrapper;

import java.util.List;

/**
 * TODO: Too many files in this package, stop this!
 */
public class FastBlockWrapper {

    private ByteArrayWrapper blockHash;

    private Block block;
    private List<TransactionReceipt> receipts;
    private byte[] state;

    public FastBlockWrapper(ByteArrayWrapper blockHash) {
        this.blockHash = blockHash;
    }

    public boolean isFilled() {
        return block != null && receipts != null && state != null;
    }

    public ByteArrayWrapper getBlockHash() {
        return blockHash;
    }

    public void setBlockHash(ByteArrayWrapper blockHash) {
        this.blockHash = blockHash;
    }

    public Block getBlock() {
        return block;
    }

    public void setBlock(Block block) {
        this.block = block;
    }

    public List<TransactionReceipt> getReceipts() {
        return receipts;
    }

    public void setReceipts(List<TransactionReceipt> receipts) {
        this.receipts = receipts;
    }

    public byte[] getState() {
        return state;
    }

    public void setState(byte[] state) {
        this.state = state;
    }
}
