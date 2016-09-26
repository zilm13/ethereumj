package org.ethereum.net.eth.message;

import org.ethereum.core.Bloom;
import org.ethereum.core.Transaction;
import org.ethereum.core.TransactionReceipt;
import org.ethereum.util.RLP;
import org.ethereum.util.RLPElement;
import org.ethereum.util.RLPItem;
import org.ethereum.util.RLPList;
import org.ethereum.vm.LogInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper around an Ethereum Receipts message on the network
 *
 * @see EthMessageCodes#RECEIPTS
 */
public class ReceiptsMessage extends EthMessage {

    private List<TransactionReceipt> receipts;

    public ReceiptsMessage(byte[] encoded) {
        super(encoded);
    }

    public ReceiptsMessage(TransactionReceipt receipt) {

        receipts = new ArrayList<>();
        receipts.add(receipt);
        parsed = true;
    }

    public ReceiptsMessage(List<TransactionReceipt> receiptList) {
        this.receipts = receiptList;
        parsed = true;
    }

    private void parse() {
        RLPList paramsList = (RLPList) RLP.decode2(encoded).get(0);
        // TODO: Maybe this parsing should be in separate constructor of TransactionReceipt
        receipts = new ArrayList<>();
        for (int i = 0; i < paramsList.size(); ++i) {
            RLPList rlpTxData = (RLPList) paramsList.get(i);
            for (RLPElement innerElement : rlpTxData) {
                RLPList innerList = (RLPList) innerElement;
                if (innerList.size() != 4) {
                    continue;
                }

                byte [] postTxState = innerList.get(0).getRLPData();
                byte [] cGas = innerList.get(1).getRLPData();
                Bloom bloom = new Bloom(innerList.get(2).getRLPData());
                List<LogInfo> logInfos = new ArrayList<>();

                for (RLPElement logInfoEl: (RLPList) innerList.get(3)) {
                    LogInfo logInfo = new LogInfo(logInfoEl.getRLPData());
                    logInfos.add(logInfo);
                }

                TransactionReceipt receipt = new TransactionReceipt(postTxState, cGas, bloom, logInfos);
                receipts.add(receipt);
            }
        }
        parsed = true;
    }

    private void encode() {
        List<byte[]> encodedElements = new ArrayList<>();
        for (TransactionReceipt receipt : receipts)
            encodedElements.add(receipt.getEncoded(true));
        byte[][] encodedElementArray = encodedElements.toArray(new byte[encodedElements.size()][]);
        this.encoded = RLP.encodeList(encodedElementArray);
    }

    @Override
    public byte[] getEncoded() {
        if (encoded == null) encode();
        return encoded;
    }


    public List<TransactionReceipt> getReceipts() {
        if (!parsed) parse();
        return receipts;
    }

    @Override
    public EthMessageCodes getCommand() {
        return EthMessageCodes.RECEIPTS;
    }

    @Override
    public Class<?> getAnswerMessage() {
        return null;
    }

    public String toString() {
        if (!parsed) parse();
        final StringBuilder sb = new StringBuilder();
        if (receipts.size() < 4) {
            for (TransactionReceipt receipt : receipts)
                sb.append("\n   ").append(receipt.toString());
        } else {
            for (int i = 0; i < 3; i++) {
                sb.append("\n   ").append(receipts.get(i).toString());
            }
            sb.append("\n   ").append("[Skipped ").append(receipts.size() - 3).append(" receipts]");
        }
        return "[" + getCommand().name() + " num:"
                + receipts.size() + " " + sb.toString() + "]";
    }
}