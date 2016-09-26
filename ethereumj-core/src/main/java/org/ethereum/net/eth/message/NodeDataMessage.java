package org.ethereum.net.eth.message;

import org.ethereum.util.RLP;
import org.ethereum.util.RLPList;
import org.spongycastle.util.encoders.Hex;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper around an Ethereum NodeData message on the network
 *
 * @see EthMessageCodes#NODE_DATA
 */
public class NodeDataMessage extends EthMessage {

    private List<byte[]> dataList;

    public NodeDataMessage(byte[] encoded) {
        super(encoded);
    }

    public NodeDataMessage(List<byte[]> dataList) {
        this.dataList = dataList;
        parsed = true;
    }

    private void parse() {
        RLPList paramsList = (RLPList) RLP.decode2(encoded).get(0);

        dataList = new ArrayList<>();
        for (int i = 0; i < paramsList.size(); ++i) {
            // Shouldn't do anything with it
            dataList.add(paramsList.get(i).getRLPData());
        }
        parsed = true;
    }

    private void encode() {

        byte[][] encodedElementArray = dataList
                .toArray(new byte[dataList.size()][]);

        this.encoded = RLP.encodeList(encodedElementArray);
    }


    @Override
    public byte[] getEncoded() {
        if (encoded == null) encode();
        return encoded;
    }

    public List<byte[]> getDataList() {
        if (!parsed) parse();
        return dataList;
    }

    @Override
    public EthMessageCodes getCommand() {
        return EthMessageCodes.NODE_DATA;
    }

    @Override
    public Class<?> getAnswerMessage() {
        return null;
    }

    public String toString() {
        if (!parsed) parse();

        StringBuilder payload = new StringBuilder();

        payload.append("count( ").append(dataList.size()).append(" )");

        if (logger.isTraceEnabled()) {
            payload.append(" ");
            for (byte[] body : dataList) {
                payload.append(Hex.toHexString(body)).append(" | ");
            }
            if (!dataList.isEmpty()) {
                payload.delete(payload.length() - 3, payload.length());
            }
        }

        return "[" + getCommand().name() + " " + payload + "]";
    }
}
