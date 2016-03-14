package org.ethereum.datasource;

import com.google.common.util.concurrent.SettableFuture;
import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.MyDataCenterInstanceConfig;
import com.netflix.discovery.DefaultEurekaClientConfig;
import com.netflix.discovery.DiscoveryManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.CompleteFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongycastle.util.encoders.Hex;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Anton Nashatyrev on 10.03.2016.
 */
public class RemoteDataSource implements KeyValueDataSource {
    private static Logger logger = LoggerFactory.getLogger("db");

    NonIterableKeyValueDataSource src;

    ChannelHandlerContext clientChannel;
    AtomicInteger ids = new AtomicInteger(1);

    String name;

    public RemoteDataSource(NonIterableKeyValueDataSource src) {
        this.src = src;
    }

    public RemoteDataSource() {
    }

    @Override
    public Set<byte[]> keys() {
        return Collections.emptySet();
    }

    @Override
    public byte[] get(byte[] key) {
        ByteBuf bb = clientChannel.alloc().buffer();
        bb.writeInt(1);
        int id = ids.getAndIncrement();
        bb.writeInt(id);
        writeByteArrays(bb, new byte[][]{key});
        SettableFuture<byte[][]> ret = SettableFuture.<byte[][]>create();
        waiters.put(id, ret);
        clientChannel.writeAndFlush(bb);
        try {
            byte[][] bytes = ret.get();
            return bytes[0];
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] put(byte[] key, byte[] value) {
        ByteBuf bb = clientChannel.alloc().buffer();
        bb.writeInt(2);
        int id = ids.getAndIncrement();
        bb.writeInt(id);
        writeByteArrays(bb, new byte[][]{key, value});
        clientChannel.writeAndFlush(bb);
        return value;
    }

    @Override
    public void delete(byte[] key) {
        ByteBuf bb = clientChannel.alloc().buffer();
        bb.writeInt(3);
        int id = ids.getAndIncrement();
        bb.writeInt(id);
        writeByteArrays(bb, new byte[][]{key});
        clientChannel.writeAndFlush(bb);
    }

    @Override
    public void updateBatch(Map<byte[], byte[]> rows) {
        ByteBuf bb = clientChannel.alloc().buffer();
        bb.writeInt(2);
        int id = ids.getAndIncrement();
        bb.writeInt(id);
        byte[][] data = new byte[rows.size() * 2][];
        int i = 0;
        for (Map.Entry<byte[], byte[]> entry : rows.entrySet()) {
            data[i++] = entry.getKey();
            data[i++] = entry.getValue();
        }
        writeByteArrays(bb, data);
        clientChannel.writeAndFlush(bb);
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void init() {

    }

    @Override
    public boolean isAlive() {
        return true;
    }

    @Override
    public void close() {

    }

    Map<Integer, SettableFuture<byte[][]>> waiters = Collections.synchronizedMap(new HashMap<Integer, SettableFuture<byte[][]>>());

    private class ClientHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            clientChannel = ctx;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf bb = (ByteBuf) msg;
            int id = bb.readInt();
            SettableFuture<byte[][]> f = waiters.remove(id);
            f.set(readByteArrays(bb));
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            logger.warn("Channel inactive: " + ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("Client error: ", cause);
        }
    }

    private class ServerHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf bb = (ByteBuf) msg;
            int cmd = bb.readInt();
            int id = bb.readInt();
            byte[][] data = new byte[0][];
            int dataLen = 0;
            logger.trace(" => " + id + ": " + cmd);
            switch (cmd) {
                case 1:  // get
                {
                    byte[][] keys = readByteArrays(bb);
                    data = new byte[keys.length][];
                    for (int i = 0; i < keys.length; i++) {
                        data[i] = src.get(keys[i]);
                        if (data[i] != null) {
                            dataLen += data[i].length;
                        }
                    }
                    break;
                }
                case 2:  // put (batch)
                {
                    byte[][] keysVals = readByteArrays(bb);
                    if (keysVals.length == 2) {
                        src.put(keysVals[0], keysVals[1]);
                    } else {
                        Map<byte[], byte[]> map = new LinkedHashMap<>();
                        for (int i = 0; i < keysVals.length; i += 2) {
                            map.put(keysVals[i], keysVals[i + 1]);
                        }
                        src.updateBatch(map);
                    }
                    break;
                }
                case 3: // delete
                {
                    byte[][] keys = readByteArrays(bb);
                    for (int i = 0; i < keys.length; i++) {
                        src.delete(keys[i]);
                    }
                    break;
                }
            }
            if (data.length > 0) {
                ByteBuf respBuf = ctx.alloc().buffer(dataLen + 4);
                respBuf.writeInt(id);
                writeByteArrays(respBuf, data);
                ctx.writeAndFlush(respBuf);
                logger.trace("<=  " + id + ": " + cmd);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            logger.warn("Channel inactive: " + ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("Server error: ", cause);
        }
    }

    CountDownLatch active = new CountDownLatch(1);
    public void waitForActive() {
        try {
            active.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[][] readByteArrays(ByteBuf bb) {
        int cnt = bb.readInt();
        byte[][] ret = new byte[cnt][];
        for (int i = 0; i < cnt; i++) {
            int len = bb.readInt();
            if (len >= 0) {
                ret[i] = new byte[len];
                bb.readBytes(ret[i]);
            } else {
                ret[i] = null;
            }
        }
        return ret;
    }

    private void writeByteArrays(ByteBuf bb, byte[][] data) {
        bb.writeInt(data.length);
        for (int i = 0; i < data.length; i++) {
            if (data[i] != null) {
                bb.writeInt(data[i].length);
                bb.writeBytes(data[i]);
            } else {
                bb.writeInt(-1);
            }
        }
    }

    public void startServer(int port) {
        logger.info("Staring server...");
        EventLoopGroup bossGroup = new NioEventLoopGroup(); // (1)
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap(); // (2)
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class) // (3)
                    .childHandler(new ChannelInitializer<SocketChannel>() { // (4)
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new LengthFieldPrepender(4));
                            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                            ch.pipeline().addLast(new ServerHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)          // (5)
                    .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(port).sync(); // (7)

            if (!f.isSuccess()) throw new RuntimeException("Can't connect");

            active.countDown();
            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public void startClient(String host, int port) {
        logger.info("Staring client...");
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap(); // (1)
            b.group(workerGroup); // (2)
            b.channel(NioSocketChannel.class); // (3)
            b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new LengthFieldPrepender(4));
                    ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                    ch.pipeline().addLast(new ClientHandler());
                }
            });

            // Start the client.
            ChannelFuture f = b.connect(host, port).sync(); // (5)

            if (!f.isSuccess()) throw new RuntimeException("Can't connect");

        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
//            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        logger.info("Creating DB.");
        LevelDbDataSource levelDbDataSource = new LevelDbDataSource();
        levelDbDataSource.setName("remote");
        levelDbDataSource.init();

        final MyDataCenterInstanceConfig instConf = new MyDataCenterInstanceConfig("db.") {
            @Override
            public String getVirtualHostName() {
                String dbName = System.getenv("DB_NAME");
                if (dbName == null) {
                    return super.getVirtualHostName();
                } else {
                    return dbName + ".test.ethereumj.org";
                }
            }

            @Override
            public String getInstanceId() {
                String dbName = System.getenv("DB_NAME");
                if (dbName == null) {
                    return super.getVirtualHostName();
                } else {
                    return dbName + ".test.ethereumj.org";
                }
            }
        };
        final RemoteDataSource server = new RemoteDataSource(levelDbDataSource);
        logger.info("Starting server on port: " + instConf.getNonSecurePort());
        new Thread(new Runnable() {
            @Override
            public void run() {
                server.startServer(instConf.getNonSecurePort());
            }
        }).start();
        server.waitForActive();

        logger.info("Registering and activating service in Eureka. VIP: " + instConf.getVirtualHostName());
        DiscoveryManager.getInstance().initComponent(instConf, new DefaultEurekaClientConfig() {
            @Override
            public List<String> getEurekaServerServiceUrls(String myZone) {
                String serviceIp = System.getenv("EUREKA_IP");
                if (serviceIp == null) {
                    return super.getEurekaServerServiceUrls(myZone);
                } else {
                    logger.info("Eureka service IP overridden by env property: " + serviceIp);
                    return Collections.singletonList("http://" + serviceIp + ":8080/eureka/v2/");
                }
            }
        });
        ApplicationInfoManager.getInstance().setInstanceStatus(InstanceInfo.InstanceStatus.UP);

    }
}
