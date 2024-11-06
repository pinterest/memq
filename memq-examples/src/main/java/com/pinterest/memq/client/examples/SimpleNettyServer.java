package com.pinterest.memq.client.examples;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;

import java.util.logging.Level;
import java.util.logging.Logger;

// Server side:
// mvn clean install -DskipTests -Dgpg.skip
// mvn compile exec:java -Dexec.mainClass="com.pinterest.memq.client.examples.SimpleNettyServer" -e -Dgpg.skip
// Client side:
// iperf3 -c {IP} -p 5678 -t 10 -V
public class SimpleNettyServer {

    private static final Logger logger = Logger.getLogger(SimpleNettyServer.class.getName());
    private final int port;
    private long transferredBytes = 0;

    public SimpleNettyServer(int port) {
        this.port = port;
    }

    public void start() throws InterruptedException {
        logger.log(Level.INFO, "Starting Netty server");
        EventLoopGroup childGroup = new NioEventLoopGroup();
        EventLoopGroup parentGroup = new NioEventLoopGroup(1);
        logger.log(Level.INFO, "Parent and child event loop groups created");
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(parentGroup, childGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        // TODO: Test global traffic shaping handler here
                        ch.pipeline().addLast(
                            new ByteArrayDecoder(),
                            new ByteArrayEncoder(),
                            new Iperf3ServerHandler());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
            ChannelFuture f = b.bind(port).sync();
            System.out.println("Server started, listening on " + port);
            f.channel().closeFuture().sync();
        } finally {
            childGroup.shutdownGracefully();
            parentGroup.shutdownGracefully();
        }
    }

    private class Iperf3ServerHandler extends io.netty.channel.ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            byte[] data = (byte[]) msg;
            transferredBytes += data.length;
            logger.log(Level.INFO, "Received " + data.length + " bytes, total received: " + transferredBytes);
            logger.log(Level.INFO, "Channel read status: " + ctx.channel().config().isAutoRead());
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        int port = 5678;
        new SimpleNettyServer(port).start();
    }
}
