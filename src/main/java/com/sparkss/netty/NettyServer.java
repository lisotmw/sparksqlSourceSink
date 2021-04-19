package com.sparkss.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

/**
 * @Author $ zho.li
 * @Date 2021/3/22 0:41
 **/
public class NettyServer {

    private final int port;

    NettyServer(int port){
        this.port = port;
    }
    public void start() throws Exception{
        final NioEventLoopGroup group = new NioEventLoopGroup();
        try{
            final ServerBootstrap boot = new ServerBootstrap();
            boot.group(group)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(new InetSocketAddress(port))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            // add own handler
                        }
                    });
            final ChannelFuture future = boot.bind().sync();
            future.channel().closeFuture().sync();
        }finally {
            group.shutdownGracefully().sync();
        }
    }
}
