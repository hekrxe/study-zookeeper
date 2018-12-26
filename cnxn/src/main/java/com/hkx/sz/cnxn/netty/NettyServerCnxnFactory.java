package com.hkx.sz.cnxn.netty;

import com.hkx.sz.cnxn.ServerCnxnFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;

/**
 * Created by tanhuayou on 2018/12/25
 */
public class NettyServerCnxnFactory implements ServerCnxnFactory {
    private ServerBootstrap bootstrap;
    private ChannelGroup allChannels = new DefaultChannelGroup("zkServerCnxns");
    private InetSocketAddress localAddress;
    private Channel parentChannel;


    private final Set<NettyServerCnxn> cnxns = new CopyOnWriteArraySet<>();
    private volatile boolean killed = false;

    public NettyServerCnxnFactory() {
        bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newFixedThreadPool(2), Executors.newCachedThreadPool()));
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("child.tcpNoDelay", true);
        /* set socket linger to off, so that socket close does not block */
        bootstrap.setOption("child.soLinger", -1);
        bootstrap.getPipeline().addLast("NettyServerCnxnFactory", new CnxnChannelHandler());
    }


    @ChannelHandler.Sharable
    private class CnxnChannelHandler extends SimpleChannelHandler {
        @Override
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
            System.out.println("Channel Closed " + e);
            allChannels.remove(ctx.getChannel());
        }

        @Override
        public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
            System.out.println("Channel connected " + e);
            allChannels.add(ctx.getChannel());
            NettyServerCnxn cnxn = new NettyServerCnxn(ctx.getChannel());
            ctx.setAttachment(cnxn);
            cnxns.add(cnxn);
        }

        @Override
        public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
            System.out.println("Channel disconnected " + e);
            NettyServerCnxn cnxn = (NettyServerCnxn) ctx.getAttachment();
            if (null != cnxn) {
                cnxns.remove(cnxn);
                cnxn.close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            System.out.println("Exception " + e.getCause());
            NettyServerCnxn cnxn = (NettyServerCnxn) ctx.getAttachment();
            if (null != cnxn) {
                cnxns.remove(cnxn);
                cnxn.close();
            }
        }

        @Override
        public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) {
            System.out.println("write complete " + e);
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            System.out.println("message received " + e);
            try {
                NettyServerCnxn cnxn = (NettyServerCnxn) ctx.getAttachment();
                synchronized (cnxn) {
                    processMessage(e, cnxn);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                throw ex;
            }
        }

        private void processMessage(MessageEvent msg, NettyServerCnxn cnxn) {
            cnxn.receiveMessage((ChannelBuffer) msg.getMessage());
        }
    }

    @Override
    public void configure(InetSocketAddress addr) throws IOException {
        this.localAddress = addr;
    }

    @Override
    public void join() throws InterruptedException {
        synchronized (this) {
            while (!killed) {
                wait();
            }
        }
    }

    @Override
    public void shutdown() {
        parentChannel.close().awaitUninterruptibly();
        allChannels.close().awaitUninterruptibly();
        bootstrap.releaseExternalResources();
        synchronized (this) {
            killed = true;
            notifyAll();
        }
    }

    @Override
    public void start() {
        parentChannel = bootstrap.bind(localAddress);
    }
}
