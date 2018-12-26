package com.hkx.sz.cnxn.test;

import com.hkx.sz.cnxn.ServerCnxnFactory;
import com.hkx.sz.cnxn.netty.NettyServerCnxnFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by tanhuayou on 2018/12/24
 */
public class Server {
    public static void main(String[] args) throws Exception {
        ServerCnxnFactory factory = factory();
        factory.configure(new InetSocketAddress(2181));
        factory.start();
        factory.join();

    }

    private static ServerCnxnFactory factory() throws IOException {
//        return new NIOServerCnxnFactory();
        return new NettyServerCnxnFactory();
    }
}
