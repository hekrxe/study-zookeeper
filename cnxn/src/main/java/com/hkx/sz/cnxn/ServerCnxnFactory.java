package com.hkx.sz.cnxn;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by tanhuayou on 2018/12/24
 */
public interface ServerCnxnFactory {

    void configure(InetSocketAddress addr) throws IOException;

    void join() throws InterruptedException;

    void shutdown();

    void start();
}
