package com.hkx.sz.cnxn.nio;

import com.hkx.sz.cnxn.ServerCnxnFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by tanhuayou on 2018/12/24
 */
public class NIOServerCnxnFactory implements ServerCnxnFactory, Runnable {

    static {
        try {
            /*
              this is to avoid the jvm bug:
              NullPointerException in Selector.open()
              http://bugs.sun.com/view_bug.do?bug_id=6427854
             */
            Selector.open().close();
        } catch (IOException ie) {
            ie.fillInStackTrace();
        }
    }

    private ServerSocketChannel serverSocketChannel;
    private final Selector selector = Selector.open();
    private Thread thread;
    private final Set<NIOServerCnxn> cnxns = new CopyOnWriteArraySet<>();

    public NIOServerCnxnFactory() throws IOException {
    }

    @Override
    public void configure(InetSocketAddress addr) throws IOException {
        thread = new Thread(this, "NIOServerCnxnFactory");
        thread.setDaemon(true);
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.socket().setReuseAddress(true);
        serverSocketChannel.socket().bind(addr);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    public void join() throws InterruptedException {
        thread.join();
    }

    @Override
    public void shutdown() {
        try {
            serverSocketChannel.close();
            cnxns.forEach(cnxn -> {
                try {
                    cnxn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            thread.interrupt();
            thread.join();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            selector.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void start() {
        if (thread.getState() == Thread.State.NEW) {
            thread.start();
        }
    }

    @Override
    public void run() {
        while (!serverSocketChannel.socket().isClosed()) {
            try {
                selector.select(1000);
                Set<SelectionKey> selected;
                synchronized (this) {
                    selected = selector.selectedKeys();
                }
                // 随机排列一下
                List<SelectionKey> selectedList = new ArrayList<>(selected);
                Collections.shuffle(selectedList);

                for (SelectionKey key : selectedList) {
                    if ((key.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                        SocketChannel socketChannel = ((ServerSocketChannel) key.channel()).accept();
                        // 做一个最大链接的判断 超过了则断开此连接
                        InetAddress remoteAddress = socketChannel.socket().getInetAddress();
                        System.out.println("Accept: " + remoteAddress);
                        socketChannel.configureBlocking(false);
                        SelectionKey selectionKey = socketChannel.register(selector, SelectionKey.OP_READ);
                        NIOServerCnxn cnxn = new NIOServerCnxn(selectionKey, socketChannel);
                        selectionKey.attach(cnxn);
                        cnxns.add(cnxn);
                    } else if ((key.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) {
                        NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                        cnxn.doIO(key);
                    } else {
                        System.out.println("unsupported op," + key);
                    }
                }
                selected.clear();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
