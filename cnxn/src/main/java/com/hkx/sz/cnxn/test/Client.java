package com.hkx.sz.cnxn.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Just test!
 * Created by tanhuayou on 2018/12/24
 */
public class Client {

    private static void doWrite(SocketChannel sc) {

        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.scheduleAtFixedRate(() -> {
            byte[] req = ("ssssssssssssssssdfsdfdsfsdfdsfdsfsdhellossssssssssssssssdfsdfdsfdsfsdhellossssssssssssssssdfsdfdsfdsfsdhellossssssssssssssssdfsdfdsfdsfsdhellossssssssssssssssdfsdfdsfdsfsdhellossssssssssssssssdfsdfdsfdsfsdhello: " + System.currentTimeMillis()).getBytes();
            ByteBuffer writeBuffer = ByteBuffer.allocate(req.length + 4);
            writeBuffer.putInt(req.length);
            writeBuffer.put(req);
            writeBuffer.flip();
            try {
                sc.write(writeBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, 500, 800, TimeUnit.MILLISECONDS);
    }


    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 2181;
        SocketChannel socketChannel;

        Selector selector = Selector.open();
        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);

        if (socketChannel.connect(new InetSocketAddress(host, port))) {
            socketChannel.register(selector, SelectionKey.OP_READ);
            doWrite(socketChannel);
        } else {
            socketChannel.register(selector, SelectionKey.OP_CONNECT);
        }

        while (true) {
            try {
                selector.select(1000);
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> it = selectionKeys.iterator();
                SelectionKey key;
                while (it.hasNext()) {
                    key = it.next();
                    it.remove();
                    try {
                        if (key.isValid()) {
                            SocketChannel sc = (SocketChannel) key.channel();
                            if (key.isConnectable()) {
                                if (sc.finishConnect()) {
                                    sc.register(selector, SelectionKey.OP_READ);
                                    doWrite(sc);
                                } else {
                                    System.exit(1);
                                }
                            }
                            if (key.isReadable()) {
                                read(sc, key);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        if (key != null) {
                            key.cancel();
                            if (key.channel() != null) {
                                key.channel().close();
                            }
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }


    private static ByteBuffer len = ByteBuffer.allocate(4);
    private static ByteBuffer packet = null;

    private static void read(SocketChannel sc, SelectionKey key) throws IOException {
        if (null == packet) {
            int rc = sc.read(len);
            if (rc < 0) {
                key.cancel();
                sc.close();
                System.out.println("Closed");
                return;
            }
        }

        if (0 == len.remaining()) {
            len.flip();
            int count = len.getInt();
            if (null == packet) {
                packet = ByteBuffer.allocate(count);
            }
            sc.read(packet);
            if (0 == packet.remaining()) {
                packet.flip();
                byte[] bytes = new byte[count];
                packet.get(bytes);
                String body = new String(bytes);
                System.out.println(body);
                packet = null;
                len.clear();
            }
        }
    }

}
