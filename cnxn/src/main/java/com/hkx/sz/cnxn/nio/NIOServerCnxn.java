package com.hkx.sz.cnxn.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by tanhuayou on 2018/12/24
 */
public class NIOServerCnxn {
    /**
     * 仅仅为了代替 factory
     */
    private static final Object LOCK = new Object();


    private final SelectionKey selectionKey;
    private final SocketChannel socket;


    private ByteBuffer lenBuffer = ByteBuffer.allocate(4);

    private ByteBuffer incomingBuffer = null;
    private LinkedBlockingQueue<ByteBuffer> outgoingBuffers = new LinkedBlockingQueue<>();
    private final ByteBuffer directBuffer = ByteBuffer.allocateDirect(64 * 1024);


    public NIOServerCnxn(SelectionKey selectionKey, SocketChannel socketChannel) {
        this.selectionKey = selectionKey;
        this.socket = socketChannel;
    }

    public void close() {
        closeSock();
        selectionKey.cancel();
    }

    public void doIO(SelectionKey key) {
        if (!socket.isOpen()) {
            System.out.println("Socket already closed");
            return;
        }

        try {
            if (key.isReadable()) {
                read();
            }
            if (key.isWritable()) {
                write();
            }
        } catch (Exception e) {
            e.printStackTrace();
            close();
        }
    }

    private void write() throws IOException {
        if (!outgoingBuffers.isEmpty()) {
            directBuffer.clear();
            // 合并一起发送
            for (ByteBuffer buf : outgoingBuffers) {
                if (directBuffer.remaining() < buf.remaining()) {
                    // 一次都发不完 那么拆开发
                    buf = (ByteBuffer) buf.slice().limit(directBuffer.remaining());
                }

                int position = buf.position();
                directBuffer.put(buf);
                buf.position(position);
                if (directBuffer.remaining() == 0) {
                    // 填满了
                    break;
                }
            }
            directBuffer.flip();
            int sent = socket.write(directBuffer);

            while (!outgoingBuffers.isEmpty()) {
                ByteBuffer peek = outgoingBuffers.peek();
                // 还剩下多少
                int left = peek.remaining() - sent;
                if (left > 0) {
                    peek.position(peek.position() + sent);
                    // 等下把再发
                    break;
                }
                sent -= peek.remaining();
                outgoingBuffers.remove();
            }
        }
        synchronized (LOCK) {
            if (outgoingBuffers.isEmpty()) {
                // 关闭无效的链接
                if (null == incomingBuffer && ((selectionKey.interestOps() & SelectionKey.OP_READ) == 0)) {
                    throw new RuntimeException("responded to info probe");
                }
                // 不再关心写事件了
                selectionKey.interestOps(selectionKey.interestOps() & (~SelectionKey.OP_WRITE));
            } else {
                selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_WRITE);
            }
        }
    }

    public void sendBuffer(ByteBuffer bb) {
        try {
            internalSendBuffer(bb);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void internalSendBuffer(ByteBuffer bb) {
        if (null == bb || bb.remaining() == 0) {
            return;
        }
        // We check if write interest here because if it is NOT set,
        // nothing is queued, so we can try to send the buffer right
        // away without waking up the selector
        if (selectionKey.isValid() &&
                ((selectionKey.interestOps() & SelectionKey.OP_WRITE) == 0)) {
            try {
                socket.write(bb);
            } catch (IOException e) {
                // we are just doing best effort right now
            }
        }
        // if there is nothing left to send, we are done
        if (bb.remaining() == 0) {
            return;
        }

        synchronized (LOCK) {
            selectionKey.selector().wakeup();
            outgoingBuffers.add(bb);
            if (selectionKey.isValid()) {
                selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_WRITE);
            }
        }
    }


    private void read() throws IOException {
        int rc = socket.read(lenBuffer);
        if (rc < 0) {
            throw new RuntimeException("Unable to read additional data from client, likely client has closed socket");
        }
        if (lenBuffer.remaining() == 0) {
            lenBuffer.flip();
            int len = lenBuffer.getInt();
            lenBuffer.clear();
            if (null == incomingBuffer) {
                // 防止分包
                incomingBuffer = ByteBuffer.allocate(len);
            }
            rc = socket.read(incomingBuffer);
            if (rc < 0) {
                throw new RuntimeException("Unable to read additional data from client, likely client has closed socket");
            }
            if (incomingBuffer.remaining() == 0) {
                incomingBuffer.flip();
                processPacket();
                incomingBuffer = null;
            }
        }
    }


    private void processPacket() {
        zkServerProcess();
    }

    private void zkServerProcess() {
        int count = incomingBuffer.remaining();
        byte[] bytes = new byte[count];
        incomingBuffer.get(bytes);

        // for zkServer
        {
            // in this case just echo
            System.out.println(new String(bytes, StandardCharsets.UTF_8));
            incomingBuffer.flip();
            ByteBuffer outing = ByteBuffer.allocate(count + 4);
            outing.putInt(count);
            outing.put(incomingBuffer);
            outing.flip();
            sendBuffer(outing);
        }
    }

    private void closeSock() {
        if (!socket.isOpen()) {
            return;
        }

        try {
            /*
             * The following sequence of code is stupid! You would think that
             * only sock.close() is needed, but alas, it doesn't work that way.
             * If you just do sock.close() there are cases where the socket
             * doesn't actually close...
             */
            socket.socket().shutdownOutput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            e.printStackTrace();
        }
        try {
            socket.socket().shutdownInput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            e.printStackTrace();
        }
        try {
            socket.socket().close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            socket.close();
            // XXX The next line doesn't seem to be needed, but some posts
            // to forums suggest that it is needed. Keep in mind if errors in
            // this section arise.
            // factory.selector.wakeup();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj;
    }
}
