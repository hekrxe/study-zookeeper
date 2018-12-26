package com.hkx.sz.cnxn.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Created by tanhuayou on 2018/12/25
 */
public class NettyServerCnxn {
    private final Channel channel;

    private ByteBuffer bbLen = ByteBuffer.allocate(4);
    private int bytesToRead = 0;
    private ByteBuffer buffer;

    public NettyServerCnxn(Channel channel) {
        this.channel = channel;
    }


    public void close() {
        if (channel.isOpen()) {
            channel.close();
        }
    }


    public void sendBuffer(ByteBuffer buffer) {
        if (null != buffer) {
            channel.write(ChannelBuffers.wrappedBuffer(buffer));
        }
    }


    public void receiveMessage(ChannelBuffer message) {
        while (message.readable()) {
            if (null == buffer) {
                message.readBytes(bbLen);
                if (0 == bbLen.remaining()) {
                    bbLen.flip();
                    bytesToRead = bbLen.getInt();
                    bbLen.clear();
                    buffer = ByteBuffer.allocate(bytesToRead);
                }
            } else {
                message.readBytes(buffer);
                if (0 == buffer.remaining()) {
                    buffer.flip();
                    processPacket();
                    buffer = null;
                    bytesToRead = 0;
                }
            }
        }
    }

    private void processPacket() {
        byte[] bytes = new byte[bytesToRead];
        buffer.get(bytes);

        System.out.println(new String(bytes, StandardCharsets.UTF_8));

        ByteBuffer outing = ByteBuffer.allocate(4 + bytesToRead);
        outing.putInt(bytesToRead);
        outing.put(bytes);
        outing.flip();
        sendBuffer(outing);
    }

    @Override
    public int hashCode() {
        return channel.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof NettyServerCnxn)) {
            return false;
        }
        return channel.equals(((NettyServerCnxn) obj).channel);
    }
}
