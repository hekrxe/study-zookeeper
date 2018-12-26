package com.hkx.sz.election;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by tanhuayou on 2018/12/25
 */
public class QuorumCnxManager {
    private final long myId;
    private static final long OBSERVER_ID = Long.MAX_VALUE;

    private volatile boolean shutdown = false;
    private final AtomicLong observerCounter = new AtomicLong(-1);
    private final ConcurrentHashMap<Long, SendWorker> senderWorkerMap = new ConcurrentHashMap<>();
    private static final int PACKETMAXSIZE = 1024 * 512;
    private static final int RECV_CAPACITY = 100;
    private final ArrayBlockingQueue<Message> recvQueue = new ArrayBlockingQueue<>(RECV_CAPACITY);
    private final Object recvQLock = new Object();
    private static final int SEND_CAPACITY = 1;
    private final ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>> queueSendMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ByteBuffer> lastMessageSent = new ConcurrentHashMap<>();
    private final Map<Long, QuorumServer> view;
    private final Listener listener = new Listener();


    public QuorumCnxManager(long myId, Map<Long, QuorumServer> view) {
        this.myId = myId;
        this.view = view;
    }

    private void setSockOpts(Socket sock) throws SocketException {
        sock.setTcpNoDelay(true);
        sock.setSoTimeout(3000);
    }

    private void handleConnection(Socket sock) throws IOException {
        long sid;
        DataInputStream din = new DataInputStream(new BufferedInputStream(sock.getInputStream()));
        try {
            // Read server id
            sid = din.readLong();
            if (sid < 0) {
                closeSocket(sock);
                return;
            }
            if (sid == OBSERVER_ID) {
                sid = observerCounter.getAndDecrement();
            }
        } catch (IOException e) {
            closeSocket(sock);
            return;
        }

        System.out.println(QuorumCnxManager.this.myId + " accept " + sid);

        if (sid < this.myId) {
            /*
             * This replica might still believe that the connection to sid is
             * up, so we have to shut down the workers before trying to open a
             * new connection.
             */
            SendWorker sw = senderWorkerMap.get(sid);
            if (sw != null) {
                sw.finish();
            }

            System.out.println(myId + " Close " + sid);
            /*
             * Now we start a new connection
             */
            closeSocket(sock);
            connectOne(sid);
            return;
        }

        startWorker(sock, sid, din);
    }

    synchronized private void connectOne(long sid) {
        if (null == senderWorkerMap.get(sid)) {
            InetSocketAddress electionAddr;

            if (view.containsKey(sid)) {
                electionAddr = view.get(sid).electionAddr;
            } else {
                System.out.println("Invalid server id: " + sid);
                return;
            }

            try {
                Socket sock = new Socket();
                setSockOpts(sock);
                sock.connect(electionAddr, 3000);
                System.out.println(myId + " Connected to server " + sid);

                // Sends connection request asynchronously if the quorum
                // sasl authentication is enabled. This is required because
                // sasl server authentication process may take few seconds to
                // finish, this may delay next peer connection requests.
                initiateConnection(sock, sid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println(myId + " There is a connection already for server " + sid);
        }
    }

    public void initiateConnection(final Socket sock, final Long sid) {
        try {
            startConnection(sock, sid);
        } catch (IOException e) {
            e.printStackTrace();
            closeSocket(sock);
        }
    }

    public void toSend(Long sid, ByteBuffer b) {
        /*
         * If sending message to myself, then simply enqueue it (loopback).
         */
        if (this.myId == sid) {
            b.position(0);
            addToRecvQueue(new Message(b.duplicate(), sid));
        } else {
            if (!queueSendMap.containsKey(sid)) {
                ArrayBlockingQueue<ByteBuffer> bq = new ArrayBlockingQueue<ByteBuffer>(
                        SEND_CAPACITY);
                queueSendMap.put(sid, bq);
                addToSendQueue(bq, b);

            } else {
                ArrayBlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                if (bq != null) {
                    addToSendQueue(bq, b);
                } else {
                    System.out.println("No queue for server " + sid);
                }
            }
            connectOne(sid);
        }
    }

    public void connectAll() {
        ArrayList<Long> sids = new ArrayList<>(view.keySet());
        Collections.shuffle(sids);
        sids.forEach(sid -> {
            if (myId != sid) {
                connectOne(sid);
            }
        });
    }

    private void addToSendQueue(ArrayBlockingQueue<ByteBuffer> queue, ByteBuffer buffer) {
        addToQueue(queue, buffer);
    }

    private void addToRecvQueue(Message msg) {
        synchronized (recvQLock) {
            addToQueue(recvQueue, msg);
        }
    }

    private <T> void addToQueue(ArrayBlockingQueue<T> queue, T buffer) {
        if (queue.remainingCapacity() == 0) {
            try {
                queue.remove();
            } catch (NoSuchElementException ne) {
                // element could be removed by poll()
                ne.printStackTrace();
            }
        }
        try {
            queue.add(buffer);
        } catch (IllegalStateException ie) {
            // This should never happen
            ie.printStackTrace();
        }
    }

    private boolean startConnection(Socket sock, Long sid) throws IOException {
        DataOutputStream dout;
        DataInputStream din;
        try {
            // Sending id and challenge
            dout = new DataOutputStream(sock.getOutputStream());
            dout.writeLong(this.myId);
            dout.flush();

            din = new DataInputStream(new BufferedInputStream(sock.getInputStream()));
        } catch (IOException e) {
            closeSocket(sock);
            throw e;
        }

        // If lost the challenge, then drop the new connection
        if (sid > this.myId) {
            closeSocket(sock);
            // Otherwise proceed with the connection
        } else {
            startWorker(sock, sid, din);
            return true;

        }
        return false;
    }

    private void startWorker(Socket sock, long sid, DataInputStream din) {
        SendWorker sw = new SendWorker(sock, sid);
        RecvWorker rw = new RecvWorker(sock, din, sid, sw);
        sw.setRecv(rw);

        SendWorker vsw = senderWorkerMap.get(sid);

        if (vsw != null) {
            vsw.finish();
        }

        senderWorkerMap.put(sid, sw);
        if (!queueSendMap.containsKey(sid)) {
            queueSendMap.put(sid, new ArrayBlockingQueue<>(SEND_CAPACITY));
        }

        sw.start();
        rw.start();
    }

    private void closeSocket(Socket sock) {
        try {
            sock.close();
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }

    class SendWorker extends Thread {
        Long sid;
        Socket sock;
        RecvWorker recvWorker;
        volatile boolean running = true;
        DataOutputStream dout;

        SendWorker(Socket sock, Long sid) {
            super("SendWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            recvWorker = null;
            try {
                dout = new DataOutputStream(sock.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
                closeSocket(sock);
                running = false;
            }
        }

        synchronized void setRecv(RecvWorker recvWorker) {
            this.recvWorker = recvWorker;
        }

        synchronized RecvWorker getRecvWorker() {
            return recvWorker;
        }

        synchronized boolean finish() {
            if (!running) {
                return running;
            }
            running = false;
            closeSocket(sock);
            this.interrupt();
            if (recvWorker != null) {
                recvWorker.finish();
            }
            senderWorkerMap.remove(sid, this);
            return running;
        }

        synchronized void send(ByteBuffer b) throws IOException {
            byte[] msgBytes = new byte[b.capacity()];
            try {
                b.position(0);
                b.get(msgBytes);
            } catch (BufferUnderflowException be) {
                be.printStackTrace();
                return;
            }
            dout.writeInt(b.capacity());
            dout.write(b.array());
            dout.flush();
        }

        @Override
        public void run() {
            try {
                ArrayBlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                if (bq == null || bq.isEmpty()) {
                    ByteBuffer b = lastMessageSent.get(sid);
                    if (b != null) {
                        System.out.println("Attempting to send lastMessage to sid=" + sid);
                        send(b);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                this.finish();
            }

            try {
                while (running && !shutdown && sock != null) {
                    ByteBuffer b;
                    try {
                        ArrayBlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                        if (bq != null) {
                            b = bq.poll(1000, TimeUnit.MILLISECONDS);
                        } else {
                            System.err.println("No queue of incoming messages for " + "server " + sid);
                            break;
                        }

                        if (b != null) {
                            lastMessageSent.put(sid, b);
                            send(b);
                        }
                    } catch (InterruptedException ignored) {
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            this.finish();
        }
    }

    class RecvWorker extends Thread {
        Long sid;
        Socket sock;
        volatile boolean running = true;
        final DataInputStream din;
        final SendWorker sw;

        RecvWorker(Socket sock, DataInputStream din, Long sid, SendWorker sw) {
            super("RecvWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            this.sw = sw;
            this.din = din;
            try {
                // OK to wait until socket disconnects while reading.
                sock.setSoTimeout(0);
            } catch (IOException e) {
                e.printStackTrace();
                closeSocket(sock);
                running = false;
            }
        }

        /**
         * Shuts down this worker
         *
         * @return boolean  Value of variable running
         */
        synchronized boolean finish() {
            if (!running) {
                /*
                 * Avoids running finish() twice.
                 */
                return running;
            }
            running = false;

            this.interrupt();
            return running;
        }

        @Override
        public void run() {
            try {
                while (running && !shutdown && sock != null) {
                    /**
                     * Reads the first int to determine the length of the
                     * message
                     */
                    int length = din.readInt();
                    if (length <= 0 || length > PACKETMAXSIZE) {
                        throw new IOException(
                                "Received packet with invalid packet: "
                                        + length);
                    }
                    /**
                     * Allocates a new ByteBuffer to receive the message
                     */
                    byte[] msgArray = new byte[length];
                    din.readFully(msgArray, 0, length);
                    ByteBuffer message = ByteBuffer.wrap(msgArray);
                    addToRecvQueue(new Message(message.duplicate(), sid));
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                sw.finish();
                if (sock != null) {
                    closeSocket(sock);
                }
            }
        }
    }

    static public class Message {

        Message(ByteBuffer buffer, long sid) {
            this.buffer = buffer;
            this.sid = sid;
        }

        ByteBuffer buffer;
        long sid;
    }

    public class Listener extends Thread {
        volatile ServerSocket serverSocket = null;

        public Listener() {
            super("QuorumCnxManager.Listener-" + myId);
        }

        @Override
        public void run() {
            int numRetries = 0;

            while ((!shutdown) && (numRetries < 3)) {
                try {
                    serverSocket = new ServerSocket();
                    serverSocket.setReuseAddress(true);
                    serverSocket.bind(view.get(QuorumCnxManager.this.myId).electionAddr);
                    System.out.println(QuorumCnxManager.this.myId + " listen on " + view.get(QuorumCnxManager.this.myId).electionAddr.getPort());
                    while (!shutdown) {
                        Socket client = serverSocket.accept();
                        setSockOpts(client);
                        handleConnection(client);
                        numRetries = 0;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    numRetries++;
                    try {
                        serverSocket.close();
                        Thread.sleep(1000);
                    } catch (IOException | InterruptedException ie) {
                        ie.printStackTrace();
                    }
                }
            }
        }
    }

    //==============

    public Listener getListener() {
        return listener;
    }
}
