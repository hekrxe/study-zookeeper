package com.hkx.sz.election.origin;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tanhuayou on 2018/12/25
 */
public class TestInterflow {
    public static void main(String[] args) throws InterruptedException {


        Map<Long, QuorumServer> view = new HashMap<>();

        QuorumServer server1 = new QuorumServer(1, new InetSocketAddress(3181));
        QuorumServer server2 = new QuorumServer(2, new InetSocketAddress(3182));
        QuorumServer server3 = new QuorumServer(3, new InetSocketAddress(3183));
        QuorumServer server4 = new QuorumServer(4, new InetSocketAddress(3184));
        QuorumServer server5 = new QuorumServer(5, new InetSocketAddress(3185));


        view.put(server1.id, server1);
        view.put(server2.id, server2);
        view.put(server3.id, server3);
        view.put(server4.id, server4);
        view.put(server5.id, server5);

        QuorumCnxManager qcm1 = new QuorumCnxManager(server1.id, view);
        QuorumCnxManager qcm2 = new QuorumCnxManager(server2.id, view);
        QuorumCnxManager qcm3 = new QuorumCnxManager(server3.id, view);
        QuorumCnxManager qcm4 = new QuorumCnxManager(server4.id, view);
        QuorumCnxManager qcm5 = new QuorumCnxManager(server5.id, view);


        qcm1.getListener().start();
        Thread.sleep(1000);
        qcm2.getListener().start();
        Thread.sleep(1000);
        qcm3.getListener().start();
        Thread.sleep(1000);
        qcm4.getListener().start();
        Thread.sleep(1000);
        qcm5.getListener().start();


        Thread.sleep(1000);
        qcm1.connectAll();
        Thread.sleep(1000);
        qcm2.connectAll();
        Thread.sleep(1000);
        qcm3.connectAll();
        Thread.sleep(1000);
        qcm4.connectAll();
        Thread.sleep(1000);
        qcm5.connectAll();


        Thread.sleep(Integer.MAX_VALUE);
    }
}
