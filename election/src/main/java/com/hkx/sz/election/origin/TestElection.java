package com.hkx.sz.election.origin;

import com.hkx.sz.common.Vote;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tanhuayou on 2018/12/26
 */
public class TestElection {
    public static void main(String[] args) throws InterruptedException {
        Map<Long, QuorumServer> view = new HashMap<>();

        QuorumServer server1 = new QuorumServer(1, new InetSocketAddress(3181));
        QuorumServer server2 = new QuorumServer(2, new InetSocketAddress(3182));
        QuorumServer server3 = new QuorumServer(3, new InetSocketAddress(3183));
//        QuorumServer server4 = new QuorumServer(4, new InetSocketAddress(3184));
//        QuorumServer server5 = new QuorumServer(5, new InetSocketAddress(3185));


        view.put(server1.id, server1);
        view.put(server2.id, server2);
        view.put(server3.id, server3);
//        view.put(server4.id, server4);
//        view.put(server5.id, server5);

        QuorumCnxManager qcm1 = new QuorumCnxManager(server1.id, view);
        QuorumCnxManager qcm2 = new QuorumCnxManager(server2.id, view);
        QuorumCnxManager qcm3 = new QuorumCnxManager(server3.id, view);
//        QuorumCnxManager qcm4 = new QuorumCnxManager(server4.id, view);
//        QuorumCnxManager qcm5 = new QuorumCnxManager(server5.id, view);


        qcm1.getListener().start();
        Thread.sleep(1000);
        qcm2.getListener().start();
        Thread.sleep(1000);
        qcm3.getListener().start();
//        Thread.sleep(1000);
//        qcm4.getListener().start();
//        Thread.sleep(1000);
//        qcm5.getListener().start();

        FastLeaderElection el1 = new FastLeaderElection(qcm1, new QuorumPeer(server1.id, LearnerType.PARTICIPANT, view));
        FastLeaderElection el2 = new FastLeaderElection(qcm2, new QuorumPeer(server2.id, LearnerType.PARTICIPANT, view));
        FastLeaderElection el3 = new FastLeaderElection(qcm3, new QuorumPeer(server3.id, LearnerType.PARTICIPANT, view));


        new Thread(() -> {
            Vote vote = el1.lookForLeader();
            System.out.println(vote);
        }).start();

        new Thread(() -> {
            Vote vote = el2.lookForLeader();
            System.out.println(vote);
        }).start();

        new Thread(() -> {
            Vote vote = el3.lookForLeader();
            System.out.println(vote);
        }).start();

        Thread.sleep(Integer.MAX_VALUE);

    }
}
