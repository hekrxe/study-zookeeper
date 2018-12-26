package com.hkx.sz.election.origin;

import com.hkx.sz.common.ServerState;
import com.hkx.sz.common.Vote;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by tanhuayou on 2018/12/26
 */
public class FastLeaderElection implements Election {
    private final QuorumCnxManager manager;
    private final QuorumPeer self;

    /**
     * read from file
     */
    private volatile long logicalclock = 0;
    private long proposedLeader;
    private long proposedZxid;
    private long proposedEpoch;
    private final LinkedBlockingQueue<ToSend> sendqueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Notification> recvqueue = new LinkedBlockingQueue<>();


    private volatile boolean shutdown = false;

    public FastLeaderElection(QuorumCnxManager manager, QuorumPeer self) {
        this.manager = manager;
        this.self = self;
        new Messenger(manager);
    }


    @Override
    public Vote lookForLeader() {
        HashMap<Long, Vote> recvset = new HashMap<>();
        HashMap<Long, Vote> outofelection = new HashMap<>();

        int pollTime = 1000;
        int maxPollTime = 5000;

        try {
            synchronized (this) {
                // 逻辑时钟(年代?)增加
                logicalclock++;
                // 认为自己是领导
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }
            // 通知其他所有人 我当前推荐谁来做领导了
            sendNotifications();

            while (self.getState() == ServerState.LOOKING && !shutdown) {
                Notification notification = recvqueue.poll(pollTime, TimeUnit.MILLISECONDS);
                if (null == notification) {
                    if (manager.haveDelivered()) {
                        sendNotifications();
                    } else {
                        manager.connectAll();
                    }
                    pollTime += pollTime;
                    pollTime = pollTime > maxPollTime ? maxPollTime : pollTime;
                } else if (!self.getView().containsKey(notification.sid)) {
                    System.err.println(self.getMyId() + " Ignoring notification from non-cluster member " + notification.sid);
                } else {
                    switch (notification.currentState) {
                        case ServerState.LOOKING:
                            if (notification.electionEpoch > logicalclock) {
                                logicalclock = notification.electionEpoch;
                                recvset.clear();
                                if (totalOrderPredicate(notification.leader, notification.zxid, notification.electionEpoch, getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                    updateProposal(notification.leader, notification.zxid, notification.electionEpoch);
                                } else {
                                    updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
                                }
                                sendNotifications();
                            } else if (notification.electionEpoch < logicalclock) {
                                System.out.println("Ignore ");
                                break;
                            } else if (totalOrderPredicate(notification.leader, notification.zxid, notification.electionEpoch,
                                    proposedLeader, proposedZxid, proposedEpoch)) {
                                updateProposal(notification.leader, notification.zxid, notification.electionEpoch);
                                sendNotifications();
                            }
                            recvset.put(notification.sid, new Vote(notification.leader, notification.zxid, notification.electionEpoch, notification.peerEpoch, ServerState.LOOKING));
                            if (termPredicate(recvset,
                                    new Vote(proposedLeader, proposedZxid,
                                            logicalclock, proposedEpoch, ServerState.LOOKING))) {
                                // Verify if there is any change in the proposed leader
                                while ((notification = recvqueue.poll(maxPollTime,
                                        TimeUnit.MILLISECONDS)) != null) {
                                    if (totalOrderPredicate(notification.leader, notification.zxid, notification.peerEpoch,
                                            proposedLeader, proposedZxid, proposedEpoch)) {
                                        recvqueue.put(notification);
                                        break;
                                    }
                                }
                                /*
                                 * This predicate is true once we don't read any new
                                 * relevant message from the reception queue
                                 */
                                if (notification == null) {
                                    self.setState((proposedLeader == self.getMyId()) ?
                                            ServerState.LEADING : learningState());

                                    Vote endVote = new Vote(proposedLeader,
                                            proposedZxid,
                                            logicalclock,
                                            proposedEpoch, self.getState());
                                    recvqueue.clear();
                                    System.out.println(self.getMyId() + " Selected " + proposedLeader);
                                    return endVote;
                                }
                            }
                            break;
                        case ServerState.FOLLOWING:
                        case ServerState.LEADING:
                            Notification n = notification;

                            if (n.electionEpoch == logicalclock) {
                                recvset.put(n.sid, new Vote(n.leader,
                                        n.zxid,
                                        n.electionEpoch,
                                        n.peerEpoch, ServerState.LOOKING));

                                if (ooePredicate(recvset, outofelection, n)) {
                                    recvset.clear();
                                    return bingo(n);
                                }
                            }

                            /*
                             * Before joining an established ensemble, verify
                             * a majority is following the same leader.
                             */
                            outofelection.put(n.sid, new Vote(n.version,
                                    n.leader,
                                    n.zxid,
                                    n.electionEpoch,
                                    n.peerEpoch,
                                    n.currentState));

                            if (ooePredicate(outofelection, outofelection, n)) {
                                synchronized (this) {
                                    logicalclock = n.electionEpoch;
                                    self.setState((n.leader == self.getMyId()) ?
                                            ServerState.LEADING : learningState());
                                }
                                recvset.clear();
                                return bingo(n);
                            }
                            break;
                        case ServerState.OBSERVING:
                            break;
                        default:
                            System.out.println(self.getMyId() + " Not match " + notification.sid);
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        return null;
    }

    private Vote bingo(Notification n) {
        System.out.println(self.getMyId() + " Selected " + n.leader);
        self.setState((n.leader == self.getMyId()) ?
                ServerState.LEADING : learningState());
        return new Vote(n.leader,
                n.zxid,
                n.electionEpoch,
                n.peerEpoch,
                ServerState.LOOKING);
    }

    protected boolean ooePredicate(HashMap<Long, Vote> recv,
                                   HashMap<Long, Vote> ooe,
                                   Notification n) {

        return (termPredicate(recv, new Vote(n.version,
                n.leader,
                n.zxid,
                n.electionEpoch,
                n.peerEpoch,
                n.currentState))
                && checkLeader(ooe, n.leader, n.electionEpoch));

    }

    protected boolean checkLeader(
            HashMap<Long, Vote> votes,
            long leader,
            long electionEpoch) {

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if (leader != self.getMyId()) {
            if (votes.get(leader) == null) {
                predicate = false;
            } else if (votes.get(leader).getState() != ServerState.LEADING) {
                predicate = false;
            }
        } else if (logicalclock != electionEpoch) {
            predicate = false;
        }

        return predicate;
    }

    protected boolean termPredicate(HashMap<Long, Vote> votes, Vote vote) {

        HashSet<Long> set = new HashSet<>();

        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())) {
                set.add(entry.getKey());
            }
        }

        return self.getView().size() / 2 <= set.size();
    }

    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        System.out.println("id: " + newId + ", proposed id: " + curId + ", zxid: 0x" +
                Long.toHexString(newZxid) + ", proposed zxid: 0x" + Long.toHexString(curZxid));

        return ((newEpoch > curEpoch) ||
                ((newEpoch == curEpoch) &&
                        ((newZxid > curZxid) || ((newZxid == curZxid) && (newId > curId)))));
    }

    private void sendNotifications() {
        for (QuorumServer server : self.getView().values()) {
            long sid = server.id;

            ToSend notmsg = new ToSend(proposedLeader,
                    proposedZxid,
                    logicalclock,
                    ServerState.LOOKING,
                    sid,
                    proposedEpoch);

            sendqueue.offer(notmsg);
        }
    }

    private long getInitId() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            return self.getMyId();
        } else {
            return Long.MIN_VALUE;
        }
    }

    private long getInitLastLoggedZxid() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            return self.getLastProcessedZxid();
        } else {
            return Long.MIN_VALUE;
        }
    }

    private long getPeerEpoch() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            return self.getCurrentEpoch();
        } else {
            return Long.MIN_VALUE;
        }
    }

    private int learningState() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            return ServerState.FOLLOWING;
        } else {
            return ServerState.OBSERVING;
        }
    }

    synchronized private void updateProposal(long leader, long zxid, long epoch) {
        System.out.println(self.getMyId() + "-> Updating proposal: " + leader + " (newleader), 0x"
                + Long.toHexString(zxid) + " (newzxid), " + proposedLeader
                + " (oldleader), 0x" + Long.toHexString(proposedZxid) + " (oldzxid)");

        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    static ByteBuffer buildMsg(int state,
                               long leader,
                               long zxid,
                               long electionEpoch,
                               long epoch) {
        byte[] requestBytes = new byte[36];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);
        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);

        return requestBuffer;
    }


    synchronized Vote getVote() {
        return new Vote(proposedLeader, proposedZxid, -1, proposedEpoch, ServerState.LOOKING);
    }

    @Override
    public void shutdown() {
        shutdown = true;
        manager.shutdown();
    }

    static public class Notification {
        int version;

        /**
         * Proposed leader
         */
        long leader;

        /**
         * zxid of the proposed leader
         */
        long zxid;

        /**
         * Epoch
         */
        long electionEpoch;

        /**
         * current state of sender
         */
        int currentState;

        /**
         * Address of sender
         */
        long sid;

        /**
         * epoch of the proposed leader
         */
        long peerEpoch;

        @Override
        public String toString() {
            return Long.toHexString(version) + " (message format version), "
                    + leader + " (n.leader), 0x"
                    + Long.toHexString(zxid) + " (n.zxid), 0x"
                    + Long.toHexString(electionEpoch) + " (n.round), " + currentState
                    + " (n.state), " + sid + " (n.sid), 0x"
                    + Long.toHexString(peerEpoch) + " (n.peerEpoch) ";
        }
    }


    static public class ToSend {
        public interface mType {
            int crequest = 1;
            int challenge = 2;
            int notification = 3;
            int ack = 4;
        }

        ToSend(long leader,
               long zxid,
               long electionEpoch,
               int state,
               long sid,
               long peerEpoch) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.currentState = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
        }

        /**
         * Proposed leader in the case of notification
         */
        long leader;

        /**
         * id contains the tag for acks, and zxid for notifications
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /**
         * Current state;
         */
        int currentState;

        /**
         * Address of recipient
         */
        long sid;

        /**
         * Leader epoch
         */
        long peerEpoch;
    }

    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */
        class WorkerReceiver extends Thread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            @Override
            public void run() {
                QuorumCnxManager.Message response;
                while (!stop) {
                    // Sleeps on receive
                    try {
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if (response == null) {
                            continue;
                        }

                        /*
                         * If it is from an observer, respond right away.
                         * Note that the following predicate assumes that
                         * if a server is not a follower, then it must be
                         * an observer. If we ever have any other type of
                         * learner in the future, we'll have to change the
                         * way we check for observers.
                         */
                        if (!self.getView().containsKey(response.sid)) {
                            Vote current = self.getCurrentVote();
                            ToSend notmsg = new ToSend(current.getId(),
                                    current.getZxid(),
                                    logicalclock,
                                    self.getState(),
                                    response.sid,
                                    current.getPeerEpoch());

                            sendqueue.offer(notmsg);
                        } else {
                            System.out.println("Receive new notification message. My id = " + self.getMyId());


                            // Instantiate Notification and set its attributes
                            Notification n = new Notification();

                            // State of peer that sent this message
                            int ackstate = response.buffer.getInt();
                            if (!ServerState.isValid(ackstate)) {
                                continue;
                            }
                            n.leader = response.buffer.getLong();
                            n.zxid = response.buffer.getLong();
                            n.electionEpoch = response.buffer.getLong();
                            n.currentState = ackstate;
                            n.sid = response.sid;
                            n.peerEpoch = response.buffer.getLong();

                            /*
                             * If this server is looking, then send proposed leader
                             */

                            if (self.getState() == ServerState.LOOKING) {
                                recvqueue.offer(n);
                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                if ((ackstate == ServerState.LOOKING)
                                        && (n.electionEpoch < logicalclock)) {
                                    Vote v = getVote();
                                    ToSend notmsg = new ToSend(
                                            v.getId(),
                                            v.getZxid(),
                                            logicalclock,
                                            self.getState(),
                                            response.sid,
                                            v.getPeerEpoch());
                                    sendqueue.offer(notmsg);
                                }
                            } else {
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                Vote current = self.getCurrentVote();
                                if (ackstate == ServerState.LOOKING) {
                                    ToSend notmsg = new ToSend(
                                            current.getId(),
                                            current.getZxid(),
                                            current.getElectionEpoch(),
                                            self.getState(),
                                            response.sid,
                                            current.getPeerEpoch());
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        System.out.println("Interrupted Exception while waiting for new message" +
                                e.toString());
                    }
                }
            }
        }


        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */
        class WorkerSender extends Thread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager) {
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            @Override
            public void run() {
                while (!stop) {
                    try {
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if (m == null) {
                            continue;
                        }

                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m message to send
             */
            void process(ToSend m) {
                ByteBuffer requestBuffer = buildMsg(m.currentState,
                        m.leader,
                        m.zxid,
                        m.electionEpoch,
                        m.peerEpoch);
                manager.toSend(m.sid, requestBuffer);
            }
        }

        /**
         * Test if both send and receive queues are empty.
         */
        public boolean queueEmpty() {
            return (sendqueue.isEmpty() || recvqueue.isEmpty());
        }


        WorkerSender ws;
        WorkerReceiver wr;

        /**
         * Constructor of class Messenger.
         *
         * @param manager Connection manager
         */
        Messenger(QuorumCnxManager manager) {

            this.ws = new WorkerSender(manager);
            this.wr = new WorkerReceiver(manager);
            ws.setDaemon(true);
            wr.setDaemon(true);
            ws.start();
            wr.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt() {
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }
}
