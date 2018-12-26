package com.hkx.sz.election.origin;

import com.hkx.sz.common.ServerState;
import com.hkx.sz.common.Vote;

import java.util.Collections;
import java.util.Map;

/**
 * Created by tanhuayou on 2018/12/26
 */
public class QuorumPeer {
    private final long myId;
    private final int learnerType;
    /**
     * 这里先都认为一直
     */
    private long dataTreeLastProcessedZxid = 2;

    /**
     * This is who I think the leader currently is.
     */
    private volatile Vote currentVote;

    private long currentEpoch = -1;


    private final Map<Long, QuorumServer> view;

    private volatile int state = ServerState.LOOKING;

    public QuorumPeer(long myId, int learnerType, Map<Long, QuorumServer> view) {
        this.myId = myId;
        this.learnerType = learnerType;
        this.view = view;
        this.currentVote = new Vote(myId, 0, 0, 0, ServerState.LOOKING);
    }

    public long getMyId() {
        return myId;
    }

    public int getLearnerType() {
        return learnerType;
    }

    public long getLastProcessedZxid() {
        return dataTreeLastProcessedZxid;
    }

    public Vote getCurrentVote() {
        return currentVote;
    }

    public long getCurrentEpoch() {
        return currentEpoch;
    }

    public Map<Long, QuorumServer> getView() {
        return Collections.unmodifiableMap(view);
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }
}
