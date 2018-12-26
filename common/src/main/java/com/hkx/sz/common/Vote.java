package com.hkx.sz.common;

/**
 * Created by tanhuayou on 2018/12/25
 */
public class Vote {
    final private int version;

    final private long id;

    final private long zxid;

    final private long electionEpoch;

    final private long peerEpoch;

    final private int state;

    public Vote(int version, long id, long zxid, long electionEpoch, long peerEpoch, int state) {
        this.version = version;
        this.id = id;
        this.zxid = zxid;
        this.electionEpoch = electionEpoch;
        this.peerEpoch = peerEpoch;
        this.state = state;
    }

    public Vote(long id, long zxid, long electionEpoch, long peerEpoch, int state) {
        this(0x0, id, zxid, electionEpoch, peerEpoch, state);
    }
}
