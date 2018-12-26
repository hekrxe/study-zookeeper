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

    public int getVersion() {
        return version;
    }

    public long getId() {
        return id;
    }

    public long getZxid() {
        return zxid;
    }

    public long getElectionEpoch() {
        return electionEpoch;
    }

    public long getPeerEpoch() {
        return peerEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Vote)) {
            return false;
        }
        Vote other = (Vote) o;


        /*
         * There are two things going on in the logic below.
         * First, we compare votes of servers out of election
         * using only id and peer epoch. Second, if one version
         * is 0x0 and the other isn't, then we only use the
         * leader id. This case is here to enable rolling upgrades.
         *
         * {@see https://issues.apache.org/jira/browse/ZOOKEEPER-1805}
         */
        if ((state == ServerState.LOOKING) ||
                (other.state == ServerState.LOOKING)) {
            return (id == other.id
                    && zxid == other.zxid
                    && electionEpoch == other.electionEpoch
                    && peerEpoch == other.peerEpoch);
        } else {
            if ((version > 0x0) ^ (other.version > 0x0)) {
                return id == other.id;
            } else {
                return (id == other.id
                        && peerEpoch == other.peerEpoch);
            }
        }
    }

    @Override
    public int hashCode() {
        return (int) (id & zxid);
    }

    @Override
    public String toString() {
        return "Vote{" +
                "version=" + version +
                ", id=" + id +
                ", zxid=" + zxid +
                ", electionEpoch=" + electionEpoch +
                ", peerEpoch=" + peerEpoch +
                ", state=" + state +
                '}';
    }

    public int getState() {
        return state;
    }


}
