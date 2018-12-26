package com.hkx.sz.election.origin;

import java.net.InetSocketAddress;

/**
 * Created by tanhuayou on 2018/12/25
 */
public class QuorumServer {
    public InetSocketAddress addr;

    public InetSocketAddress electionAddr;

    public String hostname;

    public int port = 2888;

    public int electionPort = -1;

    public long id;

    public int learnerType;

    public QuorumServer(long id, InetSocketAddress electionAddr) {
        this.id = id;
        this.electionAddr = electionAddr;
    }
}
