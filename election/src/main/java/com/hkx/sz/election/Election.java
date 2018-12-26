package com.hkx.sz.election;

import com.hkx.sz.common.Vote;

/**
 * Created by tanhuayou on 2018/12/25
 */
public interface Election {

    Vote lookForLeader(Vote currentVote);

    void shutdown();
}
