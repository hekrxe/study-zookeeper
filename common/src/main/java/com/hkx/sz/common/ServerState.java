package com.hkx.sz.common;

/**
 * Created by tanhuayou on 2018/12/25
 */
public interface ServerState {
    int BASE = 1;

    int LOOKING = BASE << 1;
    int FOLLOWING = BASE << 2;
    int LEADING = BASE << 3;
    int OBSERVING = BASE << 4;


    static boolean isValid(int state) {
        return (state == LOOKING || state == FOLLOWING || state == LEADING || state == OBSERVING);
    }
}
