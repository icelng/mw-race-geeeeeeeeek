package com.alibaba.mwrace2018.geeks;

import java.util.LinkedList;
import java.util.List;

public class TraceLogList {
    private List<TraceLog> traceLogs = new LinkedList<>();
    private boolean isTarget = false;
    private int term = 0;

    public boolean isTarget() {
        return isTarget;
    }

    public void setTarget(boolean target) {
        isTarget = target;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public List<TraceLog> getTraceLogs() {
        return traceLogs;
    }

    public void setTraceLogs(List<TraceLog> traceLogs) {
        this.traceLogs = traceLogs;
    }
}
