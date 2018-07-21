package com.alibaba.mwrace2018.geeks;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 奥陌
 */
class Result {


    /**
     * 使用消息队列保存顺序号对应的日志
     * */
    private Map<String, MessageQueue> seqNumQueueMap = new ConcurrentHashMap<>();

    /**
     * 缓存全量日志.
     */
//    private Map<String, List<TraceLog>> logs = new HashMap<>();

//    private Map<String, TraceLogList> traceLogListMap = new HashMap<>();

    /**
     * 保存命中的 TraceId.
     */
    private Set<String> targetTraceIds = new HashSet<>();

//    Map<String, List<TraceLog>> getLogs() {
//        return this.logs;
//    }

    Set<String> getTargetTraceIds() {
        return this.targetTraceIds;
    }

    public Map<String, MessageQueue> getSeqNumQueueMap() {
        return seqNumQueueMap;
    }
}
