package com.alibaba.mwrace2018.geeks;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author 奥陌
 */
class Result {

    /**
     * 缓存全量日志.
     */
    private Map<String, List<TraceLog>> logs = new HashMap<>();

    /**
     * 保存命中的 TraceId.
     */
    private Set<String> targetTraceIds = new HashSet<>();

    Map<String, List<TraceLog>> getLogs() {
        return this.logs;
    }

    Set<String> getTargetTraceIds() {
        return this.targetTraceIds;
    }

}
