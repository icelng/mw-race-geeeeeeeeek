package com.alibaba.mwrace2018.geeks;

import com.google.common.io.LineProcessor;

import java.util.ArrayList;

/**
 * @author 奥陌
 */
public class TraceLogProcessor implements LineProcessor<Result> {

    private static final int TIMEOUT_THRESHOLD = 200;
    private static final String RESULT_CODE_ERROR = "01";

    private Result result = new Result();

    @Override
    public boolean processLine(String s) {
        TraceLog traceLog = new TraceLog(s);
        if (!this.result.getLogs().containsKey(traceLog.getTraceId())) {
            this.result.getLogs().put(traceLog.getTraceId(), new ArrayList<TraceLog>());
        }
        this.result.getLogs().get(traceLog.getTraceId()).add(traceLog);

        if (this.select(traceLog)) {
            this.result.getTargetTraceIds().add(traceLog.getTraceId());
        }

        return true;
    }

    @Override
    public Result getResult() {
        return this.result;
    }

    /**
     * 当满足以下条件时选取该条日志.
     * 1. 耗时 > 200
     * 2. 错误码 = 01
     * 3. 用户数据中含有 @force=1
     * @param log 日志
     * @return 是否选取该条日志.
     */
    private boolean select(TraceLog log) {
        if (log.getRt() > TIMEOUT_THRESHOLD) {
            return true;
        }
        if (RESULT_CODE_ERROR.equals(log.getResultCode())) {
            return true;
        }
        return log.getUserData() != null && log.getUserData().contains("@force=1");
    }

}
