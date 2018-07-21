package com.alibaba.mwrace2018.geeks;

import com.google.common.io.LineProcessor;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

/**
 * @author 奥陌
 */
public class TraceLogProcessor implements LineProcessor<Result> {

    private static final int TIMEOUT_THRESHOLD = 200;
    private static final String RESULT_CODE_ERROR = "01";

    private IdlePageManager idlePageManager;
    private StoreIO storeIO;
    private ResidentPageCachePool residentPageCachePool;

    private Result result = new Result();


    public TraceLogProcessor(IdlePageManager idlePageManager, StoreIO storeIO, ResidentPageCachePool residentPageCachePool) {
        super();
        this.idlePageManager = idlePageManager;
        this.storeIO = storeIO;
        this.residentPageCachePool = residentPageCachePool;
    }

    @Override
    public boolean processLine(String s) throws UnsupportedEncodingException {
        TraceLog traceLog = new TraceLog(s);
        String seqNum = s.substring(21, 24);

        if (!this.result.getSeqNumQueueMap().containsKey(seqNum)) {
            /*如果不存在顺序号对应的队列*/
            this.result.getSeqNumQueueMap().put(seqNum, new AppendedIndexMessageQueue(idlePageManager, storeIO,residentPageCachePool));
        }

        MessageQueue seqNumMessageQueue = this.result.getSeqNumQueueMap().get(seqNum);
        seqNumMessageQueue.put(s.getBytes("UTF-8"), s.length());

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
