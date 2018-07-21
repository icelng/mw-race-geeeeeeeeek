package com.alibaba.mwrace2018.geeks;

import com.google.common.io.CharSink;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.*;

/**
 * @author 奥陌
 */
public class TraceLogProcessor implements LineProcessor<Result> {
    private static final Logger logger = LoggerFactory.getLogger(TraceLogByteProcessor.class);

    private static final int TIMEOUT_THRESHOLD = 200;
    private static final String RESULT_CODE_ERROR = "01";
    private static final int TERM_LEN = 512;
    private static final TraceLogComparator COMPARATOR = new TraceLogComparator();

    private IdlePageManager idlePageManager;
    private StoreIO storeIO;
    private ResidentPageCachePool residentPageCachePool;

    private Map<String, TraceLogList> traceLogListMap = new HashMap<>();

    private int curTerm = 0;
    private int termDays = 0;

    private Result result = new Result();

    @Value("${output.dir}")
    private String outputDir;


    public TraceLogProcessor(IdlePageManager idlePageManager, StoreIO storeIO, ResidentPageCachePool residentPageCachePool) {
        super();
        this.idlePageManager = idlePageManager;
        this.storeIO = storeIO;
        this.residentPageCachePool = residentPageCachePool;
    }

    @Override
    public boolean processLine(String s) throws IOException {
        TraceLog traceLog = new TraceLog(s);
//        String seqNum = s.substring(21, 24);

        if (!traceLogListMap.containsKey(traceLog.getTraceId())) {
            TraceLogList traceLogList = new TraceLogList();
            traceLogList.getTraceLogs().add(traceLog);
            traceLogListMap.put(traceLog.getTraceId(), traceLogList);
        }

        TraceLogList traceLogList = traceLogListMap.get(traceLog.getTraceId());
        traceLogList.setTerm(curTerm);

        if (this.select(traceLog)) {
            traceLogList.setTarget(true);
        }

        if (termDays++ >= TERM_LEN) {
            /*满一任期*/
            Set<String> traceIds = traceLogListMap.keySet();
            if (traceIds == null) {
                logger.info("Ids is empty????????????????????");
            }
            for (String traceId : traceIds) {
                traceLogList = traceLogListMap.get(traceId);

                if (traceLogList.getTerm() != curTerm) {
                    /*当前任期无活动，则判决传输完毕*/
                    if (traceLogList.isTarget()) {
                        /*是输出目标*/
                        String filePath = this.outputDir + "/" + traceId;
                        CharSink sink = Files.asCharSink(new File(filePath), Charset.forName("UTF-8"));
                        sink.writeLines(this.sort(traceLogList.getTraceLogs()));
                    }
                    traceLogListMap.remove(traceId);
                }

            }

            termDays = 0;
            curTerm++;
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

    private List<String> sort(List<TraceLog> logs) {
        List<String> lines = new ArrayList<>(logs.size());
        Collections.sort(logs, COMPARATOR);
        for (TraceLog log : logs) {
            lines.add(log.getLine());
        }
        return lines;
    }

}
