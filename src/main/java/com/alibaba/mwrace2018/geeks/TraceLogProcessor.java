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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 奥陌
 */
public class TraceLogProcessor implements LineProcessor<Result> {
    private static final Logger logger = LoggerFactory.getLogger(TraceLogByteProcessor.class);

    private static final int TIMEOUT_THRESHOLD = 200;
    private static final String RESULT_CODE_ERROR = "01";
    private static final int TERM_LEN = 512;
    private static final TraceLogComparator COMPARATOR = new TraceLogComparator();

    private Object lock = new Object();
    private FlushService flushService;
    private ProcessService processService;

    private Map<String, TraceLogList> traceLogListMap = new ConcurrentHashMap<>();

    private AtomicInteger curTerm = new AtomicInteger(0);
    private AtomicInteger termDays = new AtomicInteger(0);

    private Result result = new Result();

    private String outputDir;

    public TraceLogProcessor(String outputDir) {
        super();
        this.outputDir = outputDir;
        flushService = new FlushService(outputDir);
        processService = new ProcessService(this);
    }

    @Override
    public boolean processLine(String s) throws IOException {
        processService.requestProcess(s);
        return true;
    }

    public void doProcess(String s) {
        TraceLog traceLog = new TraceLog(s);
//        String seqNum = s.substring(21, 24);

        if (!traceLogListMap.containsKey(traceLog.getTraceId())) {
            TraceLogList traceLogList = new TraceLogList();
            traceLogListMap.put(traceLog.getTraceId(), traceLogList);
        }

        TraceLogList traceLogList = traceLogListMap.get(traceLog.getTraceId());
        traceLogList.getTraceLogs().add(traceLog);
        traceLogList.setTerm(curTerm.get());

        if (this.select(traceLog)) {
            traceLogList.setTarget(true);
        }

        synchronized (lock) {
            if (termDays.getAndAdd(1) >= TERM_LEN) {
                /*满一任期*/
                Set<String> traceIds = traceLogListMap.keySet();
                for (String traceId : traceIds) {
                    traceLogList = traceLogListMap.get(traceId);

                    if (traceLogList.getTerm() != curTerm.get()) {
                        /*当前任期无活动，则判决传输完毕*/
                        traceLogListMap.remove(traceId);
                        if (traceLogList.isTarget()) {
                            /*是输出目标*/
//                        String filePath = this.outputDir + "/" + traceId;
//                        CharSink sink = Files.asCharSink(new File(filePath), Charset.forName("UTF-8"));
//                        sink.writeLines(this.sort(traceLogList.getTraceLogs()));
                            flushService.requestFlush(traceLogList);
                        }
                    }

                }

                termDays.set(0);
                curTerm.getAndAdd(1);
            }
        }
    }

    @Override
    public Result getResult() {
        flushService.stop();
        processService.stop();
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
