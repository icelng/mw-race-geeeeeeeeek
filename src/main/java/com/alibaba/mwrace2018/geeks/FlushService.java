package com.alibaba.mwrace2018.geeks;

import com.google.common.io.CharSink;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public class FlushService {
    private static final TraceLogComparator COMPARATOR = new TraceLogComparator();
    private Semaphore requestNum = new Semaphore(0);
    private Deque<TraceLogList> requestQueue = new ConcurrentLinkedDeque<>();
    private String outputDir;
    private Thread thread;
    private AtomicBoolean stopFlag = new AtomicBoolean(false);

    public FlushService(String outputDir) {
        this.outputDir = outputDir;
        thread = new Thread(() -> {

            while (!stopFlag.get()) {
                requestNum.acquireUninterruptibly();
                TraceLogList traceLogList = requestQueue.pollFirst();
                String filePath = this.outputDir + "/" + traceLogList.getTraceLogs().get(0).getTraceId();
                CharSink sink = Files.asCharSink(new File(filePath), Charset.forName("UTF-8"));
                try {
                    sink.writeLines(this.sort(traceLogList.getTraceLogs()));
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

        });
        thread.start();
    }

    public void stop(){
        stopFlag.set(true);
    }

    public void requestFlush(TraceLogList traceLogList) {
        requestQueue.addLast(traceLogList);
        requestNum.release();
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
