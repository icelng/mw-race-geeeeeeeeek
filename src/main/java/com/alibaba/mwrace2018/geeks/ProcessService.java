package com.alibaba.mwrace2018.geeks;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;

public class ProcessService {
    private static final int THREAD_NUM = 4;
    private Thread thread[] = new Thread[THREAD_NUM];
    private Semaphore requestNum = new Semaphore(0);
    private Deque<String> requestQueue = new ConcurrentLinkedDeque<>();
    private TraceLogProcessor traceLogProcessor;

    public ProcessService(TraceLogProcessor traceLogProcessor) {
        this.traceLogProcessor = traceLogProcessor;
        for (int i = 0;i < THREAD_NUM;i++) {
            thread[i] = new Thread(() -> {
                while (true) {
                    try {
                        requestNum.acquire();
                    } catch (InterruptedException e) {
                        break;
                    }
                    String s = requestQueue.pollFirst();
                    traceLogProcessor.doProcess(s);
                }

            });
            thread[i].start();
        }
    }

    public void stop(){
        for (int i = 0;i < THREAD_NUM;i++) {
            thread[i].interrupt();
        }
    }

    public void requestProcess(String s) {
        requestQueue.addLast(s);
        requestNum.release();
    }
}
