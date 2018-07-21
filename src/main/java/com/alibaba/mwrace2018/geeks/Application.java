package com.alibaba.mwrace2018.geeks;

import com.google.common.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;

/**
 * @author 奥陌
 */
@SpringBootApplication
public class Application implements CommandLineRunner {

    private static final String MESSAGES_FILE_PATH = "./log";
    private static final long FILE_SIZE_8G = 8 * 1024 * 1024 * 1024L;  //4G
    private static final long FILE_SIZE = FILE_SIZE_8G;  //4G
    private static final int REGION_SIZE = 512 * 1024 * 1024;  //4G
    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final TraceLogComparator COMPARATOR = new TraceLogComparator();


    private IdlePageManager idlePageManager;
    private StoreIO storeIO;
    private ResidentPageCachePool residentPageCachePool;

    /**
     * 获取数据的 URL 地址.
     */
    @Value("${data.url}")
    private String dataUrl;

    /**
     * 结果输出目录.
     */
    @Value("${output.dir}")
    private String outputDir;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        Result result = this.process();
        this.output(result);
    }

    private Result process() throws IOException {
        LOGGER.info("dataUrl = {}", this.dataUrl);

        storeIO = new StoreIO(outputDir + "/log", FILE_SIZE, REGION_SIZE);
        idlePageManager = new SingleUseIdlePageManager(FILE_SIZE, 1024);
        residentPageCachePool = new ResidentPageCachePool(65535, 1024);

        ByteSource byteSource = Resources.asByteSource(new URL(this.dataUrl));
//        CharSource source = Resources.asCharSource(new URL(this.dataUrl), Charset.forName("UTF-8"));
//        URL url = Resources.getResource(this.dataUrl);
//        CharSource source = Resources.asCharSource(url, Charset.forName("UTF-8"));
//        TraceLogProcessor processor = new TraceLogProcessor(idlePageManager, storeIO, residentPageCachePool);
        TraceLogByteProcessor processor = new TraceLogByteProcessor(idlePageManager, storeIO, residentPageCachePool);
//        return source.readLines(processor);
        return byteSource.read(processor);
    }

    private void output(Result result) throws IOException {
        LOGGER.info("outputDir = {}", this.outputDir);

        for (int i = 0;i < 65535;i++) {
            String seqNum = String.valueOf(i);
            MessageQueue messageQueue = result.getSeqNumQueueMap().get(seqNum);

            if (messageQueue == null) {
                continue;
            }

            Map<String, List<TraceLog>> logsMap = new HashMap<>();
            long offset = 0;
            while (true) {
                List<byte[]> lines = messageQueue.get(offset, 10);

                for (byte[] line : lines) {
                    String strLine = new String(line);
                    TraceLog traceLog = new TraceLog(strLine);
                    if (result.getTargetTraceIds().contains(traceLog.getTraceId())) {
                        if (!logsMap.containsKey(traceLog.getTraceId())) {
                            List<TraceLog> logs = new ArrayList<>();
                            logsMap.put(traceLog.getTraceId(), logs);
                        }

                        List<TraceLog> logs = logsMap.get(traceLog.getTraceId());
                        logs.add(traceLog);

                    }
                }

                if (lines.size() < 10) {
                    /*说明取空了*/
                    break;
                }
                offset += 10;
            }

            for (String traceId : logsMap.keySet()) {
                String filePath = this.outputDir + "/" + traceId;
//            LOGGER.info("filePath = {}", filePath);
                CharSink sink = Files.asCharSink(new File(filePath), Charset.forName("UTF-8"));
                sink.writeLines(this.sort(logsMap.get(traceId)));
            }

        }
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