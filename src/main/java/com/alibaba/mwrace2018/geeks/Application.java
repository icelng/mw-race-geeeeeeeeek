package com.alibaba.mwrace2018.geeks;

import com.google.common.io.CharSink;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.io.CharSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author 奥陌
 */
@SpringBootApplication
public class Application implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final TraceLogComparator COMPARATOR = new TraceLogComparator();

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

        CharSource source = Resources.asCharSource(new URL(this.dataUrl), Charset.forName("UTF-8"));
        TraceLogProcessor processor = new TraceLogProcessor();
        return source.readLines(processor);
    }

    private void output(Result result) throws IOException {
        LOGGER.info("outputDir = {}", this.outputDir);

        for (String traceId : result.getTargetTraceIds()) {
            String filePath = this.outputDir + "/" + traceId;
            LOGGER.info("filePath = {}", filePath);
            CharSink sink = Files.asCharSink(new File(filePath), Charset.forName("UTF-8"));
            sink.writeLines(this.sort(result.getLogs().get(traceId)));
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