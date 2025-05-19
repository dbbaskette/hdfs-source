package com.baskettecase.hdfssource;


import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Configuration
@EnableScheduling
@Profile("scdf")
public class HdfsFileSupplier {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Set<String> seen = new HashSet<>();
    private final FileSystem fs;
    private final Path dir;
    private final StreamBridge bridge;

    public HdfsFileSupplier(HdfsSourceProperties props, StreamBridge bridge) throws Exception {
        this.bridge = bridge;
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", props.getUri());
        this.fs = FileSystem.get(conf);
        this.dir = new Path(props.getDirectory());
    }

    @Scheduled(fixedDelayString = "${hdfs.poll-interval}")
    public void poll() {
        try {
            for (FileStatus f : fs.listStatus(dir)) {
                Path path = f.getPath();
                if (!seen.contains(path.getName()) && f.isFile()) {
                    try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))) {
                        String content = reader.lines().collect(Collectors.joining("\n"));
                        Message<String> msg = MessageBuilder.withPayload(content)
                            .setHeader("file_name", path.getName()).build();
                        bridge.send("hdfsFileSupplier-out-0", msg);
                        seen.add(path.getName());
                    } catch (Exception ex) {
                        log.error("Error reading file {}", path.getName(), ex);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error reading HDFS files", e);
        }
    }

    @Bean
    public Supplier<String> hdfsSupplier() {
        return () -> null;
    }
}