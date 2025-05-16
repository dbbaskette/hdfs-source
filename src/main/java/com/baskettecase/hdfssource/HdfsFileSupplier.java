package com.baskettecase.hdfssource;

import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.cloud.stream.function.StreamBridge;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.springframework.context.annotation.Profile;

/**
 * HdfsFileSupplier is active only in 'scdf' profile.
 * It polls an HDFS directory and emits file contents to a Spring Cloud Stream output binding (e.g., RabbitMQ).
 * This enables integration with Spring Cloud Data Flow pipelines.
 */
@Configuration
@EnableScheduling
@Profile("scdf")
public class HdfsFileSupplier {
    private static final Logger logger = LoggerFactory.getLogger(HdfsFileSupplier.class);

    private final HdfsSourceProperties properties;
    private final StreamBridge streamBridge;
    private final Set<String> processedFiles = new HashSet<>();
    private FileSystem fileSystem;

    /**
     * Constructor initializes HDFS FileSystem using properties injected from configuration.
     * @param properties HDFS connection and polling properties
     * @param streamBridge Spring Cloud Stream bridge for sending messages
     */
    @Autowired
    public HdfsFileSupplier(HdfsSourceProperties properties, StreamBridge streamBridge) {
        this.properties = properties;
        this.streamBridge = streamBridge;
        try {
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            conf.set("fs.defaultFS", properties.getUri());
            this.fileSystem = FileSystem.get(conf);
        } catch (Exception e) {
            logger.error("Failed to initialize HDFS FileSystem", e);
        }
    }

    /**
     * Scheduled method that polls the configured HDFS directory for new files.
     * For each new file, reads its contents and sends as a message to the output binding.
     */
    @Scheduled(fixedDelayString = "${hdfs.poll-interval:5000}")
    public void pollDirectory() {
        if (fileSystem == null) return;
        try {
            Path dir = new Path(properties.getDirectory());
            FileStatus[] files = fileSystem.listStatus(dir);
            for (FileStatus fileStatus : files) {
                Path filePath = fileStatus.getPath();
                String fileName = filePath.getName();
                if (!processedFiles.contains(fileName) && fileStatus.isFile()) {
                    logger.info("Processing file: {}", fileName);
                    try (FSDataInputStream in = fileSystem.open(filePath);
                         BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                        StringBuilder content = new StringBuilder();
                        String line;
                        while ((line = reader.readLine()) != null) {
                            content.append(line).append("\n");
                        }
                        Message<String> message = MessageBuilder.withPayload(content.toString())
                                .setHeader("file_name", fileName)
                                .build();
                        streamBridge.send("hdfsFileSupplier-out-0", message);
                        processedFiles.add(fileName);
                    } catch (Exception ex) {
                        logger.error("Error reading file {}", fileName, ex);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error polling HDFS directory", e);
        }
    }

    /**
     * SCDF requires a Supplier bean for registration, but actual file sending is handled by scheduled polling.
     * @return null (no direct supply)
     */
    @Bean
    public Supplier<String> hdfsFileSupplier() {
        // This Supplier is required for SCDF registration, but actual sending is via scheduled poll.
        return () -> null;
    }
}
