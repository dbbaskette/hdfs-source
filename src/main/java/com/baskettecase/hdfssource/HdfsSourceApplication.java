package com.baskettecase.hdfssource;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * Main entrypoint for the HDFS Source application.
 * Runs in two modes:
 *  - Standalone: Exports files from HDFS to local output directory.
 *  - SCDF: (when 'scdf' profile is active) - SCDF beans handle streaming.
 */
@SpringBootApplication
@EnableConfigurationProperties(HdfsSourceProperties.class)
public class HdfsSourceApplication {
    /**
     * Application entrypoint.
     * @param args Command-line arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(HdfsSourceApplication.class, args);
    }
}
