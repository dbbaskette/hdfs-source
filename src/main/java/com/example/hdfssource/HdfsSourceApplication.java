package com.example.hdfssource;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(HdfsSourceProperties.class)
public class HdfsSourceApplication {
    public static void main(String[] args) {
        SpringApplication.run(HdfsSourceApplication.class, args);
    }
}
