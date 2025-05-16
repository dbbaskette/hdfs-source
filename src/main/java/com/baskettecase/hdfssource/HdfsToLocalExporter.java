package com.baskettecase.hdfssource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

/**
 * HdfsToLocalExporter is used in standalone mode (non-SCDF).
 * It reads files from HDFS and writes them to a local output directory as configured.
 */
@Component
public class HdfsToLocalExporter {
    private static final Logger logger = LoggerFactory.getLogger(HdfsToLocalExporter.class);

    @Value("${hdfs.uri}")
    private String hdfsUri;
    @Value("${hdfs.directory}")
    private String hdfsDirectory;
    @Value("${local.output.dir:output}")
    private String localOutputDir;

    /**
     * Copies all files from the configured HDFS directory to the local output directory.
     * Creates directories as needed and logs progress.
     * @throws IOException if any file operation fails
     */
    public void export() throws IOException {
        logger.info("Starting HDFS to local export: {} -> {}", hdfsUri + hdfsDirectory, localOutputDir);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(hdfsDirectory);
        java.nio.file.Path outputDir = java.nio.file.Paths.get(localOutputDir);
        Files.createDirectories(outputDir);
        copyFromHdfs(fs, srcPath, outputDir);
        logger.info("Done copying files from HDFS to {}", outputDir.toAbsolutePath());
    }

    /**
     * Recursively copies files and directories from HDFS to local filesystem.
     * @param fs HDFS FileSystem
     * @param src Source HDFS path
     * @param dest Destination local path
     * @throws IOException if any file operation fails
     */
    private void copyFromHdfs(FileSystem fs, Path src, java.nio.file.Path dest) throws IOException {
        FileStatus[] statuses = fs.listStatus(src);
        for (FileStatus status : statuses) {
            Path filePath = status.getPath();
            java.nio.file.Path localPath = dest.resolve(filePath.getName());
            if (status.isDirectory()) {
                Files.createDirectories(localPath);
                copyFromHdfs(fs, filePath, localPath);
            } else {
                try (InputStream in = fs.open(filePath);
                     FileOutputStream out = new FileOutputStream(localPath.toFile())) {
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = in.read(buffer)) > 0) {
                        out.write(buffer, 0, bytesRead);
                    }
                }
                logger.info("Copied: {} -> {}", filePath, localPath);
            }
        }
    }
}
