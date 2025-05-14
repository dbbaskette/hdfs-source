package com.example.hdfssource;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "hdfs")
public class HdfsSourceProperties {
    /** HDFS URI, e.g. hdfs://namenode:8020 */
    private String uri;
    /** Directory to watch on HDFS */
    private String directory;
    /** Poll interval in ms */
    private long pollInterval = 5000;

    public String getUri() { return uri; }
    public void setUri(String uri) { this.uri = uri; }

    public String getDirectory() { return directory; }
    public void setDirectory(String directory) { this.directory = directory; }

    public long getPollInterval() { return pollInterval; }
    public void setPollInterval(long pollInterval) { this.pollInterval = pollInterval; }
}
