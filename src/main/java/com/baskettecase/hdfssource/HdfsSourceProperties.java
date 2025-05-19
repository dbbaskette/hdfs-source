
package com.baskettecase.hdfssource;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "hdfs")
public class HdfsSourceProperties {
    private String uri;
    private String directory;
    private long pollInterval = 5000;

    public String getUri() { return uri; }
    public void setUri(String uri) { this.uri = uri; }

    public String getDirectory() { return directory; }
    public void setDirectory(String directory) { this.directory = directory; }

    public long getPollInterval() { return pollInterval; }
    public void setPollInterval(long pollInterval) { this.pollInterval = pollInterval; }
}