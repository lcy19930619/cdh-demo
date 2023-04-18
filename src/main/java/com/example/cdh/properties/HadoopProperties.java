package com.example.cdh.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author chunyang.leng
 * @date 2023-04-17 10:31
 */
@Configuration
@ConfigurationProperties(prefix = "hadoop")
public class HadoopProperties {
    /**
     * master 地址，示例：hdfs://cdh-master:8020
     */
    private String url;

    /**
     * 分片数量
     */
    private String replication;
    /**
     * 块文件大小
     */
    private String blockSize;
    /**
     * 操作的用户
     */
    private String user;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getReplication() {
        return replication;
    }

    public void setReplication(String replication) {
        this.replication = replication;
    }

    public String getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(String blockSize) {
        this.blockSize = blockSize;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

}
