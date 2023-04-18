package com.example.cdh.properties.spark;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author chunyang.leng
 * @date 2023-04-12 13:27
 */
@ConfigurationProperties("spark.files")
@Configuration
public class SparkFilesProperties {

    /**
     * 从driver端执行SparkContext.addFile() 抓取添加的文件的超时时间，默认60s
     */
    private String fetchTimeout;

    /**
     * 默认true，如果设为true，拉取文件时会在同一个application中本地持久化，被若干个executors共享。这使得当同一个主机下有多个executors时，执行任务效率提高。
     */
    private String useFetchCache;

    /**
     * 默认false，是否在执行SparkContext.addFile() 添加文件时，覆盖已有的内容有差异的文件。
     */
    private String overwrite;

    /**
     * 单partition中最多能容纳的文件大小，单位Bytes 默认134217728 (128 MB)
     */
    private String maxPartitionBytes;

    /**
     * 小文件合并阈值，小于该参数就会被合并到一个partition内。
     * 默认4194304 (4 MB) 。这个参数在将多个文件放入一个partition时被用到，宁可设置的小一些，因为在partition操作中，小文件肯定会比大文件快。
     */
    private String openCostInBytes;

    public String getFetchTimeout() {
        return fetchTimeout;
    }

    public void setFetchTimeout(String fetchTimeout) {
        this.fetchTimeout = fetchTimeout;
    }

    public String getUseFetchCache() {
        return useFetchCache;
    }

    public void setUseFetchCache(String useFetchCache) {
        this.useFetchCache = useFetchCache;
    }

    public String getOverwrite() {
        return overwrite;
    }

    public void setOverwrite(String overwrite) {
        this.overwrite = overwrite;
    }

    public String getMaxPartitionBytes() {
        return maxPartitionBytes;
    }

    public void setMaxPartitionBytes(String maxPartitionBytes) {
        this.maxPartitionBytes = maxPartitionBytes;
    }

    public String getOpenCostInBytes() {
        return openCostInBytes;
    }

    public void setOpenCostInBytes(String openCostInBytes) {
        this.openCostInBytes = openCostInBytes;
    }
}
