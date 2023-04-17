package com.example.cdh.properties;

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
    private Integer fetchTimeout;

    /**
     * 默认true，如果设为true，拉取文件时会在同一个application中本地持久化，被若干个executors共享。这使得当同一个主机下有多个executors时，执行任务效率提高。
     */
    private Boolean useFetchCache;

    /**
     * 默认false，是否在执行SparkContext.addFile() 添加文件时，覆盖已有的内容有差异的文件。
     */
    private Boolean overwrite;

    /**
     * 单partition中最多能容纳的文件大小，单位Bytes 默认134217728 (128 MB)
     */
    private Long maxPartitionBytes;

    /**
     * 小文件合并阈值，小于该参数就会被合并到一个partition内。
     * 默认4194304 (4 MB) 。这个参数在将多个文件放入一个partition时被用到，宁可设置的小一些，因为在partition操作中，小文件肯定会比大文件快。
     */
    private Long openCostInBytes;

    public Integer getFetchTimeout() {
        return fetchTimeout;
    }

    public void setFetchTimeout(Integer fetchTimeout) {
        this.fetchTimeout = fetchTimeout;
    }

    public Boolean getUseFetchCache() {
        return useFetchCache;
    }

    public void setUseFetchCache(Boolean useFetchCache) {
        this.useFetchCache = useFetchCache;
    }

    public Boolean getOverwrite() {
        return overwrite;
    }

    public void setOverwrite(Boolean overwrite) {
        this.overwrite = overwrite;
    }

    public Long getMaxPartitionBytes() {
        return maxPartitionBytes;
    }

    public void setMaxPartitionBytes(Long maxPartitionBytes) {
        this.maxPartitionBytes = maxPartitionBytes;
    }

    public Long getOpenCostInBytes() {
        return openCostInBytes;
    }

    public void setOpenCostInBytes(Long openCostInBytes) {
        this.openCostInBytes = openCostInBytes;
    }
}
