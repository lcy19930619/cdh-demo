package com.example.cdh.properties.spark;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author chunyang.leng
 * @date 2023-04-12 13:35
 */
@Configuration
@ConfigurationProperties(prefix = "spark.reducer")
public class SparkReducerProperties {
    /**
     * 默认48m。从每个reduce任务同时拉取的最大map数，每个reduce都会在完成任务后，需要一个堆外内存的缓冲区来存放结果，如果没有充裕的内存就尽可能把这个调小一点。。相反，堆外内存充裕，调大些就能节省gc时间。
     */
    private String maxSizeInFlight;

    /**
     * 限制了每个主机每次reduce可以被多少台远程主机拉取文件块，调低这个参数可以有效减轻node manager的负载。（默认值Int.MaxValue）
     */
    private String maxBlocksInFlightPerAddress;

    /**
     * 限制远程机器拉取本机器文件块的请求数，随着集群增大，需要对此做出限制。否则可能会使本机负载过大而挂掉。。（默认值为Int.MaxValue）
     */
    private String maxReqsInFlight;

    /**
     * shuffle请求的文件块大小 超过这个参数值，就会被强行落盘，防止一大堆并发请求把内存占满。（默认Long.MaxValue）
     */
    private String maxReqSizeShuffleToMem;

    public String getMaxSizeInFlight() {
        return maxSizeInFlight;
    }

    public void setMaxSizeInFlight(String maxSizeInFlight) {
        this.maxSizeInFlight = maxSizeInFlight;
    }

    public String getMaxBlocksInFlightPerAddress() {
        return maxBlocksInFlightPerAddress;
    }

    public void setMaxBlocksInFlightPerAddress(String maxBlocksInFlightPerAddress) {
        this.maxBlocksInFlightPerAddress = maxBlocksInFlightPerAddress;
    }

    public String getMaxReqsInFlight() {
        return maxReqsInFlight;
    }

    public void setMaxReqsInFlight(String maxReqsInFlight) {
        this.maxReqsInFlight = maxReqsInFlight;
    }

    public String getMaxReqSizeShuffleToMem() {
        return maxReqSizeShuffleToMem;
    }

    public void setMaxReqSizeShuffleToMem(String maxReqSizeShuffleToMem) {
        this.maxReqSizeShuffleToMem = maxReqSizeShuffleToMem;
    }
}
