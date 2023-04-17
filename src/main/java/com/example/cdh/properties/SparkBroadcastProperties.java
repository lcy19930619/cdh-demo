package com.example.cdh.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author chunyang.leng
 * @date 2023-04-12 13:31
 */
@ConfigurationProperties("spark.broadcast")
@Configuration
public class SparkBroadcastProperties {

    /**
     * TorrentBroadcastFactory中的每一个block大小，默认4m
     * 过大会减少广播时的并行度，过小会导致BlockManager 产生 performance hit.
     */
    private String blockSize;

    public String getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(String blockSize) {
        this.blockSize = blockSize;
    }
}
