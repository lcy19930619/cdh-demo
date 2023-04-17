package com.example.cdh.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author chunyang.leng
 * @date 2023-04-12 13:33
 */
@ConfigurationProperties("spark.store")
@Configuration
public class SparkStoreProperties {
    /**
     * 从磁盘上读文件时，最小单位不能少于该设定值，默认2m，小于或者接近操作系统的每个page的大小。
     */
    private String memoryMapThreshold;

    public String getMemoryMapThreshold() {
        return memoryMapThreshold;
    }

    public void setMemoryMapThreshold(String memoryMapThreshold) {
        this.memoryMapThreshold = memoryMapThreshold;
    }
}
