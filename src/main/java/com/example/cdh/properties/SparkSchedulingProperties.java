package com.example.cdh.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author chunyang.leng
 * @date 2023-04-12 14:09
 */
@ConfigurationProperties("spark.scheduler")
@Configuration
public class SparkSchedulingProperties {
    /**
     * 在执行前最大等待申请资源的时间，默认30s。
     */
    private Integer maxRegisteredResourcesWaitingTime;
    /**
     * 实际注册的资源数占预期需要的资源数的比例，默认0.8
     */
    private Float minRegisteredResourcesRatio;
    /**
     * 调度模式，默认FIFO 先进队列先调度，可以选择FAIR。
     */
    private String mode;

    public Integer getMaxRegisteredResourcesWaitingTime() {
        return maxRegisteredResourcesWaitingTime;
    }

    public void setMaxRegisteredResourcesWaitingTime(Integer maxRegisteredResourcesWaitingTime) {
        this.maxRegisteredResourcesWaitingTime = maxRegisteredResourcesWaitingTime;
    }

    public Float getMinRegisteredResourcesRatio() {
        return minRegisteredResourcesRatio;
    }

    public void setMinRegisteredResourcesRatio(Float minRegisteredResourcesRatio) {
        this.minRegisteredResourcesRatio = minRegisteredResourcesRatio;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }
}
