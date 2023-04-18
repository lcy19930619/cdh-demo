package com.example.cdh.properties.spark;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author chunyang.leng
 * @date 2023-04-12 13:23
 */
@ConfigurationProperties("spark.executor")
@Configuration
public class SparkExecutorProperties {
    /**
     * 每个executor的核数，默认yarn下1核，standalone下为所有可用的核。
     */
    private String cpuCores = "1";

    /**
     * 每个executor分配的内存数，默认1g，会受到yarn CDH的限制，和memoryOverhead相加 不能超过总内存限制。
     */
    private String memory = "1G";

    /**
     * executor和driver心跳发送间隔，默认10s，必须远远小于spark.network.timeout
     */
    private String heartbeatInterval;

    public String getCpuCores() {
        return cpuCores;
    }

    public void setCpuCores(String cpuCores) {
        this.cpuCores = cpuCores;
    }

    public String getMemory() {
        return memory;
    }

    public void setMemory(String memory) {
        this.memory = memory;
    }

    public String getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(String heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }
}
