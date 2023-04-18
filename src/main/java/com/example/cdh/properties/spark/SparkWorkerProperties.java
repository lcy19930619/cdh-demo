package com.example.cdh.properties.spark;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author chunyang.leng
 * @date 2023-04-12 13:21
 */
@ConfigurationProperties("spark.worker")
@Configuration
public class SparkWorkerProperties {
    /**
     * worker端分配的核数，默认为1
     * <br/>
     * thriftserver是启动thriftserver服务的机器，资源充足的话可以尽量给多。
     */
    private String cpuCores = "1";

    /**
     * worker端分配的内存数，默认为1GB，
     */
    private String memory = "1G";

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
}
