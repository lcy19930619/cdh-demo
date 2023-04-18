package com.example.cdh.properties.spark;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author lcy
 */
@Configuration
@ConfigurationProperties(prefix = "spark")
public class SparkProperties {

    /**
     * app 名字
     */
    private String appName;
    /**
     * 主节点所在地址
     */
    private String masterUrL;

    /**
     * 如果有task执行的慢了，就会重新执行它。默认false，
     */
    private String speculation;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getMasterUrL() {
        return masterUrL;
    }

    public void setMasterUrL(String masterUrL) {
        this.masterUrL = masterUrL;
    }

    public String getSpeculation() {
        return speculation;
    }

    public void setSpeculation(String speculation) {
        this.speculation = speculation;
    }
}
