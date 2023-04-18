package com.example.cdh.properties.spark;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author chunyang.leng
 * @date 2023-04-12 13:21
 */
@ConfigurationProperties("spark.driver")
@Configuration
public class SparkDriverProperties {
    /**
     * driver端分配的核数，默认为1
     * <br/>
     * thriftserver是启动thriftserver服务的机器，资源充足的话可以尽量给多。
     */
    private String cpuCores = "1";

    /**
     * driver端分配的内存数，默认为1GB，
     */
    private String memory = "1G";

    /**
     * driver端接收的最大结果大小，默认1GB，最小1MB，设置0为无限。
     * <br/>
     * 这个参数不建议设置的太大，如果要做数据可视化，更应该控制在20-30MB以内。过大会导致OOM。
     */
    private String maxResultSize;

    /**
     * driver端的ip
     * spark 会通过此地址于driver进行通讯，也就是说，如果这个地址不能被spark访问的话，调度是会失败的
     */
    private String host;

    /**
     * driver端端口。
     */
    private String port;

    /**
     * 其他的类加载路径
     * <br>
     * 在多个 Jar 包之间的分隔符默认是冒号 :
     */
    private String extraClassPath;

    private BlockManager blockManager;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }


    public BlockManager getBlockManager() {
        return blockManager;
    }

    public void setBlockManager(BlockManager blockManager) {
        this.blockManager = blockManager;
    }

    static class BlockManager{
        /**
         * driver端绑定监听block manager的端口。
         */
        private String port;
        /**
         * driver端绑定监听block manager的地址
         */
        private String address;

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }
    }

    public String getCpuCores() {
        return cpuCores;
    }

    public void setCpuCores(String cpuCores) {
        this.cpuCores = cpuCores;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getMemory() {
        return memory;
    }

    public void setMemory(String memory) {
        this.memory = memory;
    }

    public String getMaxResultSize() {
        return maxResultSize;
    }

    public void setMaxResultSize(String maxResultSize) {
        this.maxResultSize = maxResultSize;
    }

    public String getExtraClassPath() {
        return extraClassPath;
    }

    public void setExtraClassPath(String extraClassPath) {
        this.extraClassPath = extraClassPath;
    }
}
