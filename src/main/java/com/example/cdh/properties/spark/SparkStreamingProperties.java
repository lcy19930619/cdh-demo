package com.example.cdh.properties.spark;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author chunyang.leng
 * @date 2023-04-12 14:13
 */
@ConfigurationProperties("spark.streaming")
@Configuration
public class SparkStreamingProperties {

    /**
     * 确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
     */
    private String stopGracefullyOnShutdown;

    private BackPressure backpressure;

    public String getStopGracefullyOnShutdown() {
        return stopGracefullyOnShutdown;
    }

    public void setStopGracefullyOnShutdown(String stopGracefullyOnShutdown) {
        this.stopGracefullyOnShutdown = stopGracefullyOnShutdown;
    }

    public BackPressure getBackpressure() {
        return backpressure;
    }

    public void setBackpressure(BackPressure backpressure) {
        this.backpressure = backpressure;
    }

    static class BackPressure {
        /**
         * 开启后spark自动根据系统负载选择最优消费速率
         */
        private String enabled;
        /**
         * 默认直接读取所有
         * <p>
         * 在开启反压的情况下，限制第一次批处理应该消费的数据，因为程序冷启动队列里面有大量积压，防止第一次全部读取，造成系统阻塞
         */
        private String initialRate;

        public String getEnabled() {
            return enabled;
        }

        public void setEnabled(String enabled) {
            this.enabled = enabled;
        }

        public String getInitialRate() {
            return initialRate;
        }

        public void setInitialRate(String initialRate) {
            this.initialRate = initialRate;
        }
    }

    static class Kafka{
        /**
         * 默认直接读取所有
         *
         * 限制每秒每个消费线程读取每个kafka分区最大的数据量
         */
        private String maxRatePerPartition;

        public String getMaxRatePerPartition() {
            return maxRatePerPartition;
        }

        public void setMaxRatePerPartition(String maxRatePerPartition) {
            this.maxRatePerPartition = maxRatePerPartition;
        }
    }
}
