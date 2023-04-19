package com.example.cdh.properties.spark;

import java.util.Set;
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
     * 设置每个 block 的大小（单位：毫秒），默认为 200 毫秒。
     */
    private String blockInterval;
    /**
     * 设置是否自动删除未使用的元数据。
     */
    private String unpersist;

    /**
     * 设置流式块生成线程池的大小。这个参数决定了生成新块的速度。
     */
    private String blockGeneratorThreadPoolSize;

    /**
     * 确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
     */
    private String stopGracefullyOnShutdown;
    /**
     * 实时计算的时间间隔,表示间隔的时间（单位：秒）
     *
     * 假如这个时间间隔被设置为 5 秒，也就是说，每 5 秒会生成一个新的 RDD，Spark Streaming 将会对其进行处理。
     * 这个时间间隔越短，实时性就越高，但是也可能增加计算负担和网络传输的压力；反之，越长则实时性越低，但是计算和网络传输的压力也相应减小。
     * 需要注意的是，时间间隔的选择需要根据具体的业务场景进行权衡。
     * 如果是对实时性要求比较高的场景，则可以适当缩短这个时间间隔；
     * 如果是对实时性要求不是很高的场景，则可以适当增加这个时间间隔。
     * 同时，还需要考虑到集群的计算资源、数据的实时性等因素，综合选择最优的时间间隔。
     */
    private String duration;

    private BackPressure backpressure;

    private Receiver receiver;

    private Kafka kafka;

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

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
         * 启用流式处理的反压机制，可以避免较早加入处理队列的数据被较晚加入的数据“淹没”。
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

    public static class Kafka{
        /**
         * 默认直接读取所有
         *
         * 限制每秒每个消费线程读取每个kafka分区最大的数据量
         */
        private String maxRatePerPartition;
        /**
         * 设置从 Kafka topic 中读取流式数据时的最大重试次数。
         */
        private String maxRetries;

        /**
         * 自定义的参数，非官方配置，配置 消费者 topic
         */
        private Set<String> topics;

        public String getMaxRatePerPartition() {
            return maxRatePerPartition;
        }

        public void setMaxRatePerPartition(String maxRatePerPartition) {
            this.maxRatePerPartition = maxRatePerPartition;
        }

        public String getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(String maxRetries) {
            this.maxRetries = maxRetries;
        }

        public Set<String> getTopics() {
            return topics;
        }

        public void setTopics(Set<String> topics) {
            this.topics = topics;
        }
    }

    static class Receiver{
        /**
         * 设置每秒接收的最大数据速率。可以被进一步细分为每个接收器（receiver）的速率限制。
         */
        private String maxRate;

        private WriteAheadLog writeAheadLog;

        public WriteAheadLog getWriteAheadLog() {
            return writeAheadLog;
        }

        public void setWriteAheadLog(WriteAheadLog writeAheadLog) {
            this.writeAheadLog = writeAheadLog;
        }

        public String getMaxRate() {
            return maxRate;
        }

        public void setMaxRate(String maxRate) {
            this.maxRate = maxRate;
        }

        static class WriteAheadLog{
            /**
             * 启用 receiver 对日志的操作进行预写（write-ahead）。
             */
            private String enable;

            public String getEnable() {
                return enable;
            }

            public void setEnable(String enable) {
                this.enable = enable;
            }
        }
    }

    static class FileStream {
        /**
         * 设置流式处理文件（FileStream）的最小记忆时间（即文件在数据流中的保留时间）。
         */
        private String minRememberDuration;

        public String getMinRememberDuration() {
            return minRememberDuration;
        }

        public void setMinRememberDuration(String minRememberDuration) {
            this.minRememberDuration = minRememberDuration;
        }
    }
    public String getBlockInterval() {
        return blockInterval;
    }

    public void setBlockInterval(String blockInterval) {
        this.blockInterval = blockInterval;
    }

    public String getUnpersist() {
        return unpersist;
    }

    public void setUnpersist(String unpersist) {
        this.unpersist = unpersist;
    }

    public Receiver getReceiver() {
        return receiver;
    }

    public void setReceiver(Receiver receiver) {
        this.receiver = receiver;
    }

    public String getBlockGeneratorThreadPoolSize() {
        return blockGeneratorThreadPoolSize;
    }

    public void setBlockGeneratorThreadPoolSize(String blockGeneratorThreadPoolSize) {
        this.blockGeneratorThreadPoolSize = blockGeneratorThreadPoolSize;
    }

    public Kafka getKafka() {
        return kafka;
    }

    public void setKafka(Kafka kafka) {
        this.kafka = kafka;
    }
}
