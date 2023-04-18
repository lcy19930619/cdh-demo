package com.example.cdh.properties.spark;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author chunyang.leng
 * @date 2023-04-12 14:02
 */
@ConfigurationProperties("spark.rpc")
@Configuration
public class SparkRpcProperties {

    private Message message;
    /**
     * rpc任务在放弃之前的重试次数，默认3，即rpc task最多会执行3次。
     */
    private String numRetries;
    /**
     * rpc任务超时时间，默认spark.network.timeout
     */
    private String askTimeout;

    /**
     * rpc任务查找时长
     */
    private String lookupTimeout;

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public String getNumRetries() {
        return numRetries;
    }

    public void setNumRetries(String numRetries) {
        this.numRetries = numRetries;
    }

    public String getAskTimeout() {
        return askTimeout;
    }

    public void setAskTimeout(String askTimeout) {
        this.askTimeout = askTimeout;
    }

    public String getLookupTimeout() {
        return lookupTimeout;
    }

    public void setLookupTimeout(String lookupTimeout) {
        this.lookupTimeout = lookupTimeout;
    }

    static class Message{
        /**
         * executors和driver间消息传输、map输出的大小，默认128M。map多可以考虑增加。
         */
        private String maxSize;

        public String getMaxSize() {
            return maxSize;
        }

        public void setMaxSize(String maxSize) {
            this.maxSize = maxSize;
        }
    }
}
