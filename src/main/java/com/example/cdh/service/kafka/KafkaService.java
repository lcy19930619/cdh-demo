package com.example.cdh.service.kafka;

import com.google.common.util.concurrent.RateLimiter;
import java.util.List;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * @author chunyang.leng
 * @date 2023-04-20 12:55
 */
@Component
public class KafkaService {
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;
    @Autowired
    private JavaStreamingContext javaStreamingContext;
    @Autowired
    private ThreadPoolTaskExecutor commonThreadPool;
    /**
     * 投递数据到kafka，模拟实时计算输入
     * @param topic topic
     * @param data 全部数据
     * @param qps qps 限制
     */
    public void mockProduce(String topic, List<String> data,double qps){
        commonThreadPool.execute(()->{
            RateLimiter limiter = RateLimiter.create(qps);
            for (String datum : data) {
                kafkaTemplate.send(topic,"key",datum);
                limiter.acquire();
            }
        });
    }
}
