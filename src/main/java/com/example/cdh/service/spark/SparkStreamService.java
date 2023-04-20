package com.example.cdh.service.spark;

import com.google.common.util.concurrent.RateLimiter;
import java.util.List;
import javax.annotation.PostConstruct;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * @author chunyang.leng
 * @date 2023-04-19 09:52
 */
@Component
public class SparkStreamService {
    private static final Logger logger = LoggerFactory.getLogger(SparkStreamService.class);
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;
    @Autowired
    private JavaStreamingContext javaStreamingContext;
    @Autowired
    private ThreadPoolTaskExecutor commonThreadPool;
    @Autowired
    private JavaPairDStream<String,String> javaPairDStream;
    @PostConstruct
    private void initialize()  {
        commonThreadPool.execute(()->{
            logger.info("spark-streaming-kafka-consumer 已启动");
            try {
                start();
                javaStreamingContext.start();
                javaStreamingContext.awaitTermination();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

    }
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
                kafkaTemplate.send(topic,datum);
                limiter.acquire();
            }
        });
    }


    public void start(){
        javaPairDStream.saveAsHadoopFiles("hdfs://cdh-slave-1:8020/spark-streaming","suffix");

         // 遍历
        javaPairDStream.foreachRDD(new VoidFunction2<JavaPairRDD<String, String>, Time>() {
            @Override public void call(JavaPairRDD<String, String> rdd, Time time) throws Exception {
                System.out.println("RDD count:  " + rdd.count());
                rdd.foreach(record -> {
                    System.out.println("Key: " + record._1());
                    System.out.println("Value: " + record._2());
                });
            }
        });
    }
}
