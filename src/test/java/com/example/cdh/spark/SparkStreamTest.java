package com.example.cdh.spark;

import com.example.cdh.properties.spark.SparkStreamingProperties;
import com.example.cdh.service.kafka.KafkaService;
import com.example.cdh.service.spark.SparkStreamService;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import scala.Tuple2;

/**
 * @author chunyang.leng
 * @date 2023-04-19 10:18
 */
@SpringBootTest
public class SparkStreamTest {
    private static final Logger logger = LoggerFactory.getLogger(SparkStreamTest.class);
    @Autowired
    private SparkStreamService sparkStreamService;
    @Autowired
    private SparkStreamingProperties streamingProperties;
    @Autowired
    private KafkaService kafkaService;
    @Autowired
    private JavaPairDStream<String,String> javaPairDStream;
    @Autowired
    private JavaStreamingContext javaStreamingContext;
    @Test
    public void mockPublisherTest() throws InterruptedException {
        Set<String> topics = streamingProperties.getKafka().getTopics();
        logger.info("开始模拟实时数据topic:{}",String.join(",",topics));
        topics.parallelStream().forEach(topic->{
            List<String> list = new ArrayList<String>();
            for (int i = 0; i < 1000; i++) {
                list.add(UUID.randomUUID().toString());
            }
            kafkaService.mockProduce(topic,list,999999);
        });
        logger.info("模拟实时数据结束");
        javaPairDStream.saveAsHadoopFiles("hdfs://cdh-slave-1:8020/spark-streaming","suffix");
        logger.info("注册处理逻辑 hdfs 完毕");

        // 遍历
        javaPairDStream.foreachRDD(new VoidFunction2<JavaPairRDD<String, String>, Time>() {
            @Override public void call(JavaPairRDD<String, String> rdd, Time time) throws Exception {
                System.out.println("RDD count:  " + rdd.count());
                rdd.foreach(new VoidFunction<Tuple2<String, String>>() {
                    @Override public void call(Tuple2<String, String> record) throws Exception {
                        System.out.println("Key: " + record._1());
                        System.out.println("Value: " + record._2());
                    }
                });
            }
        });
        logger.info("注册处理逻辑 for 完毕");
        javaStreamingContext.start();
        logger.info("开启流处理");
        javaStreamingContext.awaitTermination();
    }
}
