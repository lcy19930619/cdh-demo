package com.example.cdh.service.spark;

import java.io.Serializable;
import javax.annotation.PostConstruct;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

/**
 * @author chunyang.leng
 * @date 2023-04-19 09:52
 */
@Component
public class SparkStreamService implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SparkStreamService.class);
    @Autowired
    private JavaStreamingContext javaStreamingContext;
    @Autowired
    private JavaPairDStream<String,String> javaPairDStream;




    public void start(){
        new Thread(new Runnable() {
            @Override public void run() {
                logger.info("spark-streaming-kafka-consumer 已启动");
                try {
                    javaPairDStream.saveAsHadoopFiles("hdfs://cdh-slave-1:8020/spark-streaming","suffix");

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
                    javaStreamingContext.start();
                    javaStreamingContext.awaitTermination();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();


    }
}
