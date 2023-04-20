package com.example.cdh.spark;

import com.example.cdh.properties.spark.SparkStreamingProperties;
import com.example.cdh.service.kafka.KafkaService;
import com.example.cdh.service.spark.SparkStreamService;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author chunyang.leng
 * @date 2023-04-19 10:18
 */
@SpringBootTest
public class SparkStreamTest {

    @Autowired
    private SparkStreamService sparkStreamService;
    @Autowired
    private SparkStreamingProperties streamingProperties;
    @Autowired
    private KafkaService kafkaService;
    @Test
    public void mockPublisherTest() throws InterruptedException {

        Set<String> topics = streamingProperties.getKafka().getTopics();
        topics.parallelStream().forEach(topic->{
            List<String> list = new ArrayList<String>();
            for (int i = 0; i < 1000; i++) {
                list.add(UUID.randomUUID().toString());
            }
            kafkaService.mockProduce(topic,list,999999);
        });
        Thread.sleep(20000);
    }
}
