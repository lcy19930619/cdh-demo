package com.example.cdh.spark;

import com.example.cdh.service.spark.SparkStreamService;
import java.util.ArrayList;
import java.util.List;
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

    @Test
    public void mockPublisherTest() throws InterruptedException {
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < 1; i++) {
            list.add(UUID.randomUUID().toString());
        }
        sparkStreamService.mockProduce("test",list,30d);
        Thread.sleep(2000);
    }
}
