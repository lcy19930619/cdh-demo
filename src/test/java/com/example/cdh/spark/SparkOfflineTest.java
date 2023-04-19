package com.example.cdh.spark;

import com.example.cdh.service.hadoop.HdfsService;
import com.example.cdh.service.spark.SparkOfflineService;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.Assert;

/**
 * @author chunyang.leng
 * @date 2023-04-12 14:55
 */
@SpringBootTest
public class SparkOfflineTest {
    private static final Logger logger = LoggerFactory.getLogger(SparkOfflineTest.class);
    @Autowired
    private SparkOfflineService sparkOfflineService;
    @Autowired
    private HdfsService hdfsService;
    String path = "hdfs://cdh-slave-1:8020/demo/csv/input.csv";


    @Test
    public void countHdfsCsvTest() throws IOException {
        initHdfsData();
        long count = sparkOfflineService.countHdfsCsv(path);
        cleanup();
        Assert.isTrue(count == 6,"查询的结果应该为6");
        logger.info("统计测试执行完毕");
    }

    @Test
    public void lteTest() throws  IOException {
        initHdfsData();
        long count = sparkOfflineService.lte(path, 19);
        cleanup();
        Assert.isTrue(count == 3,"查询的结果应该为 3");
        logger.info("简单条件测试执行完毕");

    }

    @Test
    public void aggTest() throws IOException {
        initHdfsData();
        long count = sparkOfflineService.agg(path, 1);
        cleanup();
        Assert.isTrue(count == 1,"查询的结果应该为 1");
        logger.info("聚合测试执行完毕");
    }


    private void initHdfsData() throws IOException {
        String data =
            "name,age\n"+
            "n0,17\n" +
            "n1,18\n" +
            "n2,19\n" +
            "n3,20\n" +
            "n4,20\n";
        hdfsService.delete(path,true);
        hdfsService.uploadFile(data.getBytes(StandardCharsets.UTF_8),path,true);
    }

    private void cleanup() throws IOException {
        hdfsService.delete(path,true);
    }
}
