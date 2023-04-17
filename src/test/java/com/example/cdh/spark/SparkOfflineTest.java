package com.example.cdh.spark;

import com.example.cdh.service.SparkOfflineService;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.Assert;

/**
 * @author chunyang.leng
 * @date 2023-04-12 14:55
 */
@SpringBootTest
public class SparkOfflineTest {
    @Autowired
    private SparkOfflineService sparkOfflineService;
    String path = "hdfs://cdh-slave-1:8020/demo/csv/input.csv";


    @Test
    public void countHdfsCsvTest(){
        long count = sparkOfflineService.countHdfsCsv(path);
        System.out.println("====>" + count);
        Assert.isTrue(count>1,"查询的结果应该大于1");
    }

    @Test
    public void filterHdfsCsvLteAgeTest() throws AnalysisException {
        long l = sparkOfflineService.filterHdfsCsvLteAge(path, 18);
        System.out.println(l);
    }
}
