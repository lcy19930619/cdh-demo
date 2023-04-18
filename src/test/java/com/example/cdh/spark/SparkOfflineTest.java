package com.example.cdh.spark;

import com.example.cdh.service.HdfsService;
import com.example.cdh.service.SparkOfflineService;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
    @Autowired
    private HdfsService hdfsService;
    String path = "hdfs://cdh-slave-1:8020/demo/csv/input.csv";


    @Test
    public void countHdfsCsvTest() throws IOException {
        initHdfsData();
        long count = sparkOfflineService.countHdfsCsv(path);
        System.out.println("====>" + count);
        Assert.isTrue(count>1,"查询的结果应该大于1");
        cleanup();
    }

    @Test
    public void filterHdfsCsvLteAgeTest() throws AnalysisException, IOException {
        initHdfsData();
        long count = sparkOfflineService.filterHdfsCsvLteAge(path, 50);
        Assert.isTrue(count ==5,"查询的结果应该=5");
        cleanup();
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
