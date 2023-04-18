package com.example.cdh;

import com.example.cdh.service.HdfsService;
import com.example.cdh.service.mapreduce.WordCountJob;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author chunyang.leng
 * @date 2023-04-17 15:28
 */
@SpringBootTest
public class MapReduceTest {

    private static final Logger logger = LoggerFactory.getLogger(MapReduceTest.class);
    String context = "Spring Boot makes it easy to create stand-alone, production-grade Spring based Applications that you can \"just run\". " +
        "We take an opinionated view of the Spring platform and third-party libraries so you can get started with minimum fuss. Most Spring Boot applications need minimal Spring configuration. " +
        "If you’re looking for information about a specific version, or instructions about how to upgrade from an earlier release, check out the project release notes section on our wiki.";
    @Autowired
    private HdfsService hdfsService;
    @Autowired
    private WordCountJob wordCountJob;
    @Autowired
    private FileSystem fileSystem;

    @Test
    public void testMapReduce() throws Exception {
        String fileName = "mapreduce.txt";
        String path = "/test/" + UUID.randomUUID().toString();

        String inputHdfsFilePath = path + "/" + fileName;

        String outPutHdfsFile = path + "/result/";
        hdfsService.delete(inputHdfsFilePath, true);
        logger.info("测试环境数据清理完毕");

        hdfsService.uploadFile(context.getBytes(StandardCharsets.UTF_8), inputHdfsFilePath, true);
        logger.info("MapReduce 测试文本上传完毕,开始执行 word count job");

        wordCountJob.runJob("hdfs://cdh-slave-1:8020" + inputHdfsFilePath, "hdfs://cdh-slave-1:8020" + outPutHdfsFile);
        logger.info("MapReduce 测试job执行完毕");


        List<Path> paths = hdfsService.listFiles(outPutHdfsFile, true);
        for (Path resultPath : paths) {
            FileStatus status = fileSystem.getFileStatus(resultPath);
            if (status.isDirectory()){
                continue;
            }
            if (status.isFile() && !resultPath.getName().startsWith("_SUCCESS")){
                // 是文件，并且不是成功标识文件

                try (FSDataInputStream open = fileSystem.open(resultPath);
                     ByteArrayOutputStream outputStream = new ByteArrayOutputStream()){
                    IOUtils.copy(open, outputStream);
                    byte[] bytes = outputStream.toByteArray();
                    logger.info("任务执行完毕，获取结果:{}", new String(bytes, StandardCharsets.UTF_8));
                }

            }
        }

        hdfsService.delete(path, true);
        logger.info("测试结束，清理空间完毕");

    }
}
