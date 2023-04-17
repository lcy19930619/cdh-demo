package com.example.cdh;

import com.example.cdh.service.HdfsService;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.DigestUtils;

/**
 * @author chunyang.leng
 * @date 2023-04-17 11:26
 */
@SpringBootTest
public class HdfsServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(HdfsServiceTest.class);
    String fileContent = UUID.randomUUID().toString();

    @Autowired
    private HdfsService hdfsService;

    @Test
    public void hdfsTest() throws IOException {
        File testFile = new File("./test", "hdfs-test.txt");
        FileUtils.writeStringToFile(testFile,fileContent,"utf-8");
        logger.info("生成测试文件完毕");
        byte[] before = FileUtils.readFileToByteArray(testFile);

        String testPath = "/test/" +UUID.randomUUID().toString();
        hdfsService.delete(testPath,true);
        logger.info("清理测试目录:{}",testPath);

        String hdfsFilePath = testPath +"/test.txt";
        hdfsService.uploadFile(before,hdfsFilePath,true);
        logger.info("上传流程测试完毕");

        List<Path> paths = hdfsService.listFiles(testPath, true);
        Assert.isTrue(!CollectionUtils.isEmpty(paths),"测试目录不应该为空");
        logger.info("遍历流程测试完毕");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        hdfsService.download(hdfsFilePath,outputStream);
        byte[] after = outputStream.toByteArray();

        String beforeMd5 = DigestUtils.md5DigestAsHex(before);
        String afterMd5 = DigestUtils.md5DigestAsHex(after);

        Assert.isTrue(beforeMd5.equals(afterMd5),"上传与下载的文件内容应该一致");
        logger.info("下载流程测试完毕");

        testFile.delete();
        hdfsService.delete(testPath,true);
        logger.info("测试环境清理完毕");
    }

}
