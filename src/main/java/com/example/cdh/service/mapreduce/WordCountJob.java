package com.example.cdh.service.mapreduce;

import com.example.cdh.service.mapreduce.wordcount.WordCountDriver;
import java.io.IOException;
import org.springframework.stereotype.Component;

/**
 * @author chunyang.leng
 * @date 2023-04-17 14:45
 */
@Component
public class WordCountJob {

    /**
     * 运行 job 计算
     * @param input
     * @param output
     */
    public void runJob(String input, String output) throws IOException, InterruptedException, ClassNotFoundException {
        new WordCountDriver(input, output).run();
    }
}
