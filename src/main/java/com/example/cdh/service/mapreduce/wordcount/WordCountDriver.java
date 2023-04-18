package com.example.cdh.service.mapreduce.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author chunyang.leng
 * @date 2023-04-17 13:27
 */
public class WordCountDriver {

    private final Job instance;

    public WordCountDriver(String inputPath, String outputPath) throws IOException {

        JobConf jobConf = new JobConf();
        // 设置要计算的文件读取路径
        jobConf.set(FileInputFormat.INPUT_DIR,inputPath);
        // 设置计算结果存储路径
        jobConf.set(FileOutputFormat.OUTDIR,outputPath);

        // 1.创建job实例
        instance = Job.getInstance(jobConf);
        // 2.设置jar
        instance.setJarByClass(WordCountDriver.class);
        // 3.设置Mapper和Reducer
        instance.setMapperClass(WordCountMapper.class);
        instance.setReducerClass(WordCountReducer.class);
        // 4.设置map输出的kv类型
        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(IntWritable.class);
        // 5.设置最终输出的kv类型
        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(IntWritable.class);
    }

    /**
     * 提交 job 运行
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public void run() throws IOException, InterruptedException, ClassNotFoundException {
        instance.waitForCompletion(true);
    }
}
