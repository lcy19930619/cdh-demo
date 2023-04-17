package com.example.cdh.service.mapreduce.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author chunyang.leng
 * @date 2023-04-17 13:26
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text outK = new Text();
    private final IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key,
        Text value,
        Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            outK.set(word);
            context.write(outK, outV);
        }
    }
}