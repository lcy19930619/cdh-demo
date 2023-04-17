package com.example.cdh.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author chunyang.leng
 * @date 2023-04-12 13:28
 */
@ConfigurationProperties("spark.default")
@Configuration
public class SparkDefaultProperties {
    /**
     * 默认RDD的分区数、并行数。
     * 像reduceByKey和join等这种需要分布式shuffle的操作中，最大父RDD的分区数；像parallelize之类没有父RDD的操作，则取决于运行环境下得cluster manager：
     * 如果为单机模式，本机核数；集群模式为所有executor总核数与2中最大的一个。
     */
    private Integer parallelism;

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }
}
