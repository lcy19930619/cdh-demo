package com.example.cdh.configuration;

import com.example.cdh.properties.HadoopProperties;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.context.annotation.Bean;

/**
 * @author chunyang.leng
 * @date 2023-04-17 10:40
 */
@org.springframework.context.annotation.Configuration
public class HadoopAutoConfiguration {

    @Bean
    public FileSystem fileSystem(
        HadoopProperties hadoopProperties) throws URISyntaxException, IOException, InterruptedException {
        // 获取连接集群的地址
        URI uri = new URI(hadoopProperties.getUrl());
        // 创建一个配置文件
        Configuration configuration = new Configuration();
        //设置配置文件中副本的数量
        configuration.set("dfs.replication", hadoopProperties.getReplication());
        //设置配置文件块大小
        configuration.set("dfs.blocksize", hadoopProperties.getBlockSize());
        //获取到了客户端对象
        return FileSystem.get(uri, configuration, hadoopProperties.getUser());
    }
}
