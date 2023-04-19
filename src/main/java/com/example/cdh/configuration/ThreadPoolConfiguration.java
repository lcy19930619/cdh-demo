package com.example.cdh.configuration;

import java.util.concurrent.ThreadPoolExecutor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * 线程池配置
 * @author chunyang.leng
 * @date 2023-04-19 09:54
 */
@Configuration
public class ThreadPoolConfiguration {


    @Bean
    public ThreadPoolTaskExecutor commonThreadPool(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        //获取到服务器的cpu内核
        int i = Runtime.getRuntime().availableProcessors();
        //核心池大小
        executor.setCorePoolSize(i);
        //最大线程数
        executor.setMaxPoolSize(i * 2);
        //队列长度
        executor.setQueueCapacity(60000);
        //线程空闲时间
        executor.setKeepAliveSeconds(1000);
        //线程前缀名称
        executor.setThreadNamePrefix("common-pool-");
        //配置拒绝策略
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        return executor;
    }
}
