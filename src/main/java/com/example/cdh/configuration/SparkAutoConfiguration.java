package com.example.cdh.configuration;

import com.example.cdh.properties.SparkBroadcastProperties;
import com.example.cdh.properties.SparkDefaultProperties;
import com.example.cdh.properties.SparkDriverProperties;
import com.example.cdh.properties.SparkExecutorProperties;
import com.example.cdh.properties.SparkFilesProperties;
import com.example.cdh.properties.SparkMemoryProperties;
import com.example.cdh.properties.SparkProperties;
import com.example.cdh.properties.SparkReducerProperties;
import com.example.cdh.properties.SparkRpcProperties;
import com.example.cdh.properties.SparkSchedulingProperties;
import com.example.cdh.properties.SparkShuffleProperties;
import com.example.cdh.properties.SparkStoreProperties;
import com.example.cdh.properties.SparkStreamingProperties;
import com.example.cdh.properties.SparkWorkerProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * @author lcy
 */
@Configuration
public class SparkAutoConfiguration {

    @Autowired
    private SparkProperties sparkProperties;
    @Autowired
    private SparkBroadcastProperties sparkBroadcastProperties;
    @Autowired
    private SparkDefaultProperties sparkDefaultProperties;
    @Autowired
    private SparkDriverProperties sparkDriverProperties;
    @Autowired
    private SparkExecutorProperties sparkExecutorProperties;
    @Autowired
    private SparkFilesProperties sparkFilesProperties;
    @Autowired
    private SparkMemoryProperties sparkMemoryProperties;
    @Autowired
    private SparkReducerProperties sparkReducerProperties;
    @Autowired
    private SparkRpcProperties sparkRpcProperties;
    @Autowired
    private SparkSchedulingProperties schedulingProperties;
    @Autowired
    private SparkShuffleProperties sparkShuffleProperties;
    @Autowired
    private SparkStoreProperties sparkStoreProperties;
    @Autowired
    private SparkStreamingProperties sparkStreamingProperties;
    @Autowired
    private SparkWorkerProperties sparkWorkerProperties;

    /**
     * spark 的基本配置
     *
     * @return 把 yml 里配置的内容都写入该配置项
     */
    @Bean
    public SparkConf sparkConf() {


        return new SparkConf()
            .setAppName(sparkProperties.getAppName())
            .setMaster(sparkProperties.getMasterUrL())
            // spark 会通过此地址于driver进行通讯，也就是说，如果这个地址不能被spark访问的话，调度是会失败的
            // host 就是你本机的ip地址
            .set("spark.driver.host", "10.8.0.5")
            .set("spark.driver.memory", sparkDriverProperties.getMemory())
            .set("spark.worker.memory", sparkWorkerProperties.getMemory())
            .set("spark.executor.memory", sparkExecutorProperties.getMemory())
            ;
    }

    /**
     * 连接 spark 集群
     *
     * @param sparkConf
     * @return
     */
    @Bean
    @ConditionalOnMissingBean(JavaSparkContext.class)
    public JavaSparkContext javaSparkContext(SparkConf sparkConf) {
        return new JavaSparkContext(sparkConf);
    }

    /**
     *
     * @param javaSparkContext
     * @return
     */
    @Bean
    public SparkSession sparkSession(JavaSparkContext javaSparkContext) {
        return SparkSession
            .builder()
            .sparkContext(javaSparkContext.sc())
            .appName(sparkProperties.getAppName())
            .getOrCreate();
    }


}
