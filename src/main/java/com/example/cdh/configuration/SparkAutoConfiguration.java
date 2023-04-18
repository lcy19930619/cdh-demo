package com.example.cdh.configuration;

import com.example.cdh.properties.spark.SparkProperties;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;

/**
 * @author lcy
 */
@Configuration
public class SparkAutoConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(SparkAutoConfiguration.class);
    @Autowired
    private SparkProperties sparkProperties;
    @Autowired
    private Environment env;
    /**
     * spark 的基本配置
     *
     * @return 把 yml 里配置的内容都写入该配置项
     */
    @Bean
    public SparkConf sparkConf() {
        List<String> jars = sparkProperties.getJars();
        String[] sparkJars = jars.toArray(new String[0]);
        SparkConf conf = new SparkConf()
            .setAppName(sparkProperties.getAppName())
            .setMaster(sparkProperties.getMasterUrL())
            .setJars(sparkJars);
        AbstractEnvironment abstractEnvironment = ((AbstractEnvironment) env);

        MutablePropertySources sources = abstractEnvironment.getPropertySources();
        for (PropertySource<?> source : sources) {
            if (source instanceof MapPropertySource) {
                Map<String, Object> propertyMap = ((MapPropertySource) source).getSource();
                for (Map.Entry<String, Object> entry : propertyMap.entrySet()) {
                    String key = entry.getKey();
                    if (key.startsWith("spark.")) {
                        if ("spark.jars".equals(key)){
                            continue;
                        }
                        String value = env.getProperty(key);
                        conf.set(key,value);
                        logger.info("已识别 spark 配置属性,{}:{}",key,value);
                    }
                }
            }
        }
        return conf;
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
