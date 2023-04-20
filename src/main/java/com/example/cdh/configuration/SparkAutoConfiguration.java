package com.example.cdh.configuration;

import com.example.cdh.properties.spark.SparkProperties;
import com.example.cdh.properties.spark.SparkStreamingProperties;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import scala.Tuple2;

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
    @Scope("prototype")
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
     //   也可以通过此方式设置 (方式二)   二选一即可
     //   conf.set("spark.driver.extraClassPath","hdfs://cdh-slave-1:8020/jars/mysql-connector-java-5.1.47.jar");
     //   也可以通过此方式设置 (方式三)    二选一即可
     //   conf.set("spark.executor.extraClassPath","hdfs://cdh-slave-1:8020/jars/mysql-connector-java-5.1.47.jar");
        return conf;
    }

    /**
     * 连接 spark 集群
     *
     * @param sparkConf
     * @return
     */
    @Bean("javaSparkContext")
    @ConditionalOnMissingClass("org.apache.spark.streaming.api.java.JavaStreamingContext")
    public JavaSparkContext javaSparkContext(SparkConf sparkConf) {
        return new JavaSparkContext(sparkConf);
    }

    /**
     * 如果有实时流测试环境，使用实时流上下文
     * @param javaStreamingContext
     * @return
     */
    @Bean("javaSparkContext")
    public JavaSparkContext javaStreamingContext(JavaStreamingContext javaStreamingContext){
        return javaStreamingContext.sparkContext();
    }

    /**
     *
     * @param javaSparkContext
     * @return
     */
    @Bean
    public SparkSession sparkSession(@Qualifier("javaSparkContext") JavaSparkContext javaSparkContext) {
        return SparkSession
            .builder()
            .sparkContext(javaSparkContext.sc())
            .appName(sparkProperties.getAppName())
            .getOrCreate();
    }

    
    @Bean
    public JavaStreamingContext javaStreamingContext(SparkConf sparkConf,
        SparkStreamingProperties streamingProperties){
        String duration = streamingProperties.getDuration();
        if (StringUtils.isBlank(duration) || !StringUtils.isNumeric(duration)){
            duration = "5";
            logger.warn("spark.streaming.duration 配置参数不符合要求，已使用默认值5，当前配置参数:{}",duration);
        }
        return new JavaStreamingContext(sparkConf, Durations.seconds(Integer.parseInt(duration)));
    }

    @Bean
    public <Key,Value> JavaInputDStream<ConsumerRecord<Key,Value>> kafkaReceiverStream(
        JavaStreamingContext javaStreamingContext,
        KafkaProperties kafkaProperties,
        SparkStreamingProperties streamingProperties) {
        Map<String, Object> kafkaParams = kafkaProperties.buildConsumerProperties();

        Object groupId = kafkaParams.get("group.id");
        if (Objects.isNull(groupId)) {
            kafkaParams.put("group.id", UUID.randomUUID().toString());
        }
        String topic = kafkaProperties.getTemplate().getDefaultTopic();

        Collection<String> topics = Collections.singletonList(topic);
        SparkStreamingProperties.Kafka kafka = streamingProperties.getKafka();
        if (Objects.nonNull(kafka) && kafka.getTopics() != null){
            topics = kafka.getTopics();
        }

        return KafkaUtils.<Key,Value>createDirectStream(
            javaStreamingContext,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.<Key,Value>Subscribe(topics, kafkaParams));
    }


    @Bean
    public <Key,Value> JavaPairDStream<Key,Value> javaDStream(JavaInputDStream<ConsumerRecord<Key,Value>> kafkaReceiverStream){
        return kafkaReceiverStream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
    }


}
