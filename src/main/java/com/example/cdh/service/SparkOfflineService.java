package com.example.cdh.service;

import com.example.cdh.dto.UserDTO;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Serializable;

/**
 * spark 离线计算
 *
 * @author chunyang.leng
 * @date 2023-04-12 14:53
 */
@Component
public class SparkOfflineService implements Serializable {
    @Autowired
    private SparkSession sparkSession;

    /**
     * 统计 hdfs 中一个csv文件的行数
     *
     * @param hdfsPath demo: hdfs://cdh-slave-1:8020/demo/csv/input.csv
     * @return
     */
    public long countHdfsCsv(String hdfsPath) {
        return sparkSession.read().csv(hdfsPath).count();
    }

    public long filterHdfsCsvLteAge(String hdfsPath, int age) throws AnalysisException {
        // 临时表名称
        String tempTableName = "cdh_demo_input_csv";

        // 定义数据类型
        StructField nameField = DataTypes.createStructField("name", DataTypes.StringType, true);
        StructField ageField = DataTypes.createStructField("age", DataTypes.IntegerType, true);

        StructField[] fields = new StructField[2];
        fields[0] = nameField;
        fields[1] = ageField;
        StructType schema = new StructType(fields);

        Dataset<UserDTO> csv = sparkSession
            .read()
            .schema(schema)
            .csv(hdfsPath)
            .map(row -> {
                UserDTO dto = new UserDTO();
                dto.setName(row.getString(0));
                dto.setAge(row.getInt(1));
                return dto;
            }, Encoders.bean(UserDTO.class));

        csv.createOrReplaceTempView(tempTableName);

        Dataset<UserDTO> sql = sparkSession
            .sql("select * from " + tempTableName + " where age <= " + age)
            .as(Encoders.bean(UserDTO.class));
        JavaRDD<UserDTO> rdd = sql.toJavaRDD();
        rdd.foreach(new VoidFunction<UserDTO>() {
            @Override
            public void call(UserDTO dto) throws Exception {
                System.out.println(dto.getName() + " <====> " +dto.getAge());
            }
        });
        return csv.count();
    }
}
