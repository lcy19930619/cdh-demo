package com.example.cdh.service;

import com.example.cdh.dto.UserDTO;
import java.io.Serializable;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.count;

/**
 * 使用 spark sql 离线计算
 *
 * @author chunyang.leng
 * @date 2023-04-12 14:53
 */
@Component
public class SparkOfflineService implements Serializable {
    private static final long serialVersionUID = 1L;

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

    /**
     * 小于等于 计算示例
     * <br/>
     * <pre>
     * {@code  select name, age from xx where age <=#{age} }
     * </pre>
     * @param hdfsPath 要计算的文件
     * @param age 阈值
     * @return 算出来的数据总量
     */
    public long lte(String hdfsPath, int age) {
        // 临时表名称
        String tempTableName = "cdh_demo_lte";
        // 加载 csv 数据
        Dataset<UserDTO> data = loadCsv(hdfsPath);

        // 创建临时表
        data.createOrReplaceTempView(tempTableName);
        // 执行 sql 语句
        Dataset<Row> sqlData = sparkSession
            .sql("select name,age from " + tempTableName + " where age <= " + age);

        // 存储数据
        saveToMySQL(tempTableName, sqlData);
        return sqlData.count();
    }

    /**
     * 简单的聚合查询示例
     * <br/>
     * <pre>
     * {@code
     * select
     *      count(name) as c,
     *      age
     * from
     *      xx
     * group by age
     *
     * having c > #{count}
     *
     * order by c desc
     * }
     * </pre>
     * @param hdfsPath 要统计的文件
     * @param count having > #{count}
     * @return
     */
    public long agg(String hdfsPath, int count){
        // 临时表名称
        String tempTableName = "cdh_demo_agg";
        // 加载 csv 数据
        Dataset<UserDTO> data = loadCsv(hdfsPath);

        // 创建临时表
        data.createOrReplaceTempView(tempTableName);
        // 执行 sql 语句
        Dataset<Row> sqlData = sparkSession
            .sql("select name,age from " + tempTableName)
            .groupBy(column("age").alias("age"))
            .agg(count("name").alias("c"))
            // filter = having
            .filter(column("c").gt(count))
            // 按照统计出来的数量，降序排序
            .orderBy(column("c").desc());

        saveToMySQL(tempTableName, sqlData);
        return sqlData.count();
    }

    /**
     * 加载 hdfs 中 csv 文件内容
     * @param hdfsPath
     * @return
     */
    private Dataset<UserDTO> loadCsv(String hdfsPath) {

        // 自定义数据类型，也可以使用数据类型自动推断
        StructField nameField = DataTypes.createStructField("name", DataTypes.StringType, true);
        StructField ageField = DataTypes.createStructField("age", DataTypes.IntegerType, true);

        StructField[] fields = new StructField[2];
        fields[0] = nameField;
        fields[1] = ageField;
        StructType schema = new StructType(fields);

        return sparkSession
                .read()
                .schema(schema)
                .csv(hdfsPath)
                .map(new MapFunction<Row, UserDTO>() {
                    @Override
                    public UserDTO call(Row row) throws Exception {
                        UserDTO dto = new UserDTO();
                        dto.setName(row.getString(0));
                        dto.setAge(row.getInt(1));
                        return dto;
                    }
                }, Encoders.bean(UserDTO.class));
    }

    /**
     * 数据存储到 mysql
     * @param tableName 表名字
     * @param dataset 数据
     */
    private void saveToMySQL(String tableName,Dataset<Row> dataset){
        dataset
            .write()
            // 覆盖模式，原始数据会被覆盖掉，如果需要追加，换成 SaveMode.Append
            .mode(SaveMode.Overwrite)
            .format("jdbc")
            .option("url", "jdbc:mysql://10.8.0.4/test")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("dbtable", tableName)
            .option("user", "root")
            .option("password", "q")
            .save();
    }
}
