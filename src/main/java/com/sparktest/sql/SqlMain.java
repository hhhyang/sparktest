
package com.sparktest.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class SqlMain {
    public static void main(String[] args) {


        //System.out.println(System.getProperty("user.dir"));

        SparkConf sparkConf = new SparkConf().setAppName("es-hadoop");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        /*create SQLContext, based on JavaSparkContext*/
        SQLContext sqlContext = new SQLContext(jsc);

        /*从json文件中读入数据集, json表示数据为json格式*/
        DataFrame df = sqlContext.read().json("people.json");

        JavaRDD<String> r = df.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return row.mkString(";");
            }
        });

        System.out.println(r.collect().toString());

        /*将数据打印到标准输出*/
        //df.show();

        /*打印表格式*/
        //df.printSchema();

        //df.select("name").show();

        //df.select(df.col("name"), df.col("age").plus(1)).show();

        //df.filter(df.col("age").gt(21)).show();

        //df.groupBy("age").count().show();

        df.registerTempTable("people");

        DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

        System.out.println("OK!");
    }
}
