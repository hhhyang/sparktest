
package com.sparktest.elasticsearch;




import com.google.common.collect.ImmutableList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

public class SparkMain {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("es-hadoop").setMaster("local");
        sparkConf.set("es.index.auto.create", "true");
        sparkConf.set("es.resource", "spark/docs");
        sparkConf.set("es.nodes", "node1, node2");
        sparkConf.set("es.port", "8200");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        TripBean upcoming = new TripBean("OTP", "SFO");
        TripBean lastWeek = new TripBean("MUC", "OTP");
        JavaRDD<TripBean> javaRDD = jsc.parallelize(ImmutableList.of(upcoming, lastWeek));

        JavaEsSpark.saveToEs(javaRDD, "spark/docs2");


        System.out.println("OK!");
    }
}
