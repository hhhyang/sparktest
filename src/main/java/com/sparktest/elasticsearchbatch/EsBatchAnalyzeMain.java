
package com.sparktest.elasticsearchbatch;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

import com.sparktest.streamingnetflow.NetflowBean;

public class EsBatchAnalyzeMain {

    public static void main(String[] args) {

        String query_string = "{" +
                "\"query\": {" +
                "\"filtered\": {" +
                "\"query\": { \"match_all\": {} }," +
                "\"filter\": {" +
                "\"bool\": {" +
                "\"must\": [" +
                "{\"terms\": {\"host\": [\"192.168.239.180\", \"192.168.239.181\"] } }," +
                "{\"terms\": {\"input_snmp\": [25, 26, 49, 50] } }" +
                "]" +
                "}" +
                "}" +
                "}" +
                "}" +
                "}";

        SparkConf conf = new SparkConf();
        conf.setAppName("EsBatchAnalyzeMain");
        conf.set("es.nodes", "localhost");
        conf.set("es.port", "8200");
        conf.set("es.query", query_string);
        conf.set("es.resource.read", "es-index-name/");


        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> esRDD = JavaEsSpark.esJsonRDD(jsc).values();


        JavaPairRDD<String, Long> ipRDD = esRDD.flatMapToPair(new PairFlatMapFunction<String, String, Long>() {
            public Iterable<Tuple2<String, Long>> call(String s) throws Exception {
                ObjectMapper mapper = new ObjectMapper();
                NetflowBean netflowBean = mapper.readValue(s, NetflowBean.class);
                List<Tuple2<String, Long>> ipPairs = new ArrayList<Tuple2<String, Long>>();

                Integer in_bytes = netflowBean.getIn_bytes() / 2;
                ipPairs.add(new Tuple2<>(netflowBean.getIpv4_src_addr(), in_bytes.longValue()));
                ipPairs.add(new Tuple2<>(netflowBean.getIpv4_dst_addr(), in_bytes.longValue()));
                return ipPairs;
            }
        });


        JavaPairRDD<String, Long> reducedRDD = ipRDD.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        }, 36);

        reducedRDD.saveAsTextFile("hdfs://localhost:9000/result_file_name");

/*
        List<Tuple2<String, Long>> result = ipRDD.take(10);

        for (Tuple2<String, Long> s : result) {
            System.out.println(s._1() + ": " + s._2());
        }
*/
    }
}
