package com.sparktest.streamingnetflow;


import org.apache.spark.SparkConf;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;

import org.apache.spark.streaming.api.java.*;


import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.text.SimpleDateFormat;

import java.util.Map;
import java.util.HashMap;


public class NetflowMain {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.set("es.index.auto.create", "true");
        //sparkConf.set("es.resource", "spark/docs");
        sparkConf.set("es.nodes", "node1, node1");
        sparkConf.set("es.port", "8200");


        /*RDD的时间间隔为1秒*/
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        //JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(jssc);

        String ZK_QUORUM = "localhost:2181,localhost:2181,localhost:2181";
        String GROUP_ID = "realtime";
        String TOPICS = "consumed-topic-name";
        String PARTITIONS_PER_TOPIC = "4";

        /*读取的kafka topic, 以及处理每个topic的spark partition数*/
        int partitions = Integer.parseInt(PARTITIONS_PER_TOPIC);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = TOPICS.split(",");
        for (String topic : topics) {
            topicMap.put(topic, partitions);
        }

        JavaPairReceiverInputDStream<String, String> kafkaPairStream = KafkaUtils.createStream(jssc, ZK_QUORUM, GROUP_ID, topicMap);


        JavaDStream<String> kafkaStream = kafkaPairStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._2();
            }
        });

        JavaDStream<String> stream = kafkaStream.mapPartitions(new NetflowHandler());

        stream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                //SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd");
                //String indexName = "netflow_log_v3-st01-" + df.format(new Date()) + "/netflow_log_v3";
                //JavaEsSpark.saveJsonToEs(stringJavaRDD, indexName);
                String indexName = "consumed-topic-name-{@timestamp:YYYY.MM.dd}/index-type";
                JavaEsSpark.saveJsonToEs(stringJavaRDD, indexName);
            }
        });

        /*输出操作,打印前10个record*/
        kafkaStream.print();
        //kafkaStream.saveAsHadoopFiles("output", "txt");

        /*启动*/
        jssc.start();
        jssc.awaitTermination();

    }


}
