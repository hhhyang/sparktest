
package com.sparktest.wordcount;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class WordCountMain {

    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("usage: ");
        }

        String inputFile = args[0];
        String outputFile = args[1];

        SparkConf conf = new SparkConf().setAppName("wordCount 2");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> input = jsc.textFile(inputFile, 3);

        System.out.println("partition: " + input.getNumPartitions());

        List<Partition> ps = input.partitions();
        for (Partition p : ps) {
            System.out.println("partition--: " + p.index());
        }


        //input.cache();

        input.persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //words.repartition(2);
        words.coalesce(2);
        System.out.println("partitions: " + words.getNumPartitions());

        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }, 2);

        System.out.println("partitions: " + counts.getNumPartitions());

        /*
        try {
            TimeUnit.SECONDS.sleep(20);
        }
        catch (Exception e) {

        }
        */


        // counts.cache();

        counts.saveAsTextFile(outputFile);

        /*
        Map<String, Integer> result = counts.collectAsMap();
        int i = 0;
        for (Map.Entry<String, Integer> e : result.entrySet()) {
            System.out.println("word: " + e.getKey() + " count: " + e.getValue());
            i++;
            if (i >= 10) {
                break;
            }
        }
        */


    }


}

