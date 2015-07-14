package hu.enbritely.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class Application {

    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("Wordcount");
        conf.setSparkHome("/opt/spark-1.4.0-bin-hadoop2.6/");
        conf.setMaster("local[2]");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> text = context.textFile("/home/gulyasm/panopticon/other/spark/data/war_peace_text");
        JavaRDD<String> words = text.flatMap(s -> Arrays.asList(s.split(" ")));
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counted = ones.reduceByKey((s, r) -> s + r);
        counted.saveAsTextFile("result");
    }
}
