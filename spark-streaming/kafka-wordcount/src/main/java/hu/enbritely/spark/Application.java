package hu.enbritely.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;

public class Application {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[4]");
        conf.setAppName("Socket stream");
        conf.setSparkHome("/opt/spark-1.4.0-bin-hadoop2.6");
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(10));
        HashMap<String, Integer> topicmap = new HashMap<>();
        topicmap.put("bigdata", 5);
        JavaPairReceiverInputDStream<String, String> stream = KafkaUtils.createStream(context, "localhost:2181", "wordcount", topicmap);
        JavaDStream<String> words = stream.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return Arrays.asList(stringStringTuple2._2().split(" "));
            }
        });

        JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> counted = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer.intValue() + integer2.intValue();
            }
        });
        JavaPairDStream<String, Integer> filtered = counted.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> t) throws Exception {
                return t._2().intValue() > 2;
            }
        });

        filtered.print();
        context.start();
        context.awaitTermination();

    }
}
