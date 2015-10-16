package hu.gulyasm.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class Wordcount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Wordcount").setMaster("local[2]").setSparkHome("/opt/spark-1.4.0-bin-hadoop2.6");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> text = context.textFile("egri-csillagok");
        JavaRDD<String> words = text.flatMap(new FlatMapFunction<String, String>() {

            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaRDD<String> filtered = words.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return !s.trim().matches("");
            }
        });
        JavaRDD<String> cleaned = filtered.map(new Function<String, String>() {
            public String call(String s) throws Exception {
                return s.toLowerCase().replace("\\p{P}|\\d", "");
            }
        });
        JavaPairRDD<String, Integer> ones = cleaned.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counted = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer l, Integer r) throws Exception {
                return l.intValue() + r.intValue();
            }
        });

        JavaPairRDD<String, Integer> result = counted.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            public Boolean call(Tuple2<String, Integer> tuple) throws Exception {
                return tuple._2().intValue() > 10;
            }
        });

        result.saveAsTextFile("result-" + System.currentTimeMillis());
        context.stop();

    }

}
