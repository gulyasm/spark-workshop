package hu.gulyasm.sensorflow;

import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;

public class Application {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[4]");
        conf.setAppName("Socket stream");
        conf.setSparkHome("/opt/spark-1.4.0-bin-hadoop2.6");
        final JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(10));
        HashMap<String, Integer> topicmap = new HashMap<>();
        topicmap.put("telekom-test", 10);
        JavaPairReceiverInputDStream<String, String> stream = KafkaUtils.createStream(context, "localhost:2181", "wordcount", topicmap);

        // Parse the JSON
        JavaPairDStream<String, SensorData> data = stream.mapValues(new Function<String, SensorData>() {

            @Override
            public SensorData call(String s) throws Exception {
                Gson gson = new Gson();
                return gson.fromJson(s, SensorData.class);
            }
        });

        // Clean the data
        JavaPairDStream<String, SensorData> filtered = data.filter(new Function<Tuple2<String, SensorData>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, SensorData> stringSensorDataTuple2) throws Exception {
                return stringSensorDataTuple2._2().value > 0;
            }
        });

        // Over threshold filter
        JavaPairDStream<String, SensorData> overThreshold = filtered.filter(new Function<Tuple2<String, SensorData>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, SensorData> stringSensorDataTuple2) throws Exception {
                return stringSensorDataTuple2._2().value > 50;
            }
        });



        // paired with SensorID
        JavaPairDStream<SensorPlacement, SensorData> paired = overThreshold.mapToPair(new PairFunction<Tuple2<String, SensorData>, SensorPlacement, SensorData>() {
            @Override
            public Tuple2<SensorPlacement, SensorData> call(Tuple2<String, SensorData> s) throws Exception {
                SensorPlacement sp = new SensorDB().getSensorPlacement(s._2().sensorID);
                return new Tuple2<>(sp, s._2());
            }
        });



        JavaPairDStream<SensorPlacement, Boolean> alert = paired.groupByKey().mapValues(new Function<Iterable<SensorData>, Boolean>() {
            @Override
            public Boolean call(Iterable<SensorData> sensorDatas) throws Exception {
                return Iterables.size(sensorDatas) > 2;
            }
        });

        alert.print();

        context.start();
        context.awaitTermination();
    }
}
