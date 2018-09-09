import com.google.gson.Gson;
import kafka.Kafka;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class WikiEditsStreamApp {

  public static void main(String[] argv) throws InterruptedException {

    // Create a local StreamingContext with two working thread and batch interval of 1 second
    SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("WikiEditsAnalyzer");
    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

    //kafka params
    Map<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("bootstrap.servers", "localhost:32775");

    //topic
    Set<String> topics = new HashSet<String>();
    topics.add("wikiedits");

    // Create a DStream that will connect to kafka
    JavaPairInputDStream<String, String> directKafkaStream =
        KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

    directKafkaStream.foreachRDD(rdd -> {
      System.out.println("--- New RDD with " + rdd.partitions().size()
          + " partitions and " + rdd.count() + " records");
      final AtomicInteger count = new AtomicInteger();
      rdd.filter(r -> r._2().startsWith("data")).mapValues(v ->
         (v.split(":", 2)[1])).
      foreachPartition(p -> {
        System.out.println("START partition # " + count.getAndIncrement());
        while (p.hasNext()) {
          Tuple2 t = p.next();
          System.out.println("Key" + t._1 + " Value: " + t._2);
        }
        System.out.println("END partition # ");
      });
    });

    jssc.start();
    jssc.awaitTermination();

  }

}
