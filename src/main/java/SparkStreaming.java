import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.time.Duration;
import java.util.Arrays;

public class SparkStreaming {

  public static  void main(String argv[]) throws InterruptedException {

  // Create a local StreamingContext with two working thread and batch interval of 1 second
  SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
  JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

  // Create a DStream that will connect to hostname:port, like localhost:9999
  JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

  // Split each line into words
  JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

  // Count each word in each batch
  JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
  JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKeyAndWindow((i1, i2) -> i1 + i2,
      org.apache.spark.streaming.Durations.seconds(30), Durations.seconds(30));

  // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print();


  jssc.start();              // Start the computation
  jssc.awaitTermination();   // Wait for the computation to terminate

  }

}
