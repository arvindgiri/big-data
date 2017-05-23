import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Reference : https://www.tutorialspoint.com/apache_kafka/apache_kafka_integration_spark.htm
  * Kafka Command: kafka-console-producer.sh --broker-list localhost:9092 --sync --topic testTopic
  *
  */
object KafkaStreaming {

  @inline final val Brokers = "localhost:9092"
  @inline final val SparkMaster = "local"
  @inline final val ZookeeperQuorum = "localhost:2181"


  @inline final val ConsumerGroup = "cg_123"
  @inline final val TopicName = "testTopic"
  //    val sparkMaster = "spark://arvind-ubuntu:7077"
  @inline final val AppName = "ArvindKafkaStreamingExample"


  def main(args: Array[String]): Unit = {
    print("Kafka Streaming program started ....)")
    val sparkConf = new SparkConf().setAppName(AppName).setMaster(SparkMaster)
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))
    val topicMap = Map(TopicName -> 4);
    var kafkaStream = KafkaUtils.createStream(streamingContext, ZookeeperQuorum, ConsumerGroup, topicMap, StorageLevel.MEMORY_AND_DISK)
    kafkaStream.print();

    streamingContext.start()
    streamingContext.awaitTermination()
    print("hello" + scala.math.Pi)

    // KafkaUtils.createDirectStream(streamingContext)

    //TODO https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/streaming/JavaDirectKafkaWordCount.java

  }
}
