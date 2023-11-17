package stream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkWindowDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkStreamingWindow")
    val streamingContext = new StreamingContext(conf, Seconds(3))

    streamingContext.checkpoint("checkpointWindow")

    val kafkaParams = Map(
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "kb135:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"),
      (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"),
      (ConsumerConfig.GROUP_ID_CONFIG -> "sparkStreamKafkaWindow")
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]]
    = KafkaUtils.createDirectStream(
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Set("sparkstreamstu"), kafkaParams)
    )

    /*val windowStream = kafkaStream.flatMap(x => x.value().split("\\s+"))
      .mapPartitions(x => for (y <- x) yield (y, 1))
      .reduceByKey(_+_)
      //.window(Seconds(9), Seconds(3))
      // .window(Seconds(9))  // 默认采集周期为滑动步长
      .window(Seconds(9),Seconds(9))
    windowStream.print()*/

    /*val value = kafkaStream.flatMap(x => x.value().split("\\s+"))
      .countByValueAndWindow(Seconds(9), Seconds(6))
    value.print()*/

    val windowStream = kafkaStream.flatMap(x => x.value().split("\\s+"))
      .mapPartitions(x => for (y <- x) yield (y, 1))
      //.reduceByKeyAndWindow((x, y) => x + y, Seconds(9))
      .reduceByWindow((x:(String,Int),y:(String,Int))=>("wordcount",x._2+y._2),Seconds(9),Seconds(6))
    windowStream.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
