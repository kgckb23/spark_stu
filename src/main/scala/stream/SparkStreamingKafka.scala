package stream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("sparkStreamingKafka")
    val streamingContext = new StreamingContext(conf, Seconds(5))

    streamingContext.checkpoint("checkpoint")


    val kafkaParams = Map(
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "kb135:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.GROUP_ID_CONFIG -> "sparkStreamGroup1")
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]]
    = KafkaUtils.createDirectStream(
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Set("sparkstreamstu"), kafkaParams)
    )

    //    val dsStream = kafkaStream
    //      .map(x => (x.topic(), x.offset(), x.partition(), x.value()))
    //
    //    dsStream.print

    val wordCountStateStream = kafkaStream.flatMap(x => {
        x.value().toString.split("\\s+")
      })
      .map(x => (x, 1))
      //      .reduceByKey(_ + _)
      //    wordCountStream.print()
      .updateStateByKey(
        (seq: Seq[Int], buffer: Option[Int]) => {
          println("进入到updateStateByKey函数中")
          println("seq value" + seq.toList.toString())
          println("buffer value" + buffer.getOrElse(0).toString)
          val sum = buffer.getOrElse(0) + seq.sum
          Option(sum)
        }
      )

    wordCountStateStream.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
