package stream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

import java.util

object SparkStreamingKafka_Wordcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkStreamingKafkaWordCount")
    val streamingContext = new StreamingContext(conf, Seconds(5))

    streamingContext.checkpoint("checkpoint")

    val kafkaParams = Map(
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "kb135:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"),
      (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"),
      (ConsumerConfig.GROUP_ID_CONFIG -> "sparkStreamKafkaWordCount1")
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]]
    = KafkaUtils.createDirectStream(
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Set("wordcount"), kafkaParams)
    )

    val resultStream = kafkaStream
      .flatMap(x => {
        x.value().split("\\s+")
      }).mapPartitions(x => for (word <- x) yield (word, 1)).reduceByKey(_ + _)

    resultStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          x => {
            val props = new util.HashMap[String, Object]()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.153.135:9092")
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.ACKS_CONFIG, "1")
            val producer = new KafkaProducer[String, String](props)
            for (y <- x) {
              var info = y._1 + "," + y._2
              println(info)
              val record = new ProducerRecord[String, String]("wordcountout", info)
              producer.send(record)
            }
          }
        )
      }
    )

    /*kafkaStream.foreachRDD(
      rdd => {
        rdd.flatMap(x => {
            x.value().split("\\s+")
          }).map(x => (x, 1)).reduceByKey(_ + _)
          .foreachPartition(
            x => {
              val props = new util.HashMap[String, Object]()
              props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.153.135:9092")
              props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
              props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
              props.put(ProducerConfig.ACKS_CONFIG, "1")
              val producer = new KafkaProducer[String, String](props)
              x.foreach(
                y => {
                  val info = y._1 + "," + y._2
                  println(info)
                  val record = new ProducerRecord[String, String]("wordcountout", info)
                  producer.send(record)
                }
              )
            }
          )
      }
    )*/

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
