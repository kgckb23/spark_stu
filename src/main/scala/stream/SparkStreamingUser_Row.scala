package stream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import java.util

object SparkStreamingUser_Row {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkStreamingUserRowKafka")
    val streamingContext = new StreamingContext(conf, Milliseconds(100))

    streamingContext.checkpoint("checkpoint")

    val kafkaParams = Map(
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "kb135:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"),
      (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"),
      (ConsumerConfig.GROUP_ID_CONFIG -> "sparkStreamUserRowGroup1")
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]]
    = KafkaUtils.createDirectStream(
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Set("user_friends_row"), kafkaParams)
    )

    /*kafkaStream.foreachRDD(
      rdd => {
        println("kafkaStream foreachRdd")
        rdd.foreachPartition(
          x => {
            val props = new util.HashMap[String, Object]()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.153.135:9092")
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.ACKS_CONFIG, "1")
            val producer = new KafkaProducer[String, String](props)
            x.foreach(
              y => {
                val str1 = y.value()
                val str2 = y.offset()
                var info = "  value:" + str1 + "  offset:" + str2
                println(info)
                val record = new ProducerRecord[String, String]("user_friends_row1020", info)
                producer.send(record)
              }
            )
          }
        )
      }
    )*/

    kafkaStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          x => {
            val props = new util.HashMap[String, Object]()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.153.135:9092")
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.ACKS_CONFIG, "1")
            val producer = new KafkaProducer[String, String](props)
            x.foreach(
              y => {
                // println(y.topic(),y.partition(),y.offset(),y.value())
                val splits = y.value().split(",")
                if (splits.length == 2) {
                  val userid = splits(0).trim
                  val friends = splits(1).split("\\s+")
                  for (friend <- friends) {
                    val info = userid + "," + friend
                    println(info)
                    val record = new ProducerRecord[String, String]("user_friend1020", info)
                    producer.send(record)
                  }
                }
              }
            )
          }
        )
      }
    )

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
