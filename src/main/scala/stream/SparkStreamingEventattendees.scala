package stream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

import java.util

object SparkStreamingEventattendees {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("sparkStreamingEventKafka")
    val streamingContext = new StreamingContext(conf, Milliseconds(100))

    streamingContext.checkpoint("checkpoint")

    val kafkaParams = Map(
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "kb135:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"),
      (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"),
      (ConsumerConfig.GROUP_ID_CONFIG -> "sparkStreamEventGroup1")
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]]
    = KafkaUtils.createDirectStream(
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Set("eventattendees_row"), kafkaParams)
    )

    //    val value = kafkaStream.map(_.value().toString).count()
    //    value.print()

    kafkaStream.foreachRDD(
      rdd => {
        println("kafkaStream foreachRdd")
        rdd.foreachPartition(
          x => { // x 可迭代的对象，RDD中每一个分区内的所有数据
            val props = new util.HashMap[String, Object]()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.153.135:9092")
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.ACKS_CONFIG, "1")
            val producer = new KafkaProducer[String, String](props)
            x.foreach(
              y => { // y  ConsumerRecord
                val str = y.value()
                val fields = str.split(",")
                val eventId = fields(0)
                for (i <- 1 to 4) {
                  var state = ""
                  if (i == 1) state = "yse" else if (i == 2) state = "maybe" else if (i == 3) state = "invited" else state = "no"
                  if (fields.length >= i + 1 && fields(i).trim.length > 0) {
                    val users = fields(i).split("\\s+")
                    for (user <- users) {
                      var info = eventId + "," + user + "," + state
                      println(info)
                      val record = new ProducerRecord[String, String]("eventattendees", info)
                      producer.send(record)
                    }
                  }
                }
              }
            )
          }
        )
      }
    )

    /*kafkaStream.foreachRDD(
      rdd => {
        println("kafkaStream foreachRdd")
        rdd.foreach(
          x => {
            val props = new util.HashMap[String, Object]()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.153.135:9092")
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.ACKS_CONFIG, "1")
            val producer = new KafkaProducer[String, String](props)

            val str = x.value() // 事件ID，yesUser，maybeUser,invitedUser,noUser
            val fields = str.split(",")
            val eventId = fields(0)
            for (i <- 1 to 4) {
              var state = ""
              if (i == 1) state = "yse" else if (i == 2) state = "maybe" else if (i == 3) state = "invited" else state = "no"
              if (fields.length >= i + 1 && fields(i).trim.length > 0) {
                val users = fields(i).split("\\s+")
                for (user <- users) {
                  var info = eventId + "," + user + "," + state
                  println(info)
                  val record = new ProducerRecord[String, String]("eventattendees", info)
                  producer.send(record)
                }
              }
            }
          }
        )
      }
    )*/

    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
