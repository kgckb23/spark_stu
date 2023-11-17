package stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkstream")
    val streamingContext = new StreamingContext(conf, Seconds(3))

    val socketLineStream = streamingContext.socketTextStream("192.168.153.135", 7777)

    //    socketLineStream.print()
    val wordCountStream = socketLineStream.flatMap(x => x.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _)

    wordCountStream.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
