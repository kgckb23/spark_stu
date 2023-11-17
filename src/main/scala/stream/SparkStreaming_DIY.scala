package stream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

import scala.util.Random

object SparkStreaming_DIY {
  /*def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("sparkStreamingdiy")
    val streamingContext = new StreamingContext(conf, Seconds(3))

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private val flg = true

    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flg) {
            val message = new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()
    }

    override def onStop(): Unit = {
      flg = false
    }
  }*/
}
