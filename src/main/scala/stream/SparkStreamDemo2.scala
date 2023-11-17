package stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamDemo2 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    //使用updateStateByKey必须要设置检查点目录
    ssc.checkpoint("checkpoint2")

    //3 获取一行数据
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.153.135", 7777)

    //4 切割数据
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    //5 转换数据结构
    val word2oneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    //6 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
    val result: DStream[(String, Int)] = word2oneDStream.updateStateByKey(updateFunc)

    result.print()

    //TODO 3 启动StreamingContext,并且阻塞主线程,一直执行
    ssc.start()
    ssc.awaitTermination()
  }

  //定义更新状态方法，参数seq为当前批次单词次数，state为以往批次单词次数
  def updateFunc = (seq:Seq[Int],state:Option[Int]) => {
    //获取当前批次单词的和
    val currentCount: Int = seq.sum
    //获取历史状态的数据
    val stateCount: Int = state.getOrElse(0)
    //将当前批次的和加上历史状态的数据和,返回
    Some(currentCount + stateCount)
  }
}

