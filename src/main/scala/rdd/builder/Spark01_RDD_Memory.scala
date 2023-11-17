package rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // TODO 创建RDD
    // 从内存中创建RDD，将内存中集合的数据做为处理的数据源
    val seq = Seq[Int](1, 2, 3, 4)

    // parallelize: 并行
    // val rdd = sc.parallelize(seq)
    val rdd = sc.makeRDD(seq, 2)

    rdd.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()

  }
}
