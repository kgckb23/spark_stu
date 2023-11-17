package rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_File {
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // TODO 创建RDD
    // 从文件中创建RDD，将内存中集合的数据做为处理的数据源
    val rdd = sc.textFile("./in2")  // 路径可以是目录
    rdd.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()

  }
}
