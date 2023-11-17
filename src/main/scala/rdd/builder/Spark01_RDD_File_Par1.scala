package rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_File_Par1 {
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // TODO 创建RDD
    val rdd = sc.textFile("in2/word.txt", 2)

    rdd.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()

  }
}
