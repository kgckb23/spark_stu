package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("in/test.log")
    val mapRDD = rdd.map(
      line => {
        val datas = line.split("  ")
        datas(0)
      }
    )

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
