package rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(
      List(1, 2, 3, 4)
    )

    //    rdd.map(x => x * 2).collect().foreach(println)

    rdd.map(_ * 2).collect().foreach(println)
  }
}
