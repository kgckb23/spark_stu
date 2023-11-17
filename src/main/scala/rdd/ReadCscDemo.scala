package rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}

object ReadCscDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("readCscDemo")
    val sc = SparkContext.getOrCreate(conf)

    val lines = sc.textFile("in/users.csv")
    /*println(lines.count())
    println(lines.partitions.size)
    lines.glom().collect().foreach(x => println(x.length))
    lines.take(3).foreach(println)
    println(lines.take(3).mkString("\n"))*/

    /*val lines1 = lines.filter(x => x.startsWith("user_id") == false).map(x => x.split(","))
    println(lines1.count())
    lines1.collect().foreach(x => println(x.toList))*/

    /*val lines2 = lines.mapPartitionsWithIndex((index, values) => {
      if (index == 0) {
        values.drop(1)
      } else {
        values
      }
    })

    lines2.foreach(println)
    println(lines2.count())*/

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val df = spark.read.format("csv").option("header", true).load("in/users.csv")
    df.printSchema()
//    df.show() // 默认显示20行，默认自动隐藏
    //    df.show(10, false)

    val df2 = df.select("user_id", "gender", "birthyear")
    df2.show(10, false)
    df2.printSchema()

    val df3 = df2.withColumn("birthyear2",df2("birthyear").cast(DoubleType))
    val df4 = df3.drop("birthyear")
    df4.printSchema()
    df4.show(10)
  }
}
