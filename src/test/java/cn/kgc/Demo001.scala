package cn.kgc

import org.apache.spark.{SparkConf, SparkContext}

object Demo001 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("demo001")
    val sc = SparkContext.getOrCreate(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    /*val i = rdd.reduce(_ + _)
    println(i)*/

    /*var sum = 0
    rdd.foreach(
      num => {
        sum += num
        println("在foreach中sum的值是：" + sum)
      }
    )
    println("sum = " + sum)*/

    //使用累加器进行操作
    //1.根据数据类型提供对应累加器操作
    //累加器提供名字是为了在webUI中更好的观察累加器
    var sum = sc.longAccumulator("add")
    //使用累加器中add方法
    rdd.foreach(
      num =>
        sum.add(num)
    )

    println("sum = " + sum.value)

    sc.stop()
  }
}
