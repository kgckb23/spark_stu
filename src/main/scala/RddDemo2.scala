import org.apache.spark.{SparkConf, SparkContext}

object RddDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rddDemo")
    val sc = SparkContext.getOrCreate(conf)

    val a = sc.parallelize(Array(("zhangsan", 99, 98, 100), ("lisi", 99, 98, 100), ("wangwu", 99, 98, 100), ("zhangmazi", 92, 95, 100)))
    //    a.filter(x => x._1.startsWith("zhang")).map(x => (x._1, x._2 + x._3 + x._4)).sortBy(x => -x._2).take(2).foreach(println)
    //
    //    println(a.filter(x => x._1.startsWith("zhang")).map(x => (x._2 + x._3 + x._4, x._1)).max)

    a.filter(x => x._1.startsWith("zhang")).map(x => (x._1, x._2 + x._3 + x._4)).
      groupBy(x => x._1.substring(0, 5)).map(x => {
        var name = ""
        var sumscore = 0
        val itor = x._2.iterator
        for (e <- itor) {
          if (e._2 > sumscore) {
            name = e._1;
            sumscore = e._2
          }
        }
        (name, sumscore)
      }).collect().foreach(println)
  }
}
