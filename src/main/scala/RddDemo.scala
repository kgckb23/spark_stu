import org.apache.spark.{SparkConf, SparkContext}

object RddDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rddDemo")
    val sc = SparkContext.getOrCreate(conf)

   /* val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
    println(rdd.count())
    println(rdd.partitions.size)
    rdd.glom().collect().foreach(x => println(x.toList))*/

    //val rdd2 = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), 5)

    val rdd2 = sc.parallelize(Array(("hello", 1), ("world", 1)))
    val rdd2b = rdd2.mapValues(x => x * 2)
    val rdd2c = rdd2.map(x => (x._1, x._2 * 2))
    rdd2b.collect().foreach(println)
    rdd2c.collect().foreach(println)
  }
}
