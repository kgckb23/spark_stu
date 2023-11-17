import org.apache.spark.{SparkConf, SparkContext}

object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("broadcastDemo")
    val sc = SparkContext.getOrCreate(conf)

    val arr = Array("hello", "hi", "nihao")
    val arr1 = Array((1, "hello"), (2, "hi"), (3, "nihao"))
    val bv = sc.broadcast(arr)
    val bv2 = sc.broadcast(arr1)

    val rdd = sc.parallelize(Array((1, "leader"), (2, "teamleader"), (3, "worker")))
    val rdd2 = rdd.mapValues(x => {
      println("values is " + x)
      bv2.value(1)._2 + ":" + x
    })
    rdd2.collect().foreach(println)
  }
}
