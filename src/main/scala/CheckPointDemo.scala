import org.apache.spark.{SparkConf, SparkContext}

object CheckPointDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("checkpointDemo")
    val sc = SparkContext.getOrCreate(conf)

    sc.setCheckpointDir("./checkpoint1")

    val rdd = sc.parallelize(1 to 20)
    rdd.checkpoint()
    rdd.glom().collect().foreach(x=>println(x.toList))
    println(rdd.isCheckpointed)
    println(rdd.getCheckpointFile)
  }
}
