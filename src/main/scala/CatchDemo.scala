import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CatchDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("catchDemo")
    val sc = SparkContext.getOrCreate(conf)

    val rdd = sc.textFile("in/users.csv")
    rdd.persist(StorageLevel.MEMORY_ONLY_SER)
    val value = rdd.map(x => (x, 1))

    for (i <- 1 to 10) {
      val start = System.currentTimeMillis()
      println(value.count())
      val end = System.currentTimeMillis()
      println(i + ":" + (end - start))
      Thread.sleep(10)
    }

  }
}
