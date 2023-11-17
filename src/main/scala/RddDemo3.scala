import org.apache.spark.{SparkConf, SparkContext}

object RddDemo3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rddDemo")
    val sc = SparkContext.getOrCreate(conf)

    val scores = Array(("Fred", 88), ("Fred", 95), ("Fred", 91), ("Wilma", 93), ("Wilma", 95), ("Wilma", 98))
    val a = sc.parallelize(scores,2)
    //a.combineByKey()
  }
}
