import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = SparkContext.getOrCreate(conf)

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val rdd = sc.textFile("hdfs://kb135:9000/spark/*.txt")
    val rdd2 = rdd.flatMap(x => x.split(" "))
    val rdd3 = rdd2.map(x => (x, 1))
    val res = rdd3.reduceByKey((x, y) => x + y)
    res.collect.foreach(println)

    res.saveAsTextFile("hdfs://kb135:9000/spark/output")
  }
}
