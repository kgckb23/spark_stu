package etl

import etl.utils.JdbcUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ActiveUser {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("activeuser").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val logs = JdbcUtils.getDataFrameMsql(spark, "full_log")

    val ds = logs.filter($"actionName" === "BuyCourse" || $"actionName" === "StartLearn")
    ds.printSchema()
    ds.show()

    val ds2 = ds.map(x => (
      x.getAs[String]("userSID"),
      x.getAs[String]("event_time").substring(0, 10)
    ))
    ds2.printSchema()

    val activeDF = ds2.withColumnRenamed("_2", "date")
      .withColumnRenamed("_1", "userid")
      .groupBy($"date")
      .agg(countDistinct("userid").as("activeNum"))
    activeDF.printSchema()
    activeDF.show()

    JdbcUtils.dataFrameMysql(activeDF,"active")

    spark.close()
  }
}
