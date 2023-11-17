import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DFexec1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("dfexec").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val accessLog = Array(
      "2016-12-27,001",
      "2016-12-27,001",
      "2016-12-27,002",
      "2016-12-28,003",
      "2016-12-28,004",
      "2016-12-28,002",
      "2016-12-28,002",
      "2016-12-28,001"
    )

    //    val rdd = sc.parallelize(accessLog).map(x => x.split(",")).map(x => Row(x(0), x(1)))
    val rdd = sc.parallelize(accessLog).map(x => {
      val arrs = x.split(",")
        Row(arrs(0),arrs(1).toInt)
    })

    val schema = StructType(Array(StructField("day", StringType), StructField("userid", IntegerType)))
    val frame = spark.createDataFrame(rdd, schema)
    frame.printSchema()
    frame.show()

    // 求每天所有的访问量
    import org.apache.spark.sql.functions._
    frame.groupBy("day").agg(count("userid").as("pv")).show()

    //求每天的去重访问量
    frame.groupBy("day").agg(countDistinct("userid").as("uv")).show

    spark.close()
  }
}
