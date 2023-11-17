import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import java.sql.Struct

object DataFrameDemo {

  case class Point(label: String, x: Double, y: Double)

  case class Category(id: Long, name: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("dataframedemo").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    /*val peopleRdd = sc.textFile("in/people.txt")
    //    peopleRdd.collect().foreach(println)
    val peopleRDD2 = peopleRdd.map(x => {
      val strings = x.split(" ")
      (strings(0), strings(1), strings(2).toInt)
    })
    //    peopleRDD2.collect().foreach(println)

    // 第一种方式
    val frame = spark.createDataset(peopleRDD2).toDF("id", "name", "age")
    frame.printSchema()
    frame.show()

    // 第二种方式
    val fields = Array(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType)
    )
    val schema = StructType(fields)
    val rowRDD = peopleRdd.map(x => {
      val strings = x.split(" ")
      Row(strings(0).toInt, strings(1), strings(2).toInt)
    })
    val frame1 = spark.createDataFrame(rowRDD, schema)
    frame1.printSchema()
    frame1.show()

    frame1.filter(frame1("age") > 30).select("name").show()

    frame1.filter("age>30").select("name").show()

    frame1.filter($"age" > 30).select("name").show()

    frame1.filter(col("age") > 30).select("name").show()

    frame1.createOrReplaceTempView("people")
    val frame2 = spark.sql("select name from people where age>30")
    frame2.show()*/

    val userDF = spark.read.format("json").json("in/user.json")
    userDF.printSchema()
    userDF.show()

    // Parquet操作 写出parquet
//    userDF.write.parquet("out/data/user")

    // 读取parquet
    val frame = spark.read.parquet("out/data/user")
    frame.printSchema()
    frame.show()
  }
}
