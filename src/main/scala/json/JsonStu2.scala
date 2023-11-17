package json

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}


case class StudentData(student: String)

// """{"classes":"kb23","stuId":"1","stuName":"zhangsan","teacher":"dehua"}"""
object JsonStu2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("json2").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val schema = new StructType()
      .add("classes", StringType)
      .add("stuId", StringType)
      .add("stuName", StringType)
      .add("teacher", StringType)

    /*val frame = Seq("""{"classes":"kb23","stuId":"1","stuName":"zhangsan","teacher":"dehua"}""")
      .toDF("student")
    val frame1 = frame.select(
      get_json_object($"student", "$.classes").as("classes"),
      get_json_object($"student", "$.stuId").as("stuId"),
      get_json_object($"student", "$.stuName").as("stuName"),
      get_json_object($"student", "$.teacher").as("teacher")
    )
    frame1.printSchema()
    frame1.show()*/

    val ds = Seq("""{"classes":"kb23","stuId":"1","stuName":"zhangsan","teacher":"dehua"}""")
      .toDF("student")
      .as[StudentData]
    val frame2 = ds.select(from_json($"student", schema) as "student2")
    frame2.printSchema()
    frame2.select($"student2.*").show()
  }
}
