package json

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object JsonStu4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("json4").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val rdd = sc.textFile("in/school.log")

    val frame = rdd.toDF("value")
    //    frame.printSchema()
    //    frame.show(false)

    val frame1 = frame.select(
      get_json_object($"value", "$.name").as("name"),
      get_json_object($"value", "$.roomInfo").as("roomInfo"),
      get_json_object($"value", "$.students").as("students"),
      get_json_object($"value", "$.teachers").as("teachers"),
    )
    frame1.printSchema()
    frame1.show(false)

    val frame2 = frame1.select(
      $"name",
      get_json_object($"roomInfo", "$.area").as("area"),
      get_json_object($"roomInfo", "$.employee").as("employee"),
      get_json_object($"roomInfo", "$.roomNum").as("roomNum"),
      $"students", $"teachers"
    )
    frame2.show(false)

    val studentSchema = StructType(
      Array(
        StructField("classes", StringType),
        StructField("stuId", StringType),
        StructField("stuName", StringType),
        StructField("teacher", StringType),
      )
    )

    val frame3 = frame2.select($"name", $"area", $"employee", $"roomNum",
      from_json($"students", ArrayType(studentSchema)).as("students")
    )
    frame3.show(false)
    val frame4 = frame3.withColumn("students", explode($"students"))
    frame4.show(false)
    val stuFrame = frame4
      .withColumn("stuId", $"students.stuId")
      .withColumn("stuName", $"students.stuName")
      .withColumn("classes", $"students.classes")
      .withColumn("teacher", $"students.teacher").drop("students")
    stuFrame.show(false)

    val teacherSchema = StructType(
      Array(
        StructField("name", StringType),
        StructField("skill", StringType),
        StructField("yearNum", IntegerType)
      )
    )

    val frame5 = frame2.select($"name", $"area", $"employee", $"roomNum",
      from_json($"teachers", ArrayType(teacherSchema)).as("teachers")
    )
    frame5.show(false)

    val frame6 = frame5.withColumn("teachers", explode($"teachers"))
    frame6.show(false)
    val teaFrame = frame6.withColumn("teacherName", $"teachers.name")
      .withColumn("skill", $"teachers.skill")
      .withColumn("yearNum", $"teachers.yearNum")
      .drop("teachers","name","area","employee","roomNum")
    teaFrame.show(false)

    val frame7 = stuFrame.join(teaFrame, $"teacher" === $"teacherName").drop("teacherName")
    frame7.show(false)


  }
}
