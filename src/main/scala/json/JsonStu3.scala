package json

import etl.utils.JdbcUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import java.sql.Struct

object JsonStu3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("json3").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val optionRDD = sc.textFile("in/op.log")

    //    optionRDD.foreach(println)

    val option1 = optionRDD.map(x => {
      val arr = x.split('|')
      (arr(0), arr(1))
    })
    //    option1.collect().foreach(println)

    val jsonStrDF = option1.toDF("id", "value")
    //    jsonStrDF.printSchema()
    //    jsonStrDF.show()

    val jsonObjDF = jsonStrDF.select($"id",
      get_json_object($"value", "$.cm").as("cm"),
      get_json_object($"value", "$.ap").as("ap"),
      get_json_object($"value", "$.et").as("et"),
    )
    //    jsonObjDF.printSchema()
    //    jsonObjDF.show()

    val jsonObjDF2 = jsonObjDF.select($"id", $"ap",
      get_json_object($"cm", "$.ln").cast(DoubleType).as("ln"),
      get_json_object($"cm", "$.sv").as("sv"),
      get_json_object($"cm", "$.os").as("os"),
      get_json_object($"cm", "$.g").as("g"),
      get_json_object($"cm", "$.mid").cast(IntegerType).as("mid"),
      get_json_object($"cm", "$.nw").as("nw"),
      get_json_object($"cm", "$.l").as("l"),
      get_json_object($"cm", "$.vc").cast(IntegerType).as("vc"),
      get_json_object($"cm", "$.hw").as("hw"),
      get_json_object($"cm", "$.ar").as("ar"),
      get_json_object($"cm", "$.uid").cast(IntegerType).as("uid"),
      get_json_object($"cm", "$.t").cast(LongType).as("t"),
      get_json_object($"cm", "$.la").as("la"),
      get_json_object($"cm", "$.md").as("md"),
      get_json_object($"cm", "$.vn").as("vn"),
      get_json_object($"cm", "$.ba").as("ba"),
      get_json_object($"cm", "$.sr").as("sr"),
      $"et"
    )
    //    jsonObjDF2.printSchema()
    //    jsonObjDF2.show()

    val schema = StructType(
      Array(
        StructField("ett", StringType),
        StructField("en", StringType),
        StructField("kv", StringType),
      )
    )

    val jsonObjDF3 = jsonObjDF2.select(
      $"id", $"ap", $"ln", $"sv", $"os", $"g", $"mid", $"nw", $"l", $"vc",
      $"hw", $"ar", $"uid", $"t", $"la", $"md", $"vn", $"ba", $"sr",
      from_json($"et", ArrayType(schema)).as("events"))
//    jsonObjDF3.printSchema()
//    jsonObjDF3.show(false)

    val jsonObjDF4 = jsonObjDF3.withColumn("events", explode($"events"))
//    jsonObjDF4.printSchema()
//    jsonObjDF4.show(false)

    val jsonObjDF5 = jsonObjDF4.withColumn("ett", $"events.ett")
      .withColumn("en", $"events.en")
      .withColumn("kv", $"events.kv").drop("events")
    jsonObjDF5.printSchema()
    jsonObjDF5.show()

    JdbcUtils.dataFrameMysql(jsonObjDF5,"events")

    spark.close()
  }
}
