package etl

import etl.utils.JdbcUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

case class LogInfo(event_time: String, url: String, method: String, status: String,
                   sip: String, user_uip: String, action_prepend: String, action_client: String)

/**
 * 数据清洗
 */
object EtlDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("etldemo").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val rowRdd = sc.textFile("in/test.log").map(x => x.split("\t"))
      .filter(x => x.length >= 8)
      .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7)))

    val logDF2 = sc.textFile("in/test.log")
      .map(x => x.split("\t"))
      .filter(x => x.length >= 8)
      .map(x => LogInfo(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7))).toDF()


    logDF2.printSchema()
    logDF2.show(5)

    val schema = StructType(
      Array(
        StructField("event_time", StringType),
        StructField("url", StringType),
        StructField("method", StringType),
        StructField("status", StringType),
        StructField("sip", StringType),
        StructField("user_uip", StringType),
        StructField("action_prepend", StringType),
        StructField("action_client", StringType)
      )
    )

    val logDF = spark.createDataFrame(rowRdd, schema)

    val full_logDF = logDF.dropDuplicates("event_time", "url")
      .filter(x => x(3) == "200")
      .filter(x => StringUtils.isNotEmpty(x(0).toString))
      .map(line => {
        val str = line.getAs[String]("url")
        val paramArray = str.split("\\?")
        var paramsMap: Map[String, String] = null
        if (paramArray.length == 2) {
          val tuples = paramArray(1).split("&")
            .map(x => x.split("="))
            .filter(x => x.length == 2)
            .map(x => (x(0), x(1)))
          paramsMap = tuples.toMap
        }
        (
          line.getAs[String]("event_time"),
          paramsMap.getOrElse[String]("userUID", ""),
          paramsMap.getOrElse[String]("userSID", ""),
          paramsMap.getOrElse[String]("actionBegin", ""),
          paramsMap.getOrElse[String]("actionEnd", ""),
          paramsMap.getOrElse[String]("actionType", ""),
          paramsMap.getOrElse[String]("actionName", ""),
          paramsMap.getOrElse[String]("actionValue", ""),
          paramsMap.getOrElse[String]("actionTest", ""),
          paramsMap.getOrElse[String]("ifEquipment", ""),
          line.getAs[String]("method"),
          line.getAs[String]("status"),
          line.getAs[String]("sip"),
          line.getAs[String]("user_uip"),
          line.getAs[String]("action_prepend"),
          line.getAs[String]("action_client")
        )
      }).toDF("event_time", "userUID", "userSID", "actionBegin", "actionEnd", "actionType",
        "actionName", "actionValue", "actionTest", "ifEquipment", "method", "status", "sip", "user_uip", "action_prepend", "action_client")
    full_logDF.printSchema()
    full_logDF.show()
    JdbcUtils.dataFrameMysql(full_logDF, "full_log")


    logDF.printSchema()
    logDF.show(5)
  }
}
