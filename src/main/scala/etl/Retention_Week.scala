package etl

import etl.utils.JdbcUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat

object Retention_Week {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("retention").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val logs = JdbcUtils.getDataFrameMsql(spark, "full_log")
    //    logs.printSchema()
    //    logs.show(3)

    // 找出注册用户id和注册时间
    val registered = logs.filter($"actionName" === "Registered")
      .withColumnRenamed("event_time", "register_time")
      .select("userUID", "register_time")
    registered.show(false)

    // 找出登录用户日志数据
    val signin = logs.filter($"actionName" === "Signin")
      .withColumnRenamed("event_time", "signin_time")
      .select("userUID", "signin_time")
    signin.printSchema()
    signin.show(false)

    // 两个dataframe关联
    val joined = registered.join(signin, Seq("userUID"), "left")
    joined.printSchema()
    joined.show(false)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val mydefDataFormat = spark.udf.register("mydefDataFormat", (event_time: String) => {
      if (StringUtils.isEmpty(event_time))
        0
      else
        dateFormat.parse(event_time).getTime
    })

    val joined2 = joined.withColumn("register_date", mydefDataFormat($"register_time"))
      .withColumn("sigin_date", mydefDataFormat($"signin_time"))
    joined2.printSchema()
    joined2.show(3, false)

    // 求出前一天注册，当天登录的用户数量 :m         m/n
    val signinNumDF = joined2.filter($"register_date" + 86400000 <= $"sigin_date" && $"sigin_date" <= $"register_date" + 86400000 * 7)
      .groupBy($"register_date")
      .agg(countDistinct($"userUID").as("signinNum"))
    signinNumDF.show(3, false)

    // 求出每日注册用户数量 :n
    val registerNumDF = joined2.groupBy($"register_date")
      .agg(countDistinct("userUID").as("registerNum"))
    registerNumDF.show(3, false)

    val joinRegisterAndSigninDF = signinNumDF.join(registerNumDF, Seq("register_date"))
    joinRegisterAndSigninDF.select($"register_date", ($"signinNum" / $"registerNum").as("percent")).show

    spark.close()
  }
}
