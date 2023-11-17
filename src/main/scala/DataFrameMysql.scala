import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * 加载外部数据源  MySQL
 */

object DataFrameMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkmysql").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    var url = "jdbc:mysql://localhost:3306/sql50"
    var user = "root"
    var pwd = "123456"
    val driver = "com.mysql.cj.jdbc.Driver"
    val properties = new Properties()
    properties.setProperty("user",user)
    properties.setProperty("password",pwd)
    properties.setProperty("driver",driver)

    val frame = spark.read.jdbc(url, "student", properties)
    frame.printSchema()
    frame.show()

    spark.close()
  }
}
