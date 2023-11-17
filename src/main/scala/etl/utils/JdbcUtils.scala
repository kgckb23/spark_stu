package etl.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object JdbcUtils {
  private val url = "jdbc:mysql://localhost:3306/sql50?createDatabaseIfNotExist=true"
  private val driver = "com.mysql.cj.jdbc.Driver"
  private val user = "root"
  private val password = "123456"

  private val properties = new Properties()
  properties.setProperty("user", JdbcUtils.user)
  properties.setProperty("password", JdbcUtils.password)
  properties.setProperty("driver", JdbcUtils.driver)

  /**
   * 将dataframe数据写入到MySQL
   *
   * @param df
   * @param table
   * @param op
   */
  def dataFrameMysql(df: DataFrame, table: String, op: Int = 1): Unit = {
    if (op == 0) {
      df.write.mode(SaveMode.Append).jdbc(JdbcUtils.url, table, properties)
    }
    else {
      df.write.mode(SaveMode.Overwrite).jdbc(JdbcUtils.url, table, properties)
    }
  }

  /**
   * 将MySQL中的表数据读取到spark dataframe中
   *
   * @param spark
   * @param table
   * @return
   */
  def getDataFrameMsql(spark: SparkSession, table: String): DataFrame = {
    val frame = spark.read.jdbc(url, table, properties)
    frame
  }
}
