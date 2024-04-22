package month04

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, types}

object Demo_StructType {
  def main(args: Array[String]): Unit = {

    val sparkSQL = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val sparkSession = SparkSession.builder().config(sparkSQL).getOrCreate()

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val simpleData = Seq(
      Row("张三", "zs", "", "1", "M", 1000),
      Row("李四", "", "ls", "2", "M", 5000),
      Row("王五", "ww", "", "3", "M", 3000),
      Row("赵云", "zy", "zhaoyun", "4", "M", 2000),
      Row("吕布", "lb", "lvbu", "5", "M", -1)
    )

    val simpleSchema =
      StructType(
        Array(
          StructField("firstname", StringType),
          StructField("middlename", StringType),
          StructField("lastname", StringType),
          StructField("id", StringType),
          StructField("gender", StringType),
          StructField("salary", IntegerType)
        )
      )

    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(simpleData), simpleSchema)

    df.printSchema()
    df.show()

    val simpleData1 = Seq(
      Row(Row("张三", "zs", ""), "1", "M", 1000),
      Row(Row("李四", "", "ls"), "2", "M", 5000),
      Row(Row("王五", "ww", ""), "3", "M", 3000),
      Row(Row("赵云", "zy", "zhaoyun"), "4", "M", 2000),
      Row(Row("吕布", "lb", "lvbu"), "5", "M", -1)
    )

    val simpleSchema1 = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("id", StringType)
      .add("gender", StringType)
      .add("salary", IntegerType)

    val df1 = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(simpleData1), simpleSchema1
    )

    df1.printSchema()
    df1.show()

    df1.select(col("name").getField("firstname")).show(false)

    sparkSession.close()
  }
}
