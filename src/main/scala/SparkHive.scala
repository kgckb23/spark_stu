import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("sparkHive")
      .config("hive.metastore.uris", "thrift://192.168.153.135:9083")
      .config("spark.sql.parquet.writeLegacyFormat",true)
      .enableHiveSupport().getOrCreate()


    spark.sql("show databases").show()

    spark.sql("show tables").show(false)

    val frame = spark.table("shopping.transaction_details")
    frame.show()

    val frame1 = frame.groupBy("store_id")
      .agg(count("customer_id").as("customerNum"),
        sum("price").as("totalMoney"))
    frame1.show()

    frame1.write.mode(SaveMode.Overwrite).saveAsTable("shopping.storeCal")

    spark.close()
  }
}
