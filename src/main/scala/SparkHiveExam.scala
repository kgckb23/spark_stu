import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkHiveExam {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("sparkHiveExam")
      .config("hive.metastore.uris", "thrift://192.168.153.135:9083")
      .config("spark.sql.parquet.writeLegacyFormat",true)
      .enableHiveSupport().getOrCreate()


    spark.sql("show databases").show()
    spark.sql("use exam")
    spark.sql("show tables").show(false)

    spark.sql("""with tb as (select user_id,datediff('2017-12-03',max(dt)) as diff,max(dt) from userbehavior_partition where dt>'2017-11-03' and behavior_type='buy' group by user_id),tb2 as (select user_id,(case when diff between 0 and 6 then 4 when diff between 7 and 12 then 3 when diff between 13 and 18 then 2 when diff between 19 and 24 then 1 when diff between 25 and 30 then 0  else null end ) tag from tb) select * from tb2 where tag=3""").show(false)

    spark.sql("""with tb as (select user_id,count(user_id) as num from userbehavior_partition where dt between '2017-11-03' and '2017-12-03' and behavior_type='buy' group by user_id) select user_id,(case when num between 129 and 161 then 4 when num between 97 and 128 then 3 when num between 65 and 96 then 2 when num between 33 and 64 then 1 when num between 1 and 32 then 0 else null end) tag from tb""").show(false)

    spark.close()
  }
}
