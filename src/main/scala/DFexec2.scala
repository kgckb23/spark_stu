import org.apache.spark.sql.{Row, SparkSession}

object DFexec2 {
  def main(args: Array[String]): Unit = {
    case class Hobbies(name:String,hobbies:String)

    val spark = SparkSession.builder().master("local[*]").appName("dfexec").getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.textFile("in/hobbies.txt")

    import spark.implicits._
//    val frame = rdd.map(_.split(" ")).map(x => Hobbies(x(0), x(1))).toDF()
//    frame.createOrReplaceTempView("hobbies")
//
//    spark.udf.register("hobby_num",(s:String)=>s.split(',').size)
//    spark.sql("select name,hobbies.hobby_num from hobbies").show
  }
}
