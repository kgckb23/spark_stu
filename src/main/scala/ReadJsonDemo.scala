import org.apache.spark.sql.SparkSession

object ReadJsonDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("readjson").getOrCreate()
    val sc = spark.sparkContext

    /*val lines = sc.textFile("in/user.json")
    lines.collect().foreach(println)

    import scala.util.parsing.json.JSON
    val rdd = lines.map(x => JSON.parseFull(x))
    rdd.collect().foreach(println)*/

    val userDF = spark.read.format("json").option("head", false).load("in/user.json")
    userDF.printSchema()
    userDF.show()
    userDF.select("id","age").show()

    val idColumn = userDF("id")
    val ageColumn = userDF("age")
    userDF.select(idColumn,ageColumn).show()
    userDF.filter(ageColumn>21).show()
  }
}
