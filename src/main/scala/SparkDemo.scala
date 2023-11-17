import org.apache.spark.sql.SparkSession

object SparkDemo {

  case class Point(label: String, x: Double, y: Double)

  case class Category(id: Long, name: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("sparkdemo").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    // DataSet DataFrame
    /*val ds = spark.createDataset(1 to 10)
    ds.printSchema()
    ds.show()

    println("-----------------------------")
    val ds2 = spark.createDataset(List(("zs", 5, "男"), ("lisi", 6, "女")))
    ds2.printSchema()
    ds2.show()
    val ds3 = ds2.withColumnRenamed("_1", "name")
      .withColumnRenamed("_2", "age")
      .withColumnRenamed("_3", "gender")
    ds3.printSchema()
    ds3.show()

    val rdd = sc.parallelize(List(("liuwei", 23, 170, 190), ("xupeng", 24, 180, 170)))
    val ds4 = spark.createDataset(rdd)
    val ds5 = ds4.withColumnRenamed("_1", "name")
      .withColumnRenamed("_2", "age")
    ds5.printSchema()
    ds5.show()*/

    val points = Seq(Point("nj", 23.1, 32.2), Point("bj", 23.2, 101.2), Point("xa", 80, 90))
    val pointsDS = points.toDS()

    val categories = Seq(Category(1, "bj"), Category(2, "nj"), Category(3, "xa"))
    val categoriesDS = categories.toDS()
    pointsDS.printSchema()
    categoriesDS.printSchema()
    pointsDS.show()
    categoriesDS.show()

    val df = pointsDS.join(categoriesDS, pointsDS("label") === categoriesDS("name"))
    df.printSchema()
    df.show()

  }
}
