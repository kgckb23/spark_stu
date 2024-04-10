package day0410

import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager

/**
 * 将RDD中的数据，以分区为单位，进行相应的处理
 *
 * mapPartitions方法函数的输入是一个迭代器，函数的返回值也必须是迭代器
 *
 * mapPartitionsWithIndex可以将RDD的分区编号作为参数传入
 *
 */
object MapPartitionsDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[5]")
    //1.创建SparkContext
    val sc = new SparkContext(conf)

        //商品ID，商品分类ID，商品金额
        var orders = List(
          (1,11,15.00),
          (2,12,31.00),
          (3,13,47.00),
          (4,14,99.00),
          (5,15,55.00)
        )

        val rdd1 = sc.parallelize(orders)

        //查询Mysql,关联维度数据，得到一个新的RDD
        //商品ID，商品分类ID，商品名称，商品金额
    //    val rdd2 = rdd1.map(e => {
    //      val cid = e._2
    //      //根据ID查找Mysql中维度的名称
    //      val connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/day0410?characterEncoding=utf-8", "root", "123456")
    //      val preparedStatement = connection.prepareStatement("select name from market where id = ?")
    //      preparedStatement.setInt(1, cid)
    //      val resultSet = preparedStatement.executeQuery()
    //      var name = "未知"
    //      if (resultSet.next()) {
    //        name = resultSet.getString(1)
    //      }
    //      (e._1, e._2, name, e._3)
    //    })
    //
    //    rdd2.saveAsTextFile("out1/01")

        val rdd2 = rdd1.mapPartitions(it => {
          //事先创建好连接
          val connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/day0410?characterEncoding=utf-8", "root", "123456")
          val preparedStatement = connection.prepareStatement("select name from market where id = ?")
          //一个分区中的多条数据用同一个连接对象
          it.map(e => {
            preparedStatement.setInt(1, e._2)
            val resultSet = preparedStatement.executeQuery()
            var name = "未知"
            if (resultSet.next()) {
              name = resultSet.getString(1)
            }
            (e._1, e._2, name, e._3)
          })
        })
        rdd2.saveAsTextFile("out1/01")

    /*val ints = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)

    val rdd1 = sc.parallelize(ints, 2)
    val rdd2 = rdd1.mapPartitionsWithIndex((index, it) => {
      it.map(e => {
        s"partition: $index, element: $e"
      })
    })

    rdd2.saveAsTextFile("out1/02")*/

    sc.stop()
  }
}
