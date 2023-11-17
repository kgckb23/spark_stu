package cn.kgc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

//自定义累加器
class MyAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  //提供一个HashMap对象在类中进行使用操作
  private val _hashAcc = new mutable.HashMap[String, Int]()

  //判断累加器是否初始化
  override def isZero: Boolean = {
    _hashAcc.isEmpty
  }

  //拷贝一个全新的累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new MyAccumulator
    _hashAcc.synchronized {
      newAcc._hashAcc ++= (_hashAcc)
    }
    newAcc
  }

  //重置累加器
  override def reset(): Unit = {
    _hashAcc.clear()
  }

  //向累加器中增加值（计算每一个分区内的数据）
  override def add(v: String): Unit = {
    _hashAcc.get(v) match {
      case None => _hashAcc += ((v, 1))
      case Some(a) => _hashAcc += ((v, a + 1))
    }
  }

  //合并所有分区内的累加器之和（计算每一个分区间的数据）
  //合并当前累加器和其他累加器，两两合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case o: AccumulatorV2[String, mutable.HashMap[String, Int]] => {
        for ((k, v) <- o.value) {
          _hashAcc.get(k) match {
            case None => _hashAcc += ((k, v))
            case Some(a) => _hashAcc += ((k, a + v))
          }
        }
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    _hashAcc
  }
}

object MyAccumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MyAccumulator").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //创建自定义累加器对象
    val hashAcc = new MyAccumulator
    //注册累加器
    sc.register(hashAcc, "abc")
    //提供rdd
    val rdd = sc.makeRDD(Array("a", "b", "c", "c", "a"))
    rdd.foreach(hashAcc.add(_))
    for ((k, v) <- hashAcc.value) {
      println("k = " + k + ",v = " + v)
    }
  }
}
