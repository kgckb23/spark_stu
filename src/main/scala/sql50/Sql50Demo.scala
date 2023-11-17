package sql50

import etl.utils.JdbcUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Sql50Demo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sql50demo").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val courseDF = JdbcUtils.getDataFrameMsql(spark, "course")
    //    courseDF.printSchema()
    //    courseDF.show()

    val scDF = JdbcUtils.getDataFrameMsql(spark, "sc")
    //    scDF.printSchema()
    //    scDF.show()

    val studentDF = JdbcUtils.getDataFrameMsql(spark, "student")
    //    studentDF.printSchema()
    //    studentDF.show()

    val teacherDF = JdbcUtils.getDataFrameMsql(spark, "teacher")
    //    teacherDF.printSchema()
    //    teacherDF.show()

    /*// 1.查询"01"课程比"02"课程成绩高的学生的信息及课程分数
    scDF.as("sc1").join(scDF.as("sc2"),"sid")
      .filter("sc1.cid = 01 and sc2.cid = 02 and sc1.score > sc2.score")
      .join(studentDF,"sid").show()

    // 2.查询"01"课程比"02"课程成绩低的学生的信息及课程分数
    scDF.as("sc1").join(scDF.as("sc2"),"sid")
      .filter("sc1.cid = 01 and sc2.cid = 02 and sc1.score < sc2.score")
      .join(studentDF,"sid").show()

    // 3.查询平均成绩大于等于60分的同学的学生编号和学生姓名和平均成绩
    scDF.as("sc").groupBy("sid").agg(round(avg("score"),2).as("avgscore"))
      .filter($"avgscore">=60).join(studentDF,"sid").show()

    // 4.查询平均成绩小于60分的同学的学生编号和学生姓名和平均成绩
    scDF.as("sc").groupBy("sid").agg(round(avg("score"), 2).as("avgscore"))
      .filter($"avgscore" < 60).join(studentDF, "sid").show()

    // 5.查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩
    studentDF
      .join(scDF.groupBy("sid").agg(count("sid"),sum("score")),Seq("sid"),"left_outer")
      .show

    // 6.查询"李"姓老师的数量
    val l = teacherDF.filter($"tname".like("李%")).count()
    println(l)

    // 7.查询学过"张三"老师授课的同学的信息
    scDF.join(courseDF,"cid")
      .join(teacherDF,"tid")
      .join(studentDF,"sid")
      .filter($"tname"==="张三").show

    // 8.查询没学过"张三"老师授课的同学的信息
    studentDF.as("s1")
      .join(studentDF.join(scDF, "sid")
        .join(courseDF, "cid")
        .join(teacherDF, "tid")
        .where($"tname".equalTo("张三")).as("s2"),
        Seq("sid"), "left_outer")
      .where($"score".isNull)
      .show()

    // 9.查询学过编号为"01"并且也学过编号为"02"的课程的同学的信息
    studentDF.join(scDF.as("sc1").filter($"cid" === "01" ),Seq("sid"))
      .join(scDF.as("sc2").filter($"cid" === "02"),Seq("sid")).show

    // 10.查询学过编号为"01"但是没有学过编号为"02"的课程的同学的信息
    studentDF.join(scDF.filter("cid=02"),Seq("sid"),"left_outer")
      .filter("cid is null")
      .join(scDF.filter("cid=01"),"sid").show()*/

    /*// 11.查询没有学全所有课程的同学的信息
    studentDF.join(scDF, Seq("sid"), "left_outer")
      .groupBy("sid").count().where("count<3").show()

    // 12.查询至少有一门课与学号为"01"的同学所学相同的同学的信息
    studentDF.join(scDF, "sid").as("s1")
      .join(
        studentDF.as("s2")
          .join(scDF, "sid")
          .where($"s2.sid" === "01")
        , "cid"
      )
      .select("s1.sid", "s1.sname", "s1.age", "s1.gender")
      .distinct()
      .filter($"sid" =!= "01")
      .show()

    // 13.查询和"01"号的同学学习的课程完全相同的其他同学的信息
    scDF.as("s1").filter("sid=01")
      .join(scDF.as("s2"), "cid")
      .groupBy("s2.sid").count().as("s3")
      .where($"count" === scDF.filter("sid=01").count())
      .filter($"sid" =!= "01")
      .join(studentDF, "sid")
      .show()*/

    // 14.查询没学过"张三"老师讲授的任一门课程的学生姓名
    studentDF.as("s1").join(
        studentDF.join(scDF, "sid")
          .join(courseDF, "cid")
          .join(teacherDF, "tid")
          .where($"tname".equalTo("张三")).as("s2")
        , Seq("sid"), "left_outer"
      )
      .where($"s2.tid".isNull)
      .select("s1.sid", "s1.sname", "s1.age", "s1.gender")
      .show()

    // 15.查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
    scDF.as("sc1").filter("score<60")
      .groupBy("sid").count()
      .filter("count>=2")
      .join(studentDF, "sid")
      .join(scDF, "sid")
      .groupBy("sid")
      .agg(round(avg("score"), 2))
      .show()

    // 16.检索"01"课程分数小于60，按分数降序排列的学生信息
    scDF.filter("cid=01")
      .join(studentDF, Seq("sid"), "right")
      .where("score<60 or score is null")
      .orderBy($"score".desc).show()

    // 17.按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩
    scDF.groupBy("sid").avg("score")
      .join(studentDF,"sid")
      .orderBy($"avg(score)".desc)
      .join(scDF,"sid").show()

    // 18.查询各科成绩最高分、最低分和平均分：以如下形式显示：课程ID，课程name，最高分，最低分，平均分，及格率，中等率，优良率，优秀率


  }
}
