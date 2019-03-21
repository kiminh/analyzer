package com.cpc.spark.ocpcV3.HP

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, lit}

object Lab {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().enableHiveSupport().appName("A test from sjq").getOrCreate()
    import spark.implicits._

    val sql1 =
      s"""
         |select
         |   dt,
         |   uid
         | from dl_cpc.slim_union_log
         |where dt between '2019-03-14' and '2019-03-20'
         |  and media_appsid = '80002819'
         |  group by dt, uid
       """.stripMargin

    val uid_dt = spark.sql(sql1)
    uid_dt.persist()
    uid_dt.show(10)


    val summary = scala.collection.mutable.ListBuffer[RemainMate]()
    val arr = Array("2019-03-14", "2019-03-15", "2019-03-16", "2019-03-17", "2019-03-18", "2019-03-19","2019-03-20")

    for (temp <- List(0,1,2,3,4,5)){
      val next = temp + 1
      val today = arr(temp)
      val tomorrow = arr(next)
      println("today: " + today + ", tomorrow: " + tomorrow)

      val rate = uid_dt.filter(s"dt = '$today'").selectExpr("dt as dt1", "uid")
        .join(uid_dt.filter(s"dt = '$tomorrow'").selectExpr("dt as dt2", "uid"), Seq("uid"), "left")
        .agg(
          count("dt1").alias("deno"),
          count("dt2").alias("num")
        ).withColumn("remain_rate", col("num")/col("deno"))
        .withColumn("date", lit(today))
        .select("deno", "num", "remain_rate")
      rate.show()

      val row = rate.rdd.collect().map(x => (x.getAs[Long]("deno"), x.getAs[Long]("num"), x.getAs[Double]("remain_rate")) )
      println("note1")
      val deno = row(0)._1
      val num  = row(0)._2
      val remain_rate = row(0)._3
      summary += RemainMate(deno, num, remain_rate)
    }
    val result = summary.toList.toDF()
    result.show()

  }

  case class RemainMate( var dau: Long, var remain: Long, var remainRate: Double)

}
