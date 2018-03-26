package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 18/10/2017.
  */
object CheckCatesCtr {

  val expUsers = Seq(
    //1501545,
    //1502313,
    //1500605,
    1000960,
    1502944,
    1001840,
    1501354
  )

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("check user interested words")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -args(0).toInt)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    for (userid <- expUsers) {
      println("retargeting", userid, date)
      val ulog = spark.sql(
        """
          |select isshow, isclick, ext['trigger_type'].int_value as trigger_type from dl_cpc.cpc_union_log
          |where `date` = "%s"  and isshow = 1 and userid = %d
        """.stripMargin.format(date, userid)).rdd

      val num = ulog.count()
      println("", num)

      if (num > 0) {
        val clk = ulog.filter(x => x.getAs[Int]("trigger_type") == 0)
          .map {
            x =>
              val show = x.getAs[Int]("isshow")
              val click = x.getAs[Int]("isclick")
              val tt = x.getAs[Int]("trigger_type")
              (tt, (show, click))
          }
          .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

        clk.toLocalIterator
          .foreach {
            x =>
              val c = x._2
              val ctr = c._2 / c._1
              println(x._1, c, ctr)
          }
      }
    }
  }
}
