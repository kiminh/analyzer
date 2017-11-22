package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 18/10/2017.
  */
object CheckWordsCtr {

  val expUsers = Seq(
    1501545,
    1502313,
    1500605,
    1000960,
    1502944,
    1001840,
    1501354
  )

  def main(args: Array[String]): Unit = {

    val inid = args(1).toInt

    val spark = SparkSession.builder()
      .appName("check user interested words")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -args(0).toInt)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    for (userid <- expUsers) {
      val ulog = spark.sql(
        """
          |select * from dl_cpc.cpc_union_log
          |where `date` = "%s"  and isshow = 1 and ext['trigger_type'].int_value = 1
        """.stripMargin.format(date)).as[UnionLog].rdd

      ulog.map(x => (x.isclick, x.interests.split(",")))
        .map {
          x =>
            val tag = x._2.filter(_.length > 0).map {
              v =>
                val t = v.split("=")
                if (t.length > 0) {
                  (t(0).toInt, t(1).toInt)
                } else {
                  (0, 0)
                }
            }
              .find(_._1 == inid).orNull

            if (tag != null) {
              if (tag._2 > 400) {
                (400, (x._1, 1))
              } else if (tag._2 > 200) {
                (200, (x._1, 1))
              } else if (tag._2 > 100) {
                (100, (x._1, 1))
              } else if (tag._2 > 50) {
                (50, (x._1, 1))
              } else {
                (10, (x._1, 1))
              }
            } else {
              (0, (x._1, 1))
            }
        }
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        .sortByKey()
        .map(x => (x._1, x._2._1, x._2._2))
        .toLocalIterator
        .foreach {
          x =>
            println(x._1, x._2, x._3, x._2.toDouble / x._3.toDouble)
        }
    }
  }
}
