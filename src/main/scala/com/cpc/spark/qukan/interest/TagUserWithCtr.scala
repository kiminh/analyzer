package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 10/10/2017.
  */
object TagUserWithCtr {

  def main(args: Array[String]): Unit = {
    val dayBefore = args(0).toInt
    val days = args(1).toInt

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val dataStart = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, days)
    val dataEnd = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val spark = SparkSession.builder()
      .appName("user article ")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val userCtr = spark.sql(
      """
        |select uid,isclick from dl_cpc.cpc_union_log
        |where isshow = 1 and adslot_type = 2 round(ext['adclass'].int_value / 1e6, 0) == 104 and `date` >= "%s" and `date` < "%s"
      """.stripMargin.format(dataStart, dataEnd))
      .rdd
      .map {
        row =>
          val uid = row.getString(0)
          val isclick = row.getInt(1)
          (uid, (1, isclick))
      }
      .reduceByKey {
        (x, y) =>
          (x._1 + y._1, x._2 + y._2)
      }

  }
}
