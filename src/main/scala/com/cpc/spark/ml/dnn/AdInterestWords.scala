package com.cpc.spark.ml.dnn

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

/**
  * 生成用户点过广告的关键词中间表
  * 用于追踪用户兴趣
  * created time : 2018/11/22 17:59
  *
  * @author zhj
  * @version 1.0
  *
  */
object AdInterestWords {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val date = args(0)

    val sql =
      s"""
         |select a.uid,
         |     collect_set(if(load_date='${getDay(date, 1)}',b.tokens,null)) as words1,
         |     collect_set(if(load_date>='${getDay(date, 3)}'
         |                  and load_date<='${getDay(date, 1)}',b.tokens,null)) as words3
         |from dl_cpc.cpc_user_behaviors a
         |join dl_cpc.ideaid_title b
         |  on a.ideaid = b.ideaid
         |where a.load_date >= '${getDay(date, 3)}'
         |group by a.uid
      """.stripMargin

    println(sql)

    val data = spark.sql(sql)
      .rdd
      .map { r =>
        val uid = r.getAs[String]("uid")
        val words1 = r.getAs[Seq[String]]("words1").mkString(" ").split(" ").distinct
        val words3 = r.getAs[Seq[String]]("words3").mkString(" ").split(" ").distinct
        (uid, words1, words3)
      }.toDF("uid", "interest_ad_words_1", "interest_ad_words_3")
      .persist()

    println("total num is : " + data.count)

    data.write.mode("overwrite")
      .parquet("/warehosue/dl_cpc.db/cpc_user_interest_words/")

    spark.sql("refresh table dl_cpc.cpc_user_interest_words")
  }

  def getDay(startdate: String, day: Int): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(startdate))
    cal.add(Calendar.DATE, -day)
    format.format(cal.getTime)
  }
}
