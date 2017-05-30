package com.cpc.spark.ml.train

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
import com.cpc.spark.ml.parser.FeatureParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


object AdvSvm {


  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: GenerateAdvSvm <hive_table> <date> <hour>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger().setLevel(Level.WARN)
    val dayBefore = args(0).toInt
    val days = args(1).toInt
    val ctx = SparkSession.builder()
      .appName("GenerateAdvSvm v3")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      println("get data " + date)

      val svm = ctx.sql(
        s"""
           |select * from dl_cpc.cpc_union_log where `date` = "%s" and isfill = 1 and adslotid > 0
        """.stripMargin.format(date))
        .as[UnionLog].rdd
        .filter(x => x != null && x.searchid.length > 0)
        .map(FeatureParser.parseUnionLog(_))
        .filter(_.length > 0)
        .cache()

      svm.toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/svmdata/v3/" + date)
      cal.add(Calendar.DATE, 1)

      println("done", svm.count())
      svm.unpersist()
    }

    ctx.stop()
  }
}

