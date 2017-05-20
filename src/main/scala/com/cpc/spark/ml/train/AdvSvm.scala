package com.cpc.spark.ml.train

import java.util.Calendar

import com.cpc.spark.log.parser.{LogParser, UnionLog}
import com.cpc.spark.ml.parser.MLParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object AdvSvm {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger().setLevel(Level.WARN)
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: GenerateAdvSvm <hive_table> <date> <hour>
           |
        """.stripMargin)
      System.exit(1)
    }
    val dayBefore = args(0).toInt
    val days = args(1).toInt
    val sparkSession = SparkSession.builder()
      .appName("GenerateAdvSvm v1")
      .enableHiveSupport()
      .getOrCreate()
    import sparkSession.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    for (n <- 1 to days) {
      val date = LogParser.dateFormat.format(cal.getTime)
      println("get data in " + date)

      val log = sparkSession.sql(
        s"""
           |select * from dl_cpc.cpc_union_log where `date` = "%s" and isfill = 1 and adslotid > 0
        """.stripMargin.format(date))
        .as[UnionLog].rdd

      log.map(x => MLParser.unionLogToSvm(x))
        .filter(_ != "")
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("date")
        .text("/user/cpc/svmdata/v1/" + date)

      cal.add(Calendar.DATE, 1)
      println("done")
    }

    sparkSession.stop()
  }
}

