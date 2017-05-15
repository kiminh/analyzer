package com.cpc.spark.ml.train

import java.util.Calendar

import com.cpc.spark.log.parser.{LogParser, UnionLog}
import com.cpc.spark.ml.parser.MLParser
import org.apache.spark.rdd
import org.apache.spark.sql.{SaveMode, SparkSession}

object AdvSvm {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: GenerateAdvSvm <hive_table> <date> <hour>
           |
        """.stripMargin)
      System.exit(1)
    }
    val dayBefore = args(0).toInt
    val days = args(1).toInt
    val hourBegin = args(2).toInt
    val hours = args(3).toInt
    val sparkSession = SparkSession.builder()
      .appName("GenerateAdvSvm v1")
      .enableHiveSupport()
      .getOrCreate()
    import sparkSession.implicits._

    var unionLog: rdd.RDD[UnionLog] = null
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)

    for (n <- 1 to days) {
      val date = LogParser.dateFormat.format(cal.getTime)

      for (h <- hourBegin to hourBegin + hours - 1) {
        val log = sparkSession.sql(
          s"""
             |select * from dl_cpc.cpc_union_log where `date` = "%s" and hour = "%02d" and isfill = 1 and adslotid > 0
       """.stripMargin.format(date, h))
          .as[UnionLog].rdd

        if (unionLog == null) {
          unionLog = log
        } else {
          unionLog = unionLog.union(log)
        }
      }
      cal.add(Calendar.DATE, 1)
    }

    unionLog.map {
      x =>
        MLParser.unionLogToSvm(x)
    }
    .toDF()
    .write
    .mode(SaveMode.Overwrite)
    .text("/user/cpc/svmdata/v1")
  }
}

