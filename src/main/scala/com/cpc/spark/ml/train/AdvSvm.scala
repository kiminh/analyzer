package com.cpc.spark.ml.train

import com.cpc.spark.log.parser.UnionLog
import com.cpc.spark.ml.parser.MLParser
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.hashing.MurmurHash3.stringHash

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
    val table = args(0)
    val date = args(1)
    val hour = args(2)
    val sparkSession = SparkSession.builder()
      .appName("GenerateAdvSvm from %s %s".format(table, date))
      .enableHiveSupport()
      .getOrCreate()
    import sparkSession.implicits._
    val unionLog = sparkSession.sql(
      s"""
         |select * from dl_cpc.%s where `date` = "%s" and hour = "%s" and isfill = 1 and adslotid > 0
       """.stripMargin.format(table, date, hour))
      .as[UnionLog]
    unionLog.map {
      x =>

        MLParser.unionLogToSvm(x)
    }
    .toDF()
    .write
    .mode(SaveMode.Overwrite)
    .text("/user/cpc/svmdata/" + date + "/" + hour)
  }
}

