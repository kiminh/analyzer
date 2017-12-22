package com.cpc.spark.ml.ctrmodel.v1

import com.cpc.spark.qukan.parser.HdfsParser
import org.apache.spark.sql.SparkSession

/**
  * Created by zhaolei on 22/12/2017.
  */

object saveUserAppInstalledInfo{
  def main(args:Array[String]): Unit ={
    if(args.length < 1){
      System.err.println(
        s"""
           |Usage: Tag user by installed apps <dayBefore int>
           |
        """.stripMargin
      )
      System.exit(1)
    }

    val date = args(0)

    val spark = SparkSession.builder().appName("save user app installed apps" + date)
      .enableHiveSupport().getOrCreate()

    saveUserInstalledApp(spark, date)

    spark.close()
  }

  def saveUserInstalledApp(spark : SparkSession, date: String): Unit ={

    println("------save user app installed apps %s------".format(date))

    val inpath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(date)
    val outpath = "/user/cpc/userInstalledApp/%s".format(date)

    spark.read.orc(inpath).rdd
      .map(HdfsParser.parseInstallApp(_, x => true, null))
      .filter(x => x != null && x.pkgs.length > 0)
      .map(x => (x.devid, x.pkgs.map(_.name)))
      .reduceByKey(_++_)
      .map(x => (x._1, x._2.distinct))
      .saveAsTextFile(outpath)
  }
}