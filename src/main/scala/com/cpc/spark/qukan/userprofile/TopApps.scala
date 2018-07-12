package com.cpc.spark.qukan.userprofile

import com.cpc.spark.qukan.parser.HdfsParser
import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 12/07/2018.
  */
object TopApps {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: save user installed apps <date=string> <hour=string>
           |
        """.stripMargin
      )
      System.exit(1)
    }

    val date = args(0)

    val spark = SparkSession.builder().appName("save user installed apps " + date)
      .enableHiveSupport().getOrCreate()

    val inpath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(date)
    val outpath = "/user/cpc/userInstalledApp/%s".format(date)

    println("------save user installed apps %s------".format(date))

    import spark.implicits._
    var pkgs = spark.read.orc(inpath)
      .rdd
      .map(HdfsParser.parseInstallApp(_, x => true, null))
      .filter(x => x != null && x.pkgs.length > 0)
      .map(x => (x.devid, x.pkgs.map(_.name)))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, x._2.distinct))
      .flatMap(_._2.map(x => (x, 1)))
      .reduceByKey(_ + _)
      .sortBy(x => x._2, false)

    pkgs.take(100).foreach(println)

  }

}
