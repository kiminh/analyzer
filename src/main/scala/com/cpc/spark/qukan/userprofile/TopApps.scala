package com.cpc.spark.qukan.userprofile

import com.cpc.spark.qukan.parser.HdfsParser
import org.apache.spark.sql.{SaveMode, SparkSession}

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

    val inpath = "/user/cpc/userInstalledApp/%s".format(date)
    println("------save user installed apps %s------".format(date))

    import spark.implicits._
    val pkgs = spark.read.parquet(inpath)
      .rdd
      .map(x => (x.getString(0), x.getAs[Seq[String]]("pkgs")))
      .flatMap(_._2.map(x => (x, 1l)))
      .reduceByKey(_ + _)
      .sortBy(x => x._2, false)

    pkgs.toDF("pkg", "install_user_num")
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("dl_cpc.top_apps")
  }
}


