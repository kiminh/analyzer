package com.cpc.spark.qukan.userprofile

import com.cpc.spark.qukan.parser.HdfsParser
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.cpc.spark.common.Utils.sendMail

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
      .parquet("/warehouse/dl_cpc.db/top_apps/%s".format(date))

    spark.sql(
      """
        |ALTER TABLE dl_cpc.top_apps add if not exists PARTITION(`date` = "%s")
        | LOCATION  '/warehouse/dl_cpc.db/top_apps/%s'
      """.stripMargin.format(date, date))

    pkgs.take(100).foreach(println)

    val topApps_top100 = spark.sql(
      s"""
         |select pkg, install_user_num
         |from dl_cpc.top_apps
         |where `date`='${date}'
         |sort by install_user_num desc
         |limit 10
      """.stripMargin)
      .collect()

    var txt = ""
    for (i <- 0 until topApps_top100.length) {
      val row = topApps_top100(i)
      txt = txt + "%-30s%-15s\n".format(row.getAs[String]("pkg"), row.getAs[Long]("install_user_num").toString)

    }

    val b = sendMail(txt, "'$date' topApps 活跃用户数top100", Seq("zhanghongyang@aiclk.com"))
    if (!b) {
      println("发送邮件失败")
    }

  }
}

