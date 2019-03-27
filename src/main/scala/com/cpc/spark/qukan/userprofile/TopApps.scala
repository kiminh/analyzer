package com.cpc.spark.qukan.userprofile

import com.cpc.spark.common.Utils.sendMail
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
      .map(x => (x.getString(0), x.getAs[Seq[String]]("app_name")))
      .flatMap(_._2.map(x => (x, 1l)))
      .reduceByKey(_ + _)
      .filter(_._2 > 20000)
      .sortBy(x => x._2, false)

    pkgs.toDF("app_name", "install_user_num")
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/warehouse/dl_cpc.db/top_apps/%s".format(date))

    spark.sql(
      """
        |ALTER TABLE dl_cpc.top_apps add if not exists PARTITION(`date` = "%s")
        | LOCATION  '/warehouse/dl_cpc.db/top_apps/%s'
      """.stripMargin.format(date, date))

    println("count: "+pkgs.count())


    val iterator = pkgs.toLocalIterator

    var txt = ""
    while(iterator.hasNext){
      val t=iterator.next()
      txt = txt + "%s %s\n".format(t._1, t._2)
    }

//    for (i <- 0 until topApps_top100.length) {
    //      val t = topApps_top100(i)
    //      txt = txt + "%s %s\n".format(t._1, t._2)
    //    }

//    val b = sendMail(txt, "%s topApps 活跃用户数top100".format(date), Seq("zhanghongyang@aiclk.com", "dongwei@aiclk.com",
//      "zhangting@qutoutiao.net", "huxinjie@aiclk.com", "sujiaqi@qutoutiao.net", "weijinxian@qutoutiao.net", "yishaobin@qutoutiao.net", "mayinbo@qutoutiao.net"))
    val b = sendMail(txt, "%s topApps 活跃用户数top100".format(date), Seq("zhanghongyang@aiclk.com"))
    if (!b) {
      println("发送邮件失败")
    }

  }
}

