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

    val spark = SparkSession
      .builder()
      .appName("save user installed apps " + date)
      .enableHiveSupport().getOrCreate()

    val inpath = "/user/cpc/userInstalledApp/%s".format(date)
    println("------save user installed apps %s------".format(date))

    import spark.implicits._
    val pkgs = spark.read.parquet(inpath).rdd

    //1. 所有媒体活跃用户DAU大于2w 的app
    val allApps = pkgs
      .map(x => (x.getString(0), x.getAs[Seq[String]]("app_name")))
      .flatMap(_._2.map(x => (x, 1l)))
      .reduceByKey(_ + _)
      .filter(_._2 > 20000)
      .sortBy(x => x._2, false)

    allApps.toDF("pkg", "install_user_num")
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/warehouse/dl_cpc.db/top_apps/%s".format(date))

    spark.sql(
      """
        |ALTER TABLE dl_cpc.top_apps add if not exists PARTITION(`date` = "%s")
        | LOCATION  '/warehouse/dl_cpc.db/top_apps/%s'
      """.stripMargin.format(date, date))
    println("all Apps count: " + allApps.count())

    val iterator = allApps.toLocalIterator
    var txt = ""
    while (iterator.hasNext) {
      val t = iterator.next()
      txt = txt + "%s %s\n".format(t._1, t._2)
    }

    //    val b = sendMail(txt, "%s topApps 活跃用户DAU[所有媒体]".format(date), Seq("zhanghongyang@aiclk.com", "dongwei@aiclk.com",
    //      "zhangting@qutoutiao.net", "huxinjie@aiclk.com", "sujiaqi@qutoutiao.net", "weijinxian@qutoutiao.net",
    //      "yishaobin@qutoutiao.net", "yuxiaoyang@qutoutiao.net", "heting@qutoutiao.net",
    //      "yuyao02@qutoutiao.net", "liutianlin@qutoutiao.net", "baizhen@qutoutiao.net", "zhangzhiyang@qutoutiao.net",
    //      "duruiyu@qutoutiao.net", "chenge@qutoutiao.net"))
    val b = sendMail(txt, "%s topApps 活跃用户DAU[所有媒体]".format(date), Seq("zhanghongyang@aiclk.com"))
    if (!b) {
      println("发送邮件失败")
    }


    //2. 安装qtt活跃用户DAU大于2w的app
    val qttApps = pkgs
      .map(x => (x.getString(0), x.getAs[Seq[String]]("app_name")))
      .filter(x => x._2.contains("com.jifen.qukan-趣头条"))
      .flatMap(_._2.map(x => (x, 1l)))
      .reduceByKey(_ + _)
      .filter(_._2 > 20000)
      .sortBy(x => x._2, false)

    val iteratorQtt = qttApps.toLocalIterator
    var qtt_txt = ""
    while (iteratorQtt.hasNext) {
      val t = iteratorQtt.next()
      qtt_txt = qtt_txt + "%s %s\n".format(t._1, t._2)
    }

    //    val qtt = sendMail(qtt_txt, "%s topApps 活跃用户DAU[仅趣头条]".format(date), Seq("zhanghongyang@aiclk.com", "dongwei@aiclk.com",
    //      "zhangting@qutoutiao.net", "huxinjie@aiclk.com", "sujiaqi@qutoutiao.net", "weijinxian@qutoutiao.net",
    //      "yishaobin@qutoutiao.net", "yuxiaoyang@qutoutiao.net", "heting@qutoutiao.net",
    //      "yuyao02@qutoutiao.net", "liutianlin@qutoutiao.net", "baizhen@qutoutiao.net", "zhangzhiyang@qutoutiao.net",
    //      "duruiyu@qutoutiao.net", "chenge@qutoutiao.net"))
    val qtt = sendMail(qtt_txt, "%s topApps 活跃用户DAU[仅趣头条]".format(date), Seq("zhanghongyang@aiclk.com"))
    if (!qtt) {
      println("发送邮件失败")
    }

  }
}

