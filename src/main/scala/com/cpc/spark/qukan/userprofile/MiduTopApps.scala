package com.cpc.spark.qukan.userprofile

import com.cpc.spark.common.Utils.sendMail
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author zhy
  * @date 2019/5/20 20:18
  * @version 1.0
  * @desc
  */
object MiduTopApps {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: save user installed apps <date=string> <hour=string>
        """.stripMargin
      )
      System.exit(1)
    }

    val date = args(0)

    val spark = SparkSession.builder()
      .appName("midu user installed apps " + date)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val sql =
      s"""
         |select a.uid, a.app_name
         |from
         |  (
         |    select uid, app_name
         |    from dl_cpc.cpc_user_installed_apps
         |    where load_date='$date'
         |  ) a
         |join
         |  ( select distinct uid
         |    from dl_cpc.cpc_basedata_union_events
         |    where day='$date' and media_appsid='80001098'
         |  ) b
         |on a.uid=b.uid
       """.stripMargin

    val pkgs = spark.sql(sql)
      .rdd
      .map(x => (x.getString(0), x.getAs[Seq[String]]("app_name")))
      .flatMap(_._2.map(x => (x, 1l)))
      .reduceByKey(_ + _)
      .filter(_._2 > 20000)
      .sortBy(x => x._2, false)


    pkgs.toDF("app_name", "install_user_num")
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/warehouse/dl_cpc.db/midu_top_apps/%s".format(date))

    spark.sql(
      """
        |ALTER TABLE dl_cpc.midu_top_apps add if not exists PARTITION(`date` = "%s")
        | LOCATION  '/warehouse/dl_cpc.db/midu_top_apps/%s'
      """.stripMargin.format(date, date))

    println("count: " + pkgs.count())


    val iterator = pkgs.toLocalIterator

    var txt = ""
    while (iterator.hasNext) {
      val t = iterator.next()
      txt = txt + "%s %s\n".format(t._1, t._2)
    }

    val b = sendMail(txt, "%s midu topApps[活跃用户DAU大于2w]".format(date), Seq("zhanghongyang@aiclk.com", "dongwei@aiclk.com",
       "sujiaqi@qutoutiao.net"))
    //val b = sendMail(txt, "%s topApps 活跃用户数top100".format(date), Seq("zhanghongyang@aiclk.com"))
    if (!b) {
      println("发送邮件失败")
    }
  }
}
