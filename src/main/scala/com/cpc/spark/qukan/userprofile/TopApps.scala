package com.cpc.spark.qukan.userprofile

import com.cpc.spark.common.Utils.sendMail
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by roydong on 12/07/2018.
  */
object TopApps {

  def main(args: Array[String]): Unit = {

    val date: String = args(0)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("save user installed apps " + date)
      .enableHiveSupport().getOrCreate()

    val inpath = "hdfs://emr-cluster/user/cpc/userInstalledApp/%s".format(date)

    import spark.implicits._
    val pkgs = spark.read.parquet(inpath).rdd.cache()

    //1. 所有媒体活跃用户DAU大于2w 的app
    val allApps: RDD[(String, Long)] = pkgs
      .map(x => (x.getString(0), x.getAs[Seq[String]]("app_name")))
      .flatMap(_._2.map(x => (x, 1l)))
      .reduceByKey(_ + _)
      .filter(_._2 > 20000)
      .sortBy(x => x._2, false)

    allApps.toDF("pkg", "install_user_num")
      .write
      .mode(SaveMode.Overwrite)
      .parquet("hdfs://emr-cluster/warehouse/dl_cpc.db/top_apps/%s".format(date))

    spark.sql(
      """
        |ALTER TABLE dl_cpc.top_apps add if not exists PARTITION(`date` = "%s")
        | LOCATION  'hdfs://emr-cluster/warehouse/dl_cpc.db/top_apps/%s'
      """.stripMargin.format(date, date))
    println("all Apps count: " + allApps.count())

    val iterator: Iterator[(String, Long)] = allApps.toLocalIterator
    var txt = ""
    while (iterator.hasNext) {
      val t = iterator.next()
      txt = txt + "%s %s\n".format(t._1, t._2)
    }

    val mailingList: Seq[String] = Seq(
      "dongwei@aiclk.com",
      "zhangting@qutoutiao.net",
      "huxinjie@aiclk.com",
      "sujiaqi@qutoutiao.net",
      "weijinxian@qutoutiao.net",
      "yuxiaoyang@qutoutiao.net",
      "heting@qutoutiao.net",
      "liutianlin@qutoutiao.net",
      "baizhen@qutoutiao.net",
      "zhangzhiyang@qutoutiao.net",
      "duruiyu@qutoutiao.net",
      "chenge@qutoutiao.net",
      "zhangxiaonian@qutoutiao.net",
      "zhangwei07@qutoutiao.net",
      "fanyiming@qutoutiao.net",
      "zhuqiqi@qutoutiao.net",
      "jiangxue@qutoutiao.net",
      "zhangfan03@qutoutiao.net",
      "shanshi@qutoutiao.net",
      "zhangbowen@qutoutiao.net"
    )

    val b: Boolean = sendMail(txt, "%s topApps 活跃用户DAU[所有媒体]".format(date), mailingList)

    //2. 安装qtt活跃用户DAU大于2w的app
    val qttApps = pkgs
      .map(x => (x.getString(0), x.getAs[Seq[String]]("app_name")))
      .filter(x => x._2.contains("com.jifen.qukan-趣头条"))
      .flatMap(_._2.map(x => (x, 1l)))
      .reduceByKey(_ + _)
      .filter(_._2 > 20000)
      .sortBy(x => x._2, false)

    val iteratorQtt: Iterator[(String, Long)] = qttApps.toLocalIterator
    var qtt_txt = ""
    while (iteratorQtt.hasNext) {
      val t = iteratorQtt.next()
      qtt_txt = qtt_txt + "%s %s\n".format(t._1, t._2)
    }

    val qtt: Boolean = sendMail(qtt_txt, "%s topApps 活跃用户DAU[仅趣头条]".format(date), mailingList)
    pkgs.unpersist()
    spark.close()

  }
}

