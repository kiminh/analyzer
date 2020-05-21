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
    val spark: SparkSession = SparkSession.builder().appName("[cpc-data] topapps" + date).enableHiveSupport().getOrCreate()
    import spark.implicits._
    val pkgs = spark.read.parquet("hdfs://inner-di-hdfs.1sapp.com/cpc1/dw/dl_cpc.db/cpc_user_installed_apps/%s".format(date)).rdd.cache()

    val allApps: RDD[(String, Long)] = pkgs.map(x => (x.getString(0), x.getAs[Seq[String]]("app_name"))).flatMap(_._2.map(x => (x, 1L))).reduceByKey(_ + _).filter(_._2 > 20000).sortBy(x => x._2, ascending = false)
    allApps.toDF("pkg", "install_user_num").write.mode(SaveMode.Overwrite).parquet("hdfs://emr-cluster/warehouse/dl_cpc.db/top_apps/%s".format(date))
    spark.sql("""ALTER TABLE dl_cpc.top_apps add if not exists PARTITION (`date` = "%s") """.format(date))

    val iterator: Iterator[(String, Long)] = allApps.toLocalIterator
    var txt = ""
    while (iterator.hasNext) {
      val t: (String, Long) = iterator.next()
      txt = txt + "%s %s\n".format(t._1, t._2)
    }

    val mailingList: Seq[String] = Seq(
      "zhangting@qutoutiao.net",
      "huxinjie@aiclk.com",
      "sujiaqi@qutoutiao.net",
      "weijinxian@qutoutiao.net",
      "yuxiaoyang@qutoutiao.net",
      "heting@qutoutiao.net",
      "liutianlin@qutoutiao.net",
      "baizhen@qutoutiao.net",
      "zhangxiaonian@qutoutiao.net",
      "zhangwei07@qutoutiao.net",
      "fanyiming@qutoutiao.net",
      "zhuqiqi@qutoutiao.net",
      "zhangfan03@qutoutiao.net",
      "shanshi@qutoutiao.net",
      "wangzheming@qutoutiao.net",
      "wangxinyuan@qutoutiao.net",
      "sushanshan@qutoutiao.net"
    )
    sendMail(txt, "%s topapps 活跃用户DAU [所有媒体]".format(date), mailingList)

    val qttApps: RDD[(String, Long)] = pkgs.map(x => (x.getString(0), x.getAs[Seq[String]]("app_name"))).filter(x => x._2.contains("com.jifen.qukan-趣头条")).flatMap(_._2.map(x => (x, 1l))).reduceByKey(_ + _).filter(_._2 > 20000).sortBy(x => x._2, ascending = false)
    val iteratorQtt: Iterator[(String, Long)] = qttApps.toLocalIterator
    var qtt_txt = ""
    while (iteratorQtt.hasNext) {
      val t: (String, Long) = iteratorQtt.next()
      qtt_txt = qtt_txt + "%s %s\n".format(t._1, t._2)
    }

    sendMail(qtt_txt, "%s topapps 活跃用户DAU [仅趣头条]".format(date), mailingList)
    pkgs.unpersist()
    spark.close()
  }
}

