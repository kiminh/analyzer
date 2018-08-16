package com.cpc.spark.qukan.interest

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils
import com.cpc.spark.qukan.parser.HdfsParser
import com.cpc.spark.qukan.userprofile.SetUserProfileTag
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import userprofile.Userprofile.{InterestItem, UserProfile}


/**
  * Created by roydong on 04/12/2017.
  */
object TagUserByApps {

  Logger.getRootLogger.setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: Tag user by installed apps <dayBefore int>
           |
        """.stripMargin)
      System.exit(1)
    }

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -args(0).toInt)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val spark = SparkSession.builder()
      .appName("tag user by installed apps [%s]".format(day))
      .enableHiveSupport()
      .getOrCreate()

    var conf = ConfigFactory.load("interest")

    println("-----pkg usage-----", day)
    val aiPath = "/user/cpc/userInstalledApp/%s".format(day)
    val userPkgs = spark.read.parquet(aiPath).rdd
    .map{ x =>
      (x.getAs[String]("uid"), x.getAs[Seq[String]]("pkgs").toList)
    }
    println("users", userPkgs.count())

    //保健品
    val clicklog = spark.sql(
      """
        |select distinct uid from dl_cpc.cpc_union_log where `date` = "%s"
        |and round(ext['adclass'].int_value / 1e3,0) = %d and isclick = 1
      """.stripMargin.format(day, 130104)).rdd
      .map { u => u.getString(0)}
    val baojianpinUids = uidHasApps(userPkgs, "interest", "user_tag_by_apps.baojianpin.contains")
      .union(clicklog)
      .distinct()
    var sum = tagUser(baojianpinUids, conf.getInt("user_tag_by_apps.baojianpin.tag_id"))
    println("has baojianpin app users", sum)
    baojianpinUids.take(10).foreach(println)


    //借贷
    val loanUids = uidHasApps(userPkgs, "interest", "user_tag_by_apps.loans.contains")
    sum = tagUser(loanUids, conf.getInt("user_tag_by_apps.loans.tag_id"))
    println("has loans app users", sum)
    loanUids.take(10).foreach(println)

    val finUids = uidHasApps(userPkgs, "interest", "user_tag_by_apps.invest.contains")
    sum = tagUser(finUids, conf.getInt("user_tag_by_apps.invest.tag_id"))
    println("has invest app users", sum)
    finUids.take(10).foreach(println)

    //二类电商
    val eshopUids = uidHasNoApps(userPkgs, "interest", "user_tag_by_apps.sec_eshops.excludes")
    sum = tagUser(eshopUids, conf.getInt("user_tag_by_apps.sec_eshops.tag_id"))
    println("no eshop users", sum)
    eshopUids.take(10).foreach(println)

    //非支付宝用户
    val alipayUids = uidHasNoApps(userPkgs, "interest", "user_tag_by_apps.no_alipay.excludes")
    sum = tagUser(alipayUids, conf.getInt("user_tag_by_apps.no_alipay.tag_id"))
    println("no alipay users", sum)
    alipayUids.take(10).foreach(println)

    //棋牌
    val qipaiUids = uidHasApps(userPkgs, "interest", "user_tag_by_apps.qipai.contains")
    sum = tagUser(qipaiUids, conf.getInt("user_tag_by_apps.qipai.tag_id"))
    println("has qipai app users", sum)
    qipaiUids.take(10).foreach(println)

    //医疗
    val yiliaoUids = uidHasApps(userPkgs, "interest", "user_tag_by_apps.yiliao.contains")
    sum = tagUser(yiliaoUids, conf.getInt("user_tag_by_apps.yiliao.tag_id"))
    println("has yiliao app users", sum)
    yiliaoUids.take(10).foreach(println)

    //腾讯QQ
    val qqUids = uidHasApps(userPkgs, "interest", "user_tag_by_apps.mobileqq.contains")
    sum = tagUser(qqUids, conf.getInt("user_tag_by_apps.mobileqq.tag_id"))
    println("has mobileqq app users", sum)
    qqUids.take(10).foreach(println)

    //唯品会
    val vipUids = uidHasApps(userPkgs, "interest", "user_tag_by_apps.vipshop.contains")
    sum = tagUser(vipUids, conf.getInt("user_tag_by_apps.vipshop.tag_id"))
    println("has vipshop app users", sum)
    vipUids.take(10).foreach(println)

    //微博
    val weiboUids = uidHasApps(userPkgs, "interest", "user_tag_by_apps.weibo.contains")
    sum = tagUser(weiboUids, conf.getInt("user_tag_by_apps.weibo.tag_id"))
    println("has weibo app users", sum)
    weiboUids.take(10).foreach(println)

/*
    //格斗游戏
    val game_gedou = uidHasApps(userPkgs, "user_tag_by_apps.game_gedou.contains")
    var sum = tagUser(game_gedou, conf.getInt("user_tag_by_apps.game_gedou.tag_id"))
    println("has game_gedou app users", sum)
    game_gedou.take(10).foreach(println)

    //棋牌游戏
    val game_qipai = uidHasApps(userPkgs, "user_tag_by_apps.game_qipai.contains")
    sum = tagUser(game_qipai, conf.getInt("user_tag_by_apps.game_qipai.tag_id"))
    println("has game_qipai app users", sum)
    game_qipai.take(10).foreach(println)
    */
    //益智游戏
    val game_yizhi = uidHasApps(userPkgs, "interest", "user_tag_by_apps.game_yizhi.contains")
    sum = tagUser(game_yizhi, conf.getInt("user_tag_by_apps.game_yizhi.tag_id"))
    println("has game_yizhi app users", sum)
    game_yizhi.take(10).foreach(println)
/*
    //模拟游戏
    val game_moni = uidHasApps(userPkgs, "user_tag_by_apps.game_moni.contains")
    sum = tagUser(game_moni, conf.getInt("user_tag_by_apps.game_moni.tag_id"))
    println("has game_moni app users", sum)
    game_moni.take(10).foreach(println)

    //赛车体育游戏
    val game_tiyu = uidHasApps(userPkgs, "user_tag_by_apps.game_tiyu.contains")
    sum = tagUser(game_tiyu, conf.getInt("user_tag_by_apps.game_tiyu.tag_id"))
    println("has game_tiyu app users", sum)
    game_tiyu.take(10).foreach(println)

    //休闲游戏
    val game_xiuxian = uidHasApps(userPkgs, "user_tag_by_apps.game_xiuxian.contains")
    sum = tagUser(game_xiuxian, conf.getInt("user_tag_by_apps.game_xiuxian.tag_id"))
    println("has game_xiuxian app users", sum)
    game_xiuxian.take(10).foreach(println)
*/
    //策略游戏
    val game_celue = uidHasApps(userPkgs, "interest", "user_tag_by_apps.game_celue.contains")
    sum = tagUser(game_celue, conf.getInt("user_tag_by_apps.game_celue.tag_id"))
    println("has game_celue app users", sum)
    game_celue.take(10).foreach(println)
/*
    //mmorpg
    val game_mmorpg = uidHasApps(userPkgs, "user_tag_by_apps.game_mmorpg.contains")
    sum = tagUser(game_mmorpg, conf.getInt("user_tag_by_apps.game_mmorpg.tag_id"))
    println("has game_mmorpg app users", sum)
    game_mmorpg.take(10).foreach(println)
*/
    //团队合作游戏
    val game_tuandui = uidHasApps(userPkgs, "interest", "user_tag_by_apps.game_tuandui.contains")
    sum = tagUser(game_tuandui, conf.getInt("user_tag_by_apps.game_tuandui.tag_id"))
    println("has game_tuandui app users", sum)
    game_tuandui.take(10).foreach(println)

    //京东
    val jingdong = uidHasApps(userPkgs, "interest", "user_tag_by_apps.jingdong.contains")
    sum = tagUser(jingdong, conf.getInt("user_tag_by_apps.jingdong.tag_id"))
    println("has jingdong users", sum)
    jingdong.take(10).foreach(println)

    //淘宝和天猫
    val taobao_and_tianmao = uidHasApps(userPkgs, "interest", "user_tag_by_apps.taobao_and_tianmao.contains")
    sum = tagUser(taobao_and_tianmao, conf.getInt("user_tag_by_apps.taobao_and_tianmao.tag_id"))
    println("has taobao_and_tianmao users", sum)
    taobao_and_tianmao.take(10).foreach(println)

    //拼多多
    val pingduoduo = uidHasApps(userPkgs, "interest", "user_tag_by_apps.pingduoduo.contains")
    sum = tagUser(pingduoduo, conf.getInt("user_tag_by_apps.pingduoduo.tag_id"))
    println("has pingduoduo users", sum)
    pingduoduo.take(10).foreach(println)

    //手机银行
    conf = ConfigFactory.load("people/shoujiyinhang")
    val shoujiyinhangUids = uidHasApps(userPkgs,"people/shoujiyinhang","user_tag_by_apps.shoujiyinhang.contains")
    sum = tagUser(shoujiyinhangUids, conf.getInt("user_tag_by_apps.shoujiyinhang.tag_id"))
    println("has shoujiyinhang app users", sum)
    shoujiyinhangUids.take(10).foreach(println)

    //信用卡
    conf = ConfigFactory.load("people/xinyongka")
    val xinyongkaUids = uidHasApps(userPkgs, "people/xinyongka", "user_tag_by_apps.xinyongka.contains")
    sum = tagUser(xinyongkaUids, conf.getInt("user_tag_by_apps.xinyongka.tag_id"))
    println("has xinyongka app users", sum)
    xinyongkaUids.take(10).foreach(println)

    //汽车
    conf = ConfigFactory.load("people/qiche")
    val qicheUids = uidHasApps(userPkgs, "people/qiche", "user_tag_by_apps.qiche.contains")
    sum = tagUser(qicheUids, conf.getInt("user_tag_by_apps.qiche.tag_id"))
    println("has qiche app users", sum)
    qicheUids.take(10).foreach(println)


    //    val shoujiyinhangUids = getDistribution(userPkgs, "user_tag_by_apps.shoujiyinhang.contains")
    //    println("手机银行")
    //    println("只安装了1个app的用户数:%d".format(shoujiyinhangUids.filter(x=>x._2==1).count))
    //    println("只安装了2个app的用户数:%d".format(shoujiyinhangUids.filter(x=>x._2==2).count))
    //    println("只安装了3个app的用户数:%d".format(shoujiyinhangUids.filter(x=>x._2==3).count))
    //    println("只安装了4个app的用户数:%d".format(shoujiyinhangUids.filter(x=>x._2==4).count))
    //    println("安装超过4个app的用户数:%d".format(shoujiyinhangUids.filter(x=>x._2> 4).count))
    //    println("---------")
    //    println("信用卡")
    //    val xinyongka = getDistribution(userPkgs, "user_tag_by_apps.xinyongka.contains")
    //    println("只安装了1个app的用户数:%d".format(xinyongka.filter(x=>x._2==1).count))
    //    println("只安装了2个app的用户数:%d".format(xinyongka.filter(x=>x._2==2).count))
    //    println("只安装了3个app的用户数:%d".format(xinyongka.filter(x=>x._2==3).count))
    //    println("只安装了4个app的用户数:%d".format(xinyongka.filter(x=>x._2==4).count))
    //    println("安装超过4个app的用户数:%d".format(xinyongka.filter(x=>x._2> 4).count))

    userPkgs.unpersist()
  }

  def uidHasNoApps(userPkgs: RDD[(String, List[String])],confPath: String, pkg: String): RDD[String] = {
    checkHasApps(userPkgs, confPath, pkg).filter(_._2 == false).map(_._1)
  }

  def getDistribution(userPkgs: RDD[(String, List[String])], confPath: String, pkg: String): RDD[(String, Int)] = {
    val conf = ConfigFactory.load(confPath)
    val pconf = conf.getConfigList(pkg)
    var pkgs = Seq[String]()
    for (i <- 0 until pconf.size()) {
      val c = pconf.get(i)
      pkgs = pkgs :+ c.getString("pkg")
    }
    //println("has pkg names", pkgs)
    userPkgs.flatMap(x => x._2.map(v => (x._1, v)))
      .map(x => (x._1, pkgs.contains(x._2))).filter(x=>x._2).map(x=>(x._1,1))
      .reduceByKey(_ + _)
  }

  def uidHasApps(userPkgs: RDD[(String, List[String])], confPath: String, pkg: String): RDD[String] = {
    checkHasApps(userPkgs, confPath, pkg).filter(_._2).map(_._1)
  }

  def checkHasApps(userPkgs: RDD[(String, List[String])], confPath: String, pkg: String): RDD[(String, Boolean)] = {
    val conf = ConfigFactory.load(confPath)
    val pconf = conf.getConfigList(pkg)
    var pkgs = Seq[String]()
    for (i <- 0 until pconf.size()) {
      val c = pconf.get(i)
      pkgs = pkgs :+ c.getString("pkg")
    }
    //println("has pkg names", pkgs)
    userPkgs.flatMap(x => x._2.map(v => (x._1, v)))
      .map(x => (x._1, pkgs.contains(x._2)))
      .reduceByKey(_ || _)
  }

  def tagUser(uids: RDD[String], tag: Int): (Int, Int) = {
    val sum = SetUserProfileTag.setUserProfileTag(uids.map(x => (x, tag, true)))
    sum.foreach(println)
    (0, 0)
  }
}



