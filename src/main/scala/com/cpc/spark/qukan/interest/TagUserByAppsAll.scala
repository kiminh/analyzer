package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.qukan.parser.HdfsParser
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{InterestItem, UserProfile}


/**
  * Created by roydong on 04/12/2017.
  */
object TagUserByAppsAll {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: Tag user by installed apps <dayBefore int>
           |
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -args(0).toInt)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val spark = SparkSession.builder()
      .appName("tag user by installed apps [%s]".format(day))
      .getOrCreate()

    var conf = ConfigFactory.load("interest")

    println("-----pkg usage-----", day)
    //user app install info
    val aiPath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(day)
    val userPkgs = spark.read.orc(aiPath).rdd
      .map(HdfsParser.parseInstallApp(_, x => true, null))
      .filter(x => x != null && x.pkgs.length > 0)
      .map(x => (x.devid, x.pkgs.map(_.name)))
      .reduceByKey(_ ++ _) //devid,List
      .map(x => x._2.distinct)
      .flatMap(x => x.map(v => (v, 1)))
      .reduceByKey(_+_)
      .sortBy(_._2,false)
      .take(1000)

    userPkgs.foreach(println)

    /*


        //借贷
        val loanUids = uidHasApps(userPkgs, "interest", "user_tag_by_apps.loans.contains")
        var sum = tagUser(loanUids, conf.getInt("user_tag_by_apps.loans.tag_id"))
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

        //保健品
        val baojianpinUids = uidHasApps(userPkgs, "interest", "user_tag_by_apps.baojianpin.contains")
        sum = tagUser(baojianpinUids, conf.getInt("user_tag_by_apps.baojianpin.tag_id"))
        println("has baojianpin app users", sum)
        baojianpinUids.take(10).foreach(println)

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
    */

  }

  def uidHasNoApps(userPkgs: RDD[(String, List[String])], confPath: String, pkg: String): RDD[String] = {
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
      .map(x => (x._1, pkgs.contains(x._2))).filter(x => x._2).map(x => (x._1, 1))
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
    val conf = ConfigFactory.load()
    val sum = uids
      .mapPartitions {
        p =>
          var n = 0
          var n1 = 0
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.foreach {
            uid =>
              val key = uid + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).orNull
              if (buffer != null) {
                val user = UserProfile.parseFrom(buffer).toBuilder
                val in = InterestItem.newBuilder()
                  .setTag(tag)
                  .setScore(100)
                var has = false
                for (i <- 0 until user.getInterestedWordsCount) {
                  val w = user.getInterestedWords(i)
                  if (w.getTag == in.getTag) {
                    if (!has) {
                      user.setInterestedWords(i, in)
                      has = true
                    } else {
                      user.removeInterestedWords(i)
                    }
                  }
                }
                if (!has) {
                  user.addInterestedWords(in)
                  n1 = n1 + 1
                }
                n = n + 1
                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
          }
          Seq((n, n1)).iterator
      }
    sum.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
  }
}



