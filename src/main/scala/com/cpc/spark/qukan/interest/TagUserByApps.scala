package com.cpc.spark.qukan.interest

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils
import com.cpc.spark.qukan.parser.HdfsParser
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import userprofile.Userprofile.{InterestItem, UserProfile}


/**
  * Created by roydong on 04/12/2017.
  */
object TagUserByApps {

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
      .getOrCreate()

    val conf = ConfigFactory.load("interest")

    println("-----pkg usage-----", day)
    //user app install info
    val aiPath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(day)
    val userPkgs = spark.read.orc(aiPath).rdd
      .map(HdfsParser.parseInstallApp(_, x => true, null))
      .filter(x => x != null && x.pkgs.length > 0)
      .map(x => (x.devid, x.pkgs.map(_.name)))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, x._2.distinct))
      .cache()

    println("users", userPkgs.count())

    //借贷
    val loanUids = uidHasApps(userPkgs, "user_tag_by_apps.loans.contains")
    var sum = tagUser(loanUids, conf.getInt("user_tag_by_apps.loans.tag_id"))
    println("has loans app users", sum)
    loanUids.take(10).foreach(println)

    val finUids = uidHasApps(userPkgs, "user_tag_by_apps.invest.contains")
    sum = tagUser(finUids, conf.getInt("user_tag_by_apps.invest.tag_id"))
    println("has invest app users", sum)
    finUids.take(10).foreach(println)

    //二类电商
    val eshopUids = uidHasNoApps(userPkgs, "user_tag_by_apps.sec_eshops.excludes")
    sum = tagUser(eshopUids, conf.getInt("user_tag_by_apps.sec_eshops.tag_id"))
    println("no eshop users", sum)
    eshopUids.take(10).foreach(println)

    //非支付宝用户
    val alipayUids = uidHasNoApps(userPkgs, "user_tag_by_apps.no_alipay.excludes")
    sum = tagUser(alipayUids, conf.getInt("user_tag_by_apps.no_alipay.tag_id"))
    println("no alipay users", sum)
    alipayUids.take(10).foreach(println)

    //棋牌
    val qipaiUids = uidHasApps(userPkgs, "user_tag_by_apps.qipai.contains")
    sum = tagUser(qipaiUids, conf.getInt("user_tag_by_apps.qipai.tag_id"))
    println("has qipai app users", sum)

    userPkgs.unpersist()
  }

  def uidHasNoApps(userPkgs: RDD[(String, List[String])], pkg: String): RDD[String] = {
    checkHasApps(userPkgs, pkg).filter(_._2 == false).map(_._1)
  }

  def uidHasApps(userPkgs: RDD[(String, List[String])], pkg: String): RDD[String] = {
    checkHasApps(userPkgs, pkg).filter(_._2).map(_._1)
  }

  def checkHasApps(userPkgs: RDD[(String, List[String])], pkg: String): RDD[(String, Boolean)] = {
    val conf = ConfigFactory.load("interest")
    val pconf = conf.getConfigList(pkg)
    var pkgs = Seq[String]()
    for (i <- 0 until pconf.size()) {
      val c = pconf.get(i)
      pkgs = pkgs :+ c.getString("pkg")
    }
    println("has pkg names", pkgs)
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



