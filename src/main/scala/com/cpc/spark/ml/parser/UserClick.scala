package com.cpc.spark.ml.parser

import com.cpc.spark.log.parser.UnionLog
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.Source

/**
  * Created by roydong on 2017/6/10.
  */
class UserClick(host: String, port: Int, db: Int) extends UserClickTrait {

  private val redis = new RedisClient(host, port)
  if (db > 0) {
    redis.select(db)
  }

  private var pv = 105791768L

  def getPv(): Long = {
    pv
  }

  def loadUserClickFromHdfs(ctx: SparkContext): Unit = {
    loadUserClickPV(ctx, "/user/cpc/user_click/user_click_pv.txt")
    loadUserAdClick(ctx, "/user/cpc/user_click/user_ad_click.txt")
    loadUserSlotClick(ctx, "/user/cpc/user_click/user_slot_click.txt")
    loadUserSlotAdClick(ctx, "/user/cpc/user_click/user_slot_ad_click.txt")
  }

  private def loadUserClickPV(ctx: SparkContext, path: String): Unit = {
    val txt = ctx.textFile(path)
    txt.toLocalIterator.foreach {
      line =>
        val row = line.split("\t")
        if (row.length == 3 && row(0).length > 5) {
          userClk.update(row(0), row(1).toInt)
          userPV.update(row(0), row(2).toInt)
        }
    }
  }

  private def loadUserAdClick(ctx: SparkContext, path: String): Unit = {
    val txt = ctx.textFile(path)
    txt.toLocalIterator.foreach {
      line =>
        val row = line.split("\t")
        if (row.length == 4 && row(0).length > 5) {
          userAdClick.update("%s-%s".format(row(0), row(1)), row(2).toInt)
        }
    }
  }

  private def loadUserSlotClick(ctx: SparkContext, path: String): Unit = {
    val txt = ctx.textFile(path)
    txt.toLocalIterator.foreach {
      line =>
        val row = line.split("\t")
        if (row.length == 4 && row(0).length > 5) {
          userSlotClick.update("%s-%s".format(row(0), row(1)), row(2).toInt)
        }
    }
  }

  private def loadUserSlotAdClick(ctx: SparkContext, path: String): Unit = {
    val txt = ctx.textFile(path)
    txt.toLocalIterator.foreach {
      line =>
        val row = line.split("\t")
        if (row.length == 5 && row(0).length > 5) {
          userSlotAdClick.update("%s-%s-%s".format(row(0), row(1), row(2)), row(3).toInt)
        }
    }
  }

  def sumUnionLog(ctx: SparkSession, date: String, hour: String): Int = {
    import ctx.implicits._

    var hourSql = ""
    if (hour.length > 0) {
      hourSql = "and hour = \"%s\"".format(hour)
    }
    val ulog = ctx.sql(
      s"""
         |select * from dl_cpc.cpc_union_log where `date` = "%s" %s and isfill = 1 and adslotid > 0
         |and media_appsid in ("80000001", "80000002")
        """.stripMargin.format(date, hour))
      .as[UnionLog].rdd
      .filter(u => u != null && u.searchid.length > 0 && u.uid.length > 5)
      .cache()

    pv += ulog.count()

    //uid click
    var n = 0
    ulog.map(u => (u.uid, (u.isclick, 1)))
      .reduceByKey {
        (x, y) =>
          (x._1 + y._1, x._2 + y._2)
      }
      .filter(_._2._1 > 0)
      .toLocalIterator
      .foreach {
        x =>
          val sum = x._2
          val v1 = userClk.getOrElse(x._1, 0)
          val v2 = userPV.getOrElse(x._1, 0)
          userClk.update(x._1, v1 + sum._1)
          userPV.update(x._1, v2 + sum._2)
          n += 1
      }

    println("click", userClk.size, userPV.size)

    //uid ad click
    ulog.map(u => ((u.uid, u.ideaid), (u.isclick, 1)))
      .reduceByKey {
        (x, y) =>
          (x._1 + y._1, x._2 + y._2)
      }
      .filter(_._2._1 > 0)
      .toLocalIterator
      .foreach {
        x =>
          val u = x._1
          val sum = x._2
          val k = "%s-%d".format(u._1, u._2)
          val v = userAdClick.getOrElse(k, 0)
          userAdClick.update(k, v + sum._1)
          n += 1
      }

    println("ad click", userAdClick.size)

    //uid adslot click
    ulog.map(u => ((u.uid, u.adslotid), (u.isclick, 1)))
      .reduceByKey {
        (x, y) =>
          (x._1 + y._1, x._2 + y._2)
      }
      .filter(_._2._1 > 0)
      .toLocalIterator
      .foreach {
        x =>
          val u = x._1
          val sum = x._2
          val k = "%s-%s".format(u._1, u._2)
          val v = userSlotClick.getOrElse(k, 0)
          userSlotClick.update(k, v + sum._1)
          n += 1
      }

    println("slot click", userSlotClick.size)

    //uid adslot ad click
    ulog.map(u => ((u.uid, u.adslotid, u.ideaid), (u.isclick, 1)))
      .reduceByKey {
        (x, y) =>
          (x._1 + y._1, x._2 + y._2)
      }
      .filter(_._2._1 > 0)
      .toLocalIterator
      .foreach {
        x =>
          val u = x._1
          val sum = x._2
          val k = "%s-%s-%d".format(u._1, u._2, u._3)
          val v = userSlotAdClick.getOrElse(k, 0)
          userSlotAdClick.update(k, v + sum._1)
          n += 1
      }

    println("slot ad click", userSlotAdClick.size)

    ulog.unpersist()
    n
  }

  def clearData(): Unit = {
    userClk.clear()
    userPV.clear()
    userAdClick.clear()
    userSlotClick.clear()
    userSlotAdClick.clear()
    pv = 0
  }

  def saveToRedis(): Unit = {
    redis.hmset("user_click", userClk)
    redis.hmset("user_pv", userPV)
    redis.hmset("user_ad_click", userAdClick)
    redis.hmset("user_slot_click", userSlotClick)
    redis.hmset("user_slot_ad_click", userSlotAdClick)
    redis.set("user_click_total_pv", pv)
  }

  def readFromRedis(): Unit = {
    clearData()

    redis.hgetall1[String, Int]("user_click").getOrElse(userClk)
      .foreach{case (k, v) => userClk.update(k, v)}
    redis.hgetall1[String, Int]("user_pv").getOrElse(userPV)
      .foreach{case (k, v) => userPV.update(k, v)}
    redis.hgetall1[String, Int]("user_ad_click").getOrElse(userAdClick)
      .foreach{case (k, v) => userAdClick.update(k, v)}
    redis.hgetall1[String, Int]("user_slot_click").getOrElse(userSlotClick)
      .foreach{case (k, v) => userSlotClick.update(k, v)}
    redis.hgetall1[String, Int]("user_slot_ad_click").getOrElse(userSlotAdClick)
      .foreach{case (k, v) => userSlotAdClick.update(k, v)}

    println("read from redis", userClk.size, userSlotAdClick.size)
  }

  def userCtr(uid: String): Double = {
    val v = userClk.getOrElse(uid, 0)
    if (pv > 0) {
      v.toDouble / pv.toDouble
    } else {
      0
    }
  }

  def userAdCtr(uid: String, adid: Int): Double = {
    val v = userAdClick.getOrElse("%s-%d".format(uid, adid), 0)
    if (pv > 0) {
      v.toDouble / pv.toDouble
    } else {
      0
    }
  }

  def userSlotCtr(uid: String, slotid: Int): Double = {
    val v = userSlotClick.getOrElse("%s-%d".format(uid, slotid), 0)
    if (pv > 0) {
      v.toDouble / pv.toDouble
    } else {
      0
    }
  }

  def userSlotAdCtr(uid: String, slotid: Int, adid: Int): Double = {
    val v = userSlotAdClick.getOrElse("%s-%d-%d".format(uid, slotid, adid), 0)
    if (pv > 0) {
      v.toDouble / pv.toDouble
    } else {
      0
    }
  }
}


trait UserClickTrait {

  val userClk = mutable.Map[String, Int]()

  val userPV = mutable.Map[String, Int]()

  val userAdClick = mutable.Map[String, Int]()

  val userSlotClick = mutable.Map[String, Int]()

  val userSlotAdClick = mutable.Map[String, Int]()

  def loadUserClickFromFile(): Unit = {
    loadUserClickPV("/home/cpc/t/user_click/user_click_pv.txt")
    loadUserAdClick("/home/cpc/t/user_click/user_ad_click.txt")
    loadUserSlotClick("/home/cpc/t/user_click/user_slot_click.txt")
    loadUserSlotAdClick("/home/cpc/t/user_click/user_slot_ad_click.txt")
  }

  def loadUserClickPV(path: String): Unit = {
    for (line <- Source.fromFile(path, "UTF8").getLines()) {
      val row = line.split("\t")
      if (row.length == 3 && row(0).length > 5) {
        userClk.update(row(0), row(1).toInt)
        userPV.update(row(0), row(2).toInt)
      }
    }
  }

  def loadUserAdClick(path: String): Unit = {
    for (line <- Source.fromFile(path, "UTF8").getLines()) {
      val row = line.split("\t")
      if (row.length == 4 && row(0).length > 5) {
        userAdClick.update("%s-%s".format(row(0), row(1)), row(2).toInt)
      }
    }
  }

  def loadUserSlotClick(path: String): Unit = {
    for (line <- Source.fromFile(path, "UTF8").getLines()) {
      val row = line.split("\t")
      if (row.length == 4 && row(0).length > 5) {
        userSlotClick.update("%s-%s".format(row(0), row(1)), row(2).toInt)
      }
    }
  }

  def loadUserSlotAdClick(path: String): Unit = {
    for (line <- Source.fromFile(path, "UTF8").getLines()) {
      val row = line.split("\t")
      if (row.length == 5 && row(0).length > 5) {
        userSlotAdClick.update("%s-%s-%s".format(row(0), row(1), row(2)), row(3).toInt)
      }
    }
  }

}
