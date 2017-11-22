package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.Random

/**
  * Created by Roy on 2017/5/18.
  *
  */
object ConditionTouchedUV {

  var redis: RedisClient = _

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetUserProfile <date:string>
           |
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).trim
    val rate = args(1).toFloat
    val conf = ConfigFactory.load()
    redis = new RedisClient(conf.getString("touched_uv.redis.host"), conf.getInt("touched_uv.redis.port"))
    redis.select(3)
    val ctx = SparkSession.builder()
      .appName("cpc anal condition touched uv from %s".format(date))
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._
    var ulog = ctx.sql(
      "select * from dl_cpc.cpc_union_log where `date` = \"%s\" and media_appsid in (\"80000001\", \"80000002\")"
        .format(date))
      .as[UnionLog].rdd

    val uv = ulog.map(x => (x.uid, 1)).reduceByKey((x, y) => x).count()
    redis.set("touched_uv_total", uv)
    println("uv", uv)
    ulog = ulog.randomSplit(Array(rate, 1-rate), new Date().getTime)(0)

    ulog.cache()
    calcCondPercent("province", ulog.filter(_.province > 0).map(u => ((u.province, u.uid), 1)))
    calcCondPercent("city", ulog.filter(_.city > 0).map(u => ((u.city, u.uid), 1)))
    calcCondPercent("sex", ulog.filter(_.sex > 0).map(u => ((u.sex, u.uid), 1)))
    calcCondPercent("age", ulog.filter(_.age > 0).map(u => ((u.age, u.uid), 1)))
    calcCondPercent("coin", ulog.filter(_.coin > 0)
      .map {
        u =>
          val coin = u.ext.getOrElse("share_coin", ExtValue()).int_value
          var lvl = 0
          if (coin == 0) {
            lvl = 1
          } else if (coin <= 60) {
            lvl = 2
          } else if (coin <= 90) {
            lvl = 3
          } else {
            lvl = 4
          }
          ((lvl, u.uid), 1)})

    calcCondPercent("os", ulog.filter(_.os > 0).map(u => ((u.os, u.uid), 1)))
    calcCondPercent("net", ulog.filter(_.network > 0).map(u => ((u.network, u.uid), 1)))
    calcCondPercent("phone_level", ulog.map(u => (u.ext.getOrElse("phone_level", ExtValue()).int_value, u))
      .filter(_._1 > 0)
      .map(x => ((x._1, x._2.uid), 1)))

    calcCondPercent("interest", ulog
      .flatMap{
        x =>
          x.interests.split(",")
            .map(i => i.split("="))
            .filter(x => x.length == 2 && x(0).toInt > 0)
            .map(i => ((i(0).toInt, x.uid), 1))
      })

    calcCondPercent("user_type", ulog
      .flatMap{
        x =>
          x.interests.split(",")
            .map(i => i.split("="))
            .filter(x => x.length == 2 && x(0).toInt > 0)
            .map(i => ((i(0).toInt, x.uid), 1))
      })

    println("done")
    ctx.stop()
  }

  def calcCondPercent(name: String, ulog: RDD[((Int, String), Int)]): Unit = {
    val cond = ulog.reduceByKey((x, y) => x)
      .map(x => (x._1._1, 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2))
    val sum = cond.map(x => x._2).sum()
    if (name == "coin") {
      val coins = cond.toLocalIterator.toSeq.sortWith((x, y) => x._1 < y._1)
      var n = 0d
      coins.foreach {
        case (i, v) =>
          val key = "touched_uv_percent_%s_%d".format(name, i)
          n = n + v
          val p = n / sum
          redis.set(key, "%.8f".format(p))
          println(name, key, n, "%.3f".format(p))
      }
    } else {
      cond.toLocalIterator
        .foreach {
          x =>
            val p = x._2 / sum
            val key = "touched_uv_percent_%s_%d".format(name, x._1)
            redis.set(key, "%.8f".format(p))
            println(name, x._1, x._2, "%.3f".format(p))
        }
    }
  }
}


