package com.cpc.spark.log.anal

import java.util.Date

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by Roy on 2017/5/18.
  * fym 190428: a remedy.
  */
object ConditionTouchedUVByOS {

  var redis: RedisClient = _


  var interestids = Seq(110, 125, 201, 202, 203, 204, 205, 206, 207, 208, 209)



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
      .appName("cpc anal condition touched uv by city from %s".format(date))
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._
    var ulog = ctx.sql(
      "select *, share_coin, phone_level from dl_cpc.cpc_basedata_union_events where day = \"%s\" and media_appsid in (\"80000001\", \"80000002\") and isshow=1 and ideaid>0"
        .format(date))
      //      .as[UnionLog]
      .rdd

    calcCondPercent("os", ulog.filter(_.getAs[Int]("os") > 0).map(u => ((u.getAs[Int]("os"), u.getAs[String]("uid")), 1)))

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