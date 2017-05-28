package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.{LogParser, UnionLog}
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.util.Random

/**
  * Created by Roy on 2017/5/18.
  *
  */
object AnalTouchedUV {

  var redis: RedisClient = null

  val provinces = Seq(
    0, 4, 8, 18, 12, 3, 29,
    6, 34, 1, 16, 20, 10, 26,
    17, 11, 19, 22, 23, 13, 14,
    21, 30, 7, 28, 2, 27, 31, 25,
    32, 9, 15, 5, 33, 24)

  val sex = Seq(0, 1, 2)

  val age = Seq(0, 1, 2, 3, 4, 5, 6)

  val coin = Seq(0, 1, 2, 3, 4)

  val os = Seq(0, 1, 2, 3)

  val net = Seq(0, 1, 2, 3, 4)

  val provinces1 = Seq(
    4, 8, 18, 12, 3, 29,
    6, 34, 1, 16, 20, 10, 26,
    17, 11, 19, 22, 23, 13, 14,
    21, 30, 7, 28, 2, 27, 31, 25,
    32, 9, 15, 5, 33, 24)

  val sex1 = Seq(1, 2)

  val age1 = Seq(1, 2, 3, 4, 5, 6)

  val coin1 = Seq(1, 2, 3, 4)

  //4 为映射原始0的数据
  val os1 = Seq(1, 2, 3)

  //5 为映射原始0的数据
  val net1 = Seq(1, 2, 3, 4)

  val allCols = Seq(provinces1, sex1, age1, coin1, os1, net1)

  /*统计维度
  地域
  性别   暂时通过评价随机
  年龄   暂时通过平均随机
  人群分类
  操作系统
  网络环境
  投放时间
   */

  var m = Seq[Seq[Int]]()

  def mapZeroCol(cols: mutable.Seq[Int], n: Int): Unit = {
    if (n >= 6) {
      m :+= cols
    } else if (cols(n) == 0) {
      for (v <- allCols(n)) {
        mapZeroCol(cols.updated(n, v), n + 1)
      }
    } else {
      mapZeroCol(cols, n + 1)
    }
  }

  def sumZeroValues(m: Seq[Seq[Int]]): Int = {
    var sum = 0
    for (cols <- m) {
      val key = cols.mkString("-") + "_TOUCHEDUV"
      val v = redis.get[Int](key).getOrElse(0)
      if (v > 0) {
        sum += v
      }
    }
    sum
  }

  def sumColsWithZero(): Unit = {
    for (p <- provinces) {
      for (s <- sex) {
        for (a <- age) {
          for (c <- coin) {
            for (o <- os) {
              for (n <- net) {
                val cols = mutable.Seq(p, s, a, c, o, n)
                if (cols.contains(0)) {
                  m = Seq[Seq[Int]]()
                  mapZeroCol(cols, 0)
                  val v = sumZeroValues(m)
                  if (v > 0) {
                    redis.set(cols.mkString("-") + "_TOUCHEDUV", v)
                    println(cols.mkString("-") + "_TOUCHEDUV", v)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetUserProfile <
           |
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    val dayBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val conf = ConfigFactory.load()
    redis = new RedisClient(conf.getString("touched_uv.redis.host"), conf.getInt("touched_uv.redis.port"))

    val ctx = SparkSession.builder()
      .appName("anal ad touched uv[%s]".format(date))
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    if (args(1).toBoolean) {
      val log = ctx.sql("select * from dl_cpc.cpc_union_log where `date` = \"%s\" ".format(date)).as[UnionLog]
      val ret = log.rdd
        .map {
          x =>
            var rndSex = x.sex
            if (rndSex == 0) {
              rndSex = Random.nextInt(2) + 1  //随机性别
            }

            val rnd = Random.nextInt(100)
            var rndAge = 0
            if (rnd < 8) {
              rndAge = 1
            } else if (rnd < 20) {
              rndAge = 2
            } else if (rnd < 39) {
              rndAge = 3
            } else if (rnd < 63) {
              rndAge = 4
            } else if (rnd < 84) {
              rndAge = 5
            } else {
              rndAge = 6
            }

            var lvl = 0
            if (x.coin < 10) {
              lvl = 1
            } else if (x.coin < 1000) {
              lvl = 2
            } else if (x.coin < 10000) {
              lvl = 3
            } else {
              lvl = 4
            }


            //未知数据随机到其他数据
            var os = x.os
            if (x.os == 0) {
              os = Random.nextInt(2) + 1
            }

            //未知数据随机到其他数据   50(wifi) 5(2g) 15(3g) 30(4g)
            var net = x.network
            if (x.network == 0) {
              val r = Random.nextInt(100)
              if (r < 50) {
                net = 1
              } else if (r < 55) {
                net = 2
              } else if (r < 70) {
                net = 3
              } else {
                net = 4
              }
            }

            AnalCond(
              province = x.province,
              sex = rndSex,
              age = rndAge,
              coin_level = lvl,
              os = os,
              network = net,
              sum = 1,
              uid = x.uid,
              date = date
            )
        }
        .cache()

      val pv = ret.count()
      val uv = ret.map(x => (x.uid, x)).reduceByKey((x, y) => x).count()
      val upv = uv.toFloat / pv.toFloat
      val ret1 = ret.map(x => (x.key, x))
        .reduceByKey((x, y) => x.sum(y))
        .map(_._2)
        .cache()

      ret1.toLocalIterator
        .foreach {
          x =>
            /*
            province-sex-age-coin_level-os-network_TOUCHEDUV
            16-1-5-0-1-1_TOUCHEDUV  => 14674
             */
            val sum = x.sum * upv * 1.5
            redis.set(x.key + "_TOUCHEDUV", sum.toInt)
        }

      println(uv, pv, ret1.count())
    }

    sumColsWithZero()
    ctx.stop()
  }
}

case class AnalCond(
                   province: Int = 0,

                   //暂时按照100分比例来算
                   sex: Int = 0,
                   age: Int = 0,

                   //coin_level 注意保证和bs一致
                   //用户积分级别. 0默认全选 1第一档用户，积分在0-10分。 2第二档用户，积分在0-1000分。 3第三档用户，积分在0-10000分。4全选
                   coin_level: Int = 0,
                   os: Int = 0,
                   network: Int = 0,
                   sum: Int = 0,
                   uid: String = "",
                   date: String = ""

               ) {


  val key = "%d-%d-%d-%d-%d-%d".format(province, sex, age, coin_level, os, network)

  val keyuid = "%d-%d-%d-%d-%d-%d-%s".format(province, sex, age, coin_level, os, network, uid)

  def sum(k: AnalCond): AnalCond = {
    copy(sum = sum + k.sum)
  }
}


