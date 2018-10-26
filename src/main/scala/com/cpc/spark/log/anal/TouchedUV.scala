package com.cpc.spark.log.anal
import java.util.Date

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2018-10-24
  */
object TouchedUV {
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
        val appType = args(1).trim
        //加载配置
        val conf = ConfigFactory.load()
        //获取redis
        val redis = new RedisClient(conf.getString("touched_uv.redis.host"), conf.getInt("touched_uv.redis.port"))
        redis.select(3)

        val spark = SparkSession.builder()
          .appName("cpc anal condition touched uv from %s in %s".format(appType,date))
          .enableHiveSupport()
          .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()
        import spark.implicits._
        //根据趣头条、米读选择不同的sql语句
        val sql = appType match {
            case "qtt" =>
                "select uid,province,city,sex,age,coin,os,network,ext['share_coin'].int_value as share_coin ,ext['phone_level'].int_value as phone_level  from dl_cpc.cpc_union_log where `date` = \"%s\" and media_appsid in (\"80000001\", \"80000002\") and isshow=1 and ideaid>0"
                  .format(date)
            case "miRead" =>
                "select uid,province,city,sex,age,coin,os,network,ext['share_coin'].int_value as share_coin ,ext['phone_level'].int_value as phone_level  from dl_cpc.cpc_union_log where `date` = \"%s\" and media_appsid in (\"80001098\", \"80001292\") and isshow=1 and ideaid>0"
                  .format(date)
            case _ => ""
        }
        if (sql=="") {
            System.exit(1)
        }
        //uid,province,city,sex,age,coin,os,network
        val ulog = spark.sql(sql).rdd
        //根据uid计算uv
        val uv = ulog.map(x => (x.getAs[String]("uid"), 1)).reduceByKey((x, y) => x).count()
        if (appType=="qtt")
            redis.set("touched_uv_total", uv)
        else
            redis.set("%s_touched_uv_total".format(appType), uv)
        System.out.println("%s_touched_uv_total is %s".format(appType,uv.toString))

//        val unionLog = ulog.randomSplit(Array(rate, 1 - rate), new Date().getTime)(0).coalesce(900).cache()

        val unionLog = ulog.coalesce(900).cache()
        //省
        val province = unionLog.filter(_.getAs[Int]("province") > 0)
          .map(u => ((u.getAs[Int]("province"), u.getAs[String]("uid")), 1))
        calcCondPercent(appType, "province", province, redis)
        //城市
        val city = unionLog.filter(_.getAs[Int]("city") > 0)
          .map(u => ((u.getAs[Int]("city"), u.getAs[String]("uid")), 1))
        calcCondPercent(appType, "city", city, redis)
        //性别
        val sex = unionLog.filter(_.getAs[Int]("sex") > 0)
          .map(u => ((u.getAs[Int]("sex"), u.getAs[String]("uid")), 1))
        calcCondPercent(appType, "sex", sex, redis)
        //年龄
        val age = unionLog.filter(_.getAs[Int]("age") > 0)
          .map(u => ((u.getAs[Int]("age"), u.getAs[String]("uid")), 1))
        calcCondPercent(appType, "age", age, redis)
        //coin
        val coin = unionLog.filter(_.getAs[Int]("coin") > 0)
          .map {
              u =>
                  val coin = u.getAs[Int]("share_coin")
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
                  ((lvl, u.getAs[String]("uid")), 1)
          }
        calcCondPercent(appType, "coin", coin, redis)
        //操作系统
        val os = unionLog.filter(_.getAs[Int]("os") > 0)
          .map(u => ((u.getAs[Int]("os"), u.getAs[String]("uid")), 1))
        calcCondPercent(appType, "os", os, redis)
        //网络情况
        val net = unionLog.filter(_.getAs[Int]("network") > 0)
          .map(u => ((u.getAs[Int]("network"), u.getAs[String]("uid")), 1))
        calcCondPercent(appType, "net", net, redis)
        //手机机型
        val phone_level = unionLog.map(u => (u.getAs[Int]("phone_level"), u))
          .filter(_._1 > 0)
          .map(x => ((x._1, x._2.getAs[String]("uid")), 1))
        calcCondPercent(appType, "phone_level", phone_level, redis)
    }

    def calcCondPercent(appType:String, name:String, unionLog: RDD[((Int,String),Int)], redis: RedisClient): Unit = {
        val cond = unionLog.reduceByKey((x,y) => x)
          .map(x => (x._1._1, 1))
          .reduceByKey(_ + _)
          .map(x => (x._1, x._2))
        val sum = cond.map(x => x._2).sum()

        if (name == "coin") {
            val coins = cond.toLocalIterator.toSeq.sortWith((x, y) => x._1 < y._1)
            var n = 0d
            coins.foreach {
                case (i, v) =>
                    val key = appType match {
                        case "qtt" => "touched_uv_percent_%s_%d".format(name, i)
                        case "miRead" => "%s_touched_uv_percent_%s_%d".format(appType, name, i)
                        case _ => "touched_uv_percent_%s_%d".format(name, i)
                    }
                    //val key = "%s_touched_uv_percent_%s_%d".format(appType, name, i)
                    n = n + v
                    val p = n / sum
                    redis.set(key, "%.8f".format(p))
                    System.out.println(name, key, n, "%.8f".format(p))
            }
        } else {
            cond.toLocalIterator.foreach {
                  x =>
                      val p = x._2 / sum
                      val key = appType match {
                          case "qtt" => "touched_uv_percent_%s_%d".format(name, x._1)
                          case "miRead" => "%s_touched_uv_percent_%s_%d".format(appType, name, x._1)
                          case _ => "touched_uv_percent_%s_%d".format(name, x._1)
                      }
                      //val key = "%s_touched_uv_percent_%s_%d".format(appType, name, x._1)
                      redis.set(key, "%.8f".format(p))
                      System.out.println(name, key, x._1, x._2, "%.8f".format(p))
            }
        }
    }
}
