package com.cpc.spark.log.anal

import java.util.Date

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, lit, row_number, when}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
  * fym 190507.
  *
  * 趣头条/米读uv每日计算.
  *
  * conditioned UV daily calculation for qtt / midu.
  * an updated version for ConditionTouchedUV.scala.
  *
  */

object ConditionedUVDaily {

  def main(args: Array[String]): Unit = {

    val date = args(0)
    val appType = args(1)

    val conf = ConfigFactory.load()
    val redis = new RedisClient(
      conf.getString("touched_uv.redis.host"),
      conf.getInt("touched_uv.redis.port")
    )
    redis.select(3)

    val spark = SparkSession
      .builder()
      .appName("[trident] daily uv - %s - %s"
        .format(appType, date)
      )
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._

    // 省份 / 城市 / 性别 / 年龄段 / 金币 / 操作系统 / 网络状况 / 携带电话级别
    // fym 190508: it occurs that coin/share_coin seem to be deprecated.
    val conditions = s"province,city,sex,age,coin,os,network,phone_level"

    val queryRawDataFromUnionEvents =
      s"""
         |select
         |  uid
         |  , %s
         |  , case
         |    when share_coin=0 then 1
         |    when (share_coin>0 and share_coin<=60) then 2
         |    when (share_coin>60 and share_coin<=90) then 3
         |    else 4
         |  end as share_coin_level
         |from dl_cpc.cpc_basedata_union_events
         |where
         |  day='%s'
         |  and media_appsid in %s
         |  and ideaid > 0
       """
      .format(
        conditions,
        date,
        appType match {
          case "qtt" =>
            s"""
               |("80000001", "80000002")
             """.stripMargin
          case "midu" =>
            s"""
               |('80004786','80004787','80001098','80001292','80001539','80002480','80001011')
             """.stripMargin
          case "miRead" =>
            s"""
               |('80004786','80004787','80001098','80001292','80001539','80002480','80001011')
             """.stripMargin // bottom-up compatibility.
          case _ =>
            s"""
               |("80000001", "80000002")
             """.stripMargin // default : qtt
        }
      )
        .stripMargin

    val rawDataFromUnionEvents = spark
      .sql(queryRawDataFromUnionEvents)
      .persist()

    // total uv.
    val uv = rawDataFromUnionEvents
      .groupBy(col("uid"))
      .agg(expr("count(*)").alias("count"))
      .count()

    println(
      "-- total uv: %s --".format(uv)
    )

    val keyToGo = "%stouched_uv_total".format(if (appType == "qtt") "" else "miRead_")
    println(keyToGo)
    redis.set(keyToGo, "%s".format(uv))

    // calculate conditioned uv for each column.
    conditions
      .split("\\,")
      .foreach(x => {
        calculateConditionalUVByPercent(
          appType,
          x, // condition
          rawDataFromUnionEvents,
          spark,
          redis
        )
      }
    )
  }

  def calculateConditionalUVByPercent(
                                     appType: String,
                                     condition: String,
                                     df: DataFrame,
                                     spark: SparkSession,
                                     redis: RedisClient
                                     ) : Unit = {
    val filteredUidWithCondition = df
      .filter(_.getAs[Int](condition) > 0) // coin -> coin.

    val conditionedColumn = {
      if (condition == "coin") {
        "share_coin_level" // coin -> shared_coin_level.
      } else {
        condition
      }
    }

    val uidWithCondition = filteredUidWithCondition
      .select(
        col(conditionedColumn),
        col("uid")
      )
      .groupBy(
        col(conditionedColumn),
        col("uid")
      )
      .agg(expr("count(*)").alias("count")) // calculate total uv given specified column.

    val totalCountUidWithCondition = uidWithCondition
      .count()

    println(totalCountUidWithCondition)

    val percentageUidWithCondition = uidWithCondition
      .groupBy(col(conditionedColumn))
      .agg(expr("count(*)").alias("count_by_condition")) // sub-uv given specified column.
      .withColumn(
        "percentage_by_condition",
        expr("count_by_condition/%s" // percentage.
          .format(
            totalCountUidWithCondition
          )
        )
      )
      .rdd
      .map(x => {
        (
          (
            x.getAs[Int](conditionedColumn),
            x.getAs[Double]("percentage_by_condition")
          )
          , 1
        )
      })
      .map(x => x._1)
      .toLocalIterator
      .toSeq
      .sortWith((x, y) => x._1 < y._1)

    if (condition == "coin") { // accumulative percentage.
      var n = 0d
      percentageUidWithCondition
        .foreach(x => {
          n = n + x._2
          printPercentageAndSetKeyIntoRedis(appType, conditionedColumn, x._1, n, redis)
        }
      )
    } else { // simply print them.
      percentageUidWithCondition
        .foreach(x => {
          printPercentageAndSetKeyIntoRedis(appType, conditionedColumn, x._1, x._2, redis)
        }
      )
    }
  }

  def printPercentageAndSetKeyIntoRedis(
                                       appType: String,
                                       condition: String,
                                       key: Int,
                                       value: Double,
                                       redis: RedisClient
                                       ) : Unit = {
    println("-- %s - %s - %s percentage: %s --"
      .format(
        appType,
        condition,
        key,
        value
      )
    ) // black-box after flight.

    val keyToGo = "%stouched_uv_percent_%s_%d"
      .format(
        if (appType == "qtt") "" else "miRead_",
        condition,
        key
      ) // to-fly.

    println(keyToGo)
    redis.set(keyToGo, "%.8f".format(value)) // fly.
  }
}
