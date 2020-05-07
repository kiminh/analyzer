package com.cpc.spark.log.anal

import com.redis.RedisClient
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * fym 190507.
  */

object ConditionedUVDaily {

  def main(args: Array[String]): Unit = {
    val date: String = args(0)
    val appType: String = args(1)
    val conf: Config = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("touched_uv.redis.host"), conf.getInt("touched_uv.redis.port"))
    redis.select(3)
    val redisNew = new RedisClient(conf.getString("touched_uv_new_redis.redis.host"), conf.getInt("touched_uv_new_redis.redis.port"), 9, Some(conf.getString("touched_uv_new_redis.redis.secret")))
    val spark: SparkSession = SparkSession.builder().appName("[cpc-data] uv-%s".format(appType)).enableHiveSupport().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()

    // 省份 / 城市 / 性别 / 年龄段 / 金币 / 操作系统 / 网络状况 / 携带电话级别 / 是否新老用户
    val conditions = s"province,city,sex,age,coin,os,network,phone_level,qukan_new_user"

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
      .format(conditions, date, appType match {
        case "qtt" => s"('80000001', '80000002')"
        case "midu" => s"('80004786','80004787','80001098','80001292','80001539','80002480','80001011')"
        case "miRead" => s"('80004786','80004787','80001098','80001292','80001539','80002480','80001011')"
        case _ => s"('80000001', '80000002')"
      }).stripMargin
    val rawDataFromUnionEvents: DataFrame = spark.sql(queryRawDataFromUnionEvents).persist()

    // total uv.
    val uv = rawDataFromUnionEvents.groupBy(col("uid")).agg(expr("count(*)").alias("count")).count()
    println("-- total uv: %s --".format(uv))

    val keyToGo: String = "%stouched_uv_total".format(if (appType == "qtt") "" else "miRead_")
    println(keyToGo)
    redis.set(keyToGo, "%s".format(uv))
    redisNew.set(keyToGo, "%s".format(uv))
    conditions.split("\\,").foreach((x: String) => {calculateConditionalUVByPercent(appType, x, rawDataFromUnionEvents, spark, redis, redisNew)})
  }

  def calculateConditionalUVByPercent(appType: String, condition: String, df: DataFrame, spark: SparkSession, redis: RedisClient,redisNew: RedisClient) : Unit = {
    val filteredUidWithCondition: Dataset[Row] = df.filter((_: Row).getAs[Int](condition) >= 0) // coin -> coin.
    val conditionedColumn: String = condition match {
      case "coin" => "share_coin_level"
      case _ => condition
    }

    val uidWithCondition: DataFrame = filteredUidWithCondition.select(col(conditionedColumn), col("uid")).groupBy(col(conditionedColumn), col("uid")).agg(expr("count(*)").alias("count")) // calculate total uv given specified column.
    val percentageUidWithCondition: Seq[(Int, Double)] = uidWithCondition.groupBy(col(conditionedColumn)).agg(expr("count(*)").alias("count_by_condition")) .withColumn("percentage_by_condition", expr("count_by_condition/%s".format(uidWithCondition.count()))).rdd.map((x: Row) => {((x.getAs[Int](conditionedColumn), x.getAs[Double]("percentage_by_condition")), 1)}).map((x: ((Int, Double), Int)) => x._1).toLocalIterator.toSeq.sortWith((x: (Int, Double), y: (Int, Double)) => x._1 < y._1)

    val conditionToGoRedis: String = conditionedColumn match {
      case "qukan_new_user" => "old_or_new_user"
      case _ => conditionedColumn
    }

    if (condition == "coin") { // accumulative percentage.
      var n = 0d
      percentageUidWithCondition.foreach((x: (Int, Double)) => {
        n = n + x._2
        printPercentageAndSetKeyIntoRedis(appType, conditionToGoRedis, x._1, n, redis)
        printPercentageAndSetKeyIntoRedis(appType, conditionToGoRedis, x._1, n, redisNew)
      })
    } else { // simply print them.
      percentageUidWithCondition.foreach((x: (Int, Double)) => {
        printPercentageAndSetKeyIntoRedis(appType, conditionToGoRedis, x._1, x._2, redis)
        printPercentageAndSetKeyIntoRedis(appType, conditionToGoRedis, x._1, x._2, redisNew)
      })
    }
  }

  def printPercentageAndSetKeyIntoRedis(appType: String, condition: String, key: Int, value: Double, redis: RedisClient) : Unit = {
    /*
      新老用户
        union_events逻辑:0老用户  1新用户 2未知
        写入redis逻辑：   1新用户  2老用户 0未知
     */
    val keyShuffled: Int = if (condition == "old_or_new_user") {
      key match {
        case 0 => 2 // 老
        case 1 => 1 // 新
        case 2 => 0 // 未知
        case _ => 0 // 未知
      }
    } else key
    val keyToGo: String = "%stouched_uv_percent_%s_%d".format(if (appType == "qtt") "" else "miRead_", condition, keyShuffled) // to-fly.
    println(keyToGo)
    redis.set(keyToGo, "%.8f".format(value)) // fly.
  }
}
