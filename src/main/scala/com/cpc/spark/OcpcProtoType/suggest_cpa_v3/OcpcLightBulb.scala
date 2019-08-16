package com.cpc.spark.OcpcProtoType.suggest_cpa_v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools._
import com.redis.RedisClient
import org.apache.spark.sql.DataFrame
//import com.cpc.spark.ocpc.OcpcUtils._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcLightBulb{
  def main(args: Array[String]): Unit = {
    /*
    通过向redis中存储suggest cpa来控制灯泡的亮灯逻辑
    1. 抽取recommendation数据表
    2. mappartition打开redis，并存储数据
     */
    // 计算日期周期
//    2019-02-02 10 qtt_demo qtt
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString


    val spark = SparkSession
      .builder()
      .appName(s"OcpcLightBulbV2: $date, $hour, $version")
      .enableHiveSupport().getOrCreate()

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version")

    // 获取当前灯泡数据
    val currentLight = getCurrentLight(version, date, hour, spark)

    currentLight
      .repartition(5)
//      .write.mode("overwrite").insertInto("test.ocpc_unit_light_control_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_unit_light_control_hourly")

    currentLight
      .repartition(5)
      .select("unitid", "userid", "adclass", "media", "cpa", "version")
      .repartition(5)
//      .write.mode("overwrite").insertInto("test.ocpc_unit_light_control_version")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_unit_light_control_version")

    // 根据上一个小时的灯泡数据，分别判断需要熄灭和点亮的灯泡
    val result = getUpdateTableV2(currentLight, date, hour, version, spark)

    // 抽取adv的ocpc单元
    val ocpcUnitsRaw = getConversionGoal(date, hour, spark)
    val ocpcUnits = ocpcUnitsRaw
      .filter(s"is_ocpc=1")
      .select("unitid").distinct()

    // 存储到redis
    val resultDF = result
      .join(ocpcUnits, Seq("unitid"), "inner")
      .withColumn("unit_id", col("unitid"))
      .selectExpr("unit_id", "ocpc_light", "cast(round(current_cpa, 2) as double) as ocpc_suggest_price")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF
      .repartition(5)
//      .write.mode("overwrite").insertInto("test.ocpc_light_api_control_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_light_api_control_hourly")

    // 存入redis
    saveDataToRedis(version, date, hour, spark)
    println(s"############## saving redis database ################")
  }

  def cleanRedis(version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    将对应key的值设成空的json字符串
     */
    val data = spark
      .table("dl_cpc.ocpc_light_api_control_hourly")
      .where(s"version = '$version' and `date` = '$date' and `hour` = '$hour' and ocpc_light = 0")
      .selectExpr("cast(unit_id as int) unitid", "cast(round(ocpc_suggest_price, 2) as double) as cpa")
      .repartition(2)
    data.show(10)
    val cnt = data.count()
    println(s"total size of the data is: $cnt")
    val conf = ConfigFactory.load("ocpc")
    val host = conf.getString("adv_redis.host")
    val port = conf.getInt("adv_redis.port")
    val auth = conf.getString("adv_redis.auth")
    println(s"host: $host")
    println(s"port: $port")

    // 测试
    for (record <- data.collect()) {
      val identifier = record.getAs[Int]("unitid").toString
      val valueDouble = record.getAs[Double]("cpa")
      var key = "new_algorithm_unit_ocpc_" + identifier
      println(s"key:$key")
    }

//    data.foreachPartition(iterator => {
//      val redis = new RedisClient(host, port)
//      redis.auth(auth)
//      iterator.foreach{
//        record => {
//          val identifier = record.getAs[Int]("unitid").toString
//          var key = "new_algorithm_unit_ocpc_" + identifier
//          redis.del(key)
//        }
//      }
//      redis.disconnect
//    })
  }

  def saveDataToRedis(version: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark
      .table("dl_cpc.ocpc_light_api_control_hourly")
      .where(s"version = '$version' and `date` = '$date' and `hour` = '$hour' and ocpc_light = 1")
      .selectExpr("cast(unit_id as int) unitid", "cast(round(ocpc_suggest_price, 2) as double) as cpa")
      .repartition(2)

    data.show(10)
    data.printSchema()
    val cnt = data.count()
    println(s"total size of the data is: $cnt")
    val conf = ConfigFactory.load("ocpc")
    val host = conf.getString("adv_redis.host")
    val port = conf.getInt("adv_redis.port")
    val auth = conf.getString("adv_redis.auth")
    println(s"host: $host")
    println(s"port: $port")

//    // 测试
//    for (record <- data.collect()) {
//      val identifier = record.getAs[Int]("unitid").toString
//      val valueDouble = record.getAs[Double]("cpa")
//      var key = "new_algorithm_unit_ocpc_" + identifier
//      if (valueDouble >= 0) {
//        var valueString = valueDouble.toString
//        if (valueString == "0.0") {
//          valueString = "0"
//        }
//        println(s"key:$key, value:$valueString")
//      }
//    }

    data.foreachPartition(iterator => {
      val redis = new RedisClient(host, port)
      redis.auth(auth)
      iterator.foreach{
        record => {
          val identifier = record.getAs[Int]("unitid").toString
          val valueDouble = record.getAs[Double]("cpa")
          var key = "new_algorithm_unit_ocpc_" + identifier
          if (valueDouble >= 0) {
            var valueString = valueDouble.toString
            if (valueString == "0.0") {
              valueString = "0"
            }
            println(s"key:$key, value:$valueString")
            redis.setex(key, 1 * 24 * 60 * 60, valueString)
          }
        }
      }
      redis.disconnect
    })
  }

  def getUpdateTableV2(currentLight: DataFrame, date: String, hour: String, version: String, spark: SparkSession) = {
    /*
    对于ocpc_status=2的ocpc单元，如果本次的推荐cpa清单中没有，则使灯泡灭掉
     */
    val ocpcUnit = getConversionGoal(date, hour, spark)
    ocpcUnit.createOrReplaceTempView("ocpc_unit")
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  cpagiven as prev_cpa
         |FROM
         |  ocpc_unit
         |WHERE
         |  is_ocpc = 1
         |AND
         |  ocpc_status in (2, 4)
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .select("unitid", "userid", "prev_cpa")
      .cache()

    data1.show(10)
    ocpcUnit
      .filter(s"is_ocpc = 1")
      .withColumn("ocpc_light", when(col("ocpc_status") === 2 || col("ocpc_status") === 4, 1).otherwise(0))
      .select("unitid", "ocpc_light")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .repartition(5)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_adv_light_status_hourly")

    val data2 = currentLight
      .groupBy("unitid", "userid")
      .agg(min(col("cpa")).alias("current_cpa"))
      .select("unitid", "userid", "current_cpa")
      .cache()
    data2.show(10)

    // 数据关联
    val data = data2
      .join(data1, Seq("unitid", "userid"), "outer")
      .select("unitid", "userid", "current_cpa", "prev_cpa")
      .na.fill(-1, Seq("current_cpa", "prev_cpa"))
      .withColumn("ocpc_light", udfSetLightSwitch()(col("current_cpa"), col("prev_cpa")))
      .filter(s"userid != 1630465")
      .cache()

    data

  }

  def udfSetLightSwitch() = udf((currentCPA: Double, prevCPA: Double) => {
    var result = 1
    if (currentCPA >= 0) {
      result = 1
    } else {
      result = 0
    }
    result
  })

  def getCurrentLight(version: String, date: String, hour: String, spark: SparkSession) = {
    // 抽取数据
    val cpcData = getRecommendationAd(version, date, hour, spark)
    val ocpcData = getOcpcRecord(version, date, hour, spark)


    val result = cpcData
      .join(ocpcData, Seq("unitid", "userid", "adclass", "media"), "outer")
      .select("unitid", "userid", "adclass", "media", "cpa1", "cpa2")
      .na.fill(-1.0, Seq("cpa1", "cpa2"))
      .withColumn("cpa", udfSelectCPA()(col("cpa1"), col("cpa2")))
      .na.fill(0.0, Seq("cpa1", "cpa2", "cpa"))

    result.show(10)
    val resultDF = result
      .select("unitid", "userid", "adclass", "media", "cpa")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .cache()

    resultDF
  }

  def udfSelectCPA() = udf((cpa1: Double, cpa2: Double) => {
    var cpa = 0.0
    if (cpa2 >= 0) {
      cpa = cpa2
    } else {
      cpa = cpa1
    }

    cpa
  })

  def getOcpcRecord(version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    抽取最近三天所有广告单元的投放记录
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -3)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = getTimeRangeSqlDate(date1, hour, date, hour)

    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    val sqlRequest1 =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    adclass,
         |    cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
         |    (case
         |        when media_appsid in ('80000001', '80000002') then 'qtt'
         |        when media_appsid in ('80002819', '80004944') then 'hottopic'
         |        else 'novel'
         |    end) as media
         |FROM
         |    dl_cpc.ocpc_filter_unionlog
         |WHERE
         |    $selectCondition
         |AND
         |    $mediaSelection
         |AND
         |    is_ocpc = 1
         |AND
         |    isclick = 1
       """.stripMargin
    println(sqlRequest1)
    val rawData1 = spark
      .sql(sqlRequest1)
      .filter(s"is_hidden = 0")
      .select("unitid", "userid", "adclass", "media")
      .distinct()

    // 取近三小时数据
    val hourConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val newToday = hourConverter.parse(newDate)
    val newCalendar = Calendar.getInstance
    newCalendar.setTime(newToday)
    newCalendar.add(Calendar.HOUR, +3)
    val newYesterday = newCalendar.getTime
    val prevTime = hourConverter.format(newYesterday)
    val prevTimeValue = prevTime.split(" ")
    val newDate1 = prevTimeValue(0)
    val newHour1 = prevTimeValue(1)
    val newSelectCondition = getTimeRangeSqlDay(date, hour, newDate1, newHour1)

    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  adclass,
         |  (case
         |      when media_appsid in ('80000001', '80000002') then 'qtt'
         |      when media_appsid in ('80002819', '80004944') then 'hottopic'
         |      else 'novel'
         |  end) as media
         |FROM
         |  dl_cpc.cpc_basedata_click_event
         |WHERE
         |  $newSelectCondition
         |AND
         |  $mediaSelection
         |AND
         |  isclick = 1
         |AND
         |  ocpc_step = 2
       """.stripMargin
    println(sqlRequest2)
    val rawData2 = spark
      .sql(sqlRequest2)
      .select("unitid", "userid", "adclass", "media")
      .distinct()
    val rawData = rawData1
      .join(rawData2, Seq("unitid", "userid", "adclass", "media"), "outer")
      .select("unitid", "userid", "adclass", "media")
      .distinct()

    val sqlRequet3 =
      s"""
         |SELECT
         |  unitid,
         |  media,
         |  cpa_suggest * 1.0 / 100 as cpa_suggest
         |FROM
         |  dl_cpc.ocpc_history_suggest_cpa_version
         |WHERE
         |  version = '$version'
       """.stripMargin
    println(sqlRequet3)
    val suggestDataRaw = spark.sql(sqlRequet3)

    val result = rawData
      .join(suggestDataRaw, Seq("unitid", "media"), "left_outer")
      .select("unitid", "userid", "adclass", "media", "cpa_suggest")
      .withColumn("cpa2", col("cpa_suggest"))
      .na.fill(0, Seq("cpa2"))

    result.show(10)
    val resultDF = result.select("unitid", "userid", "adclass", "media", "cpa2")

    resultDF
  }

  def getRecommendationAd(version: String, date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    adclass,
         |    media,
         |    cpa * 1.0 / 100 as cpa1,
         |    is_recommend
         |FROM
         |    dl_cpc.ocpc_recommend_units_hourly
         |WHERE
         |    date = '$date'
         |AND
         |    `hour` = '$hour'
         |and version = '$version'
       """.stripMargin

    println(sqlRequest)
    val data1 = spark.sql(sqlRequest)

    val data2raw = getConfCPA(version, date, hour, spark)
    val data2 = data2raw
      .withColumn("cpa3", col("cpa_suggest") * 1.0 / 100)
      .select("unitid", "media", "cpa3")

    val result = data1
        .join(data2, Seq("unitid", "media"), "left_outer")
        .select("unitid", "userid", "adclass", "media", "cpa1", "cpa3", "is_recommend")
        .withColumn("is_recommend", when(col("cpa3").isNotNull, 1).otherwise(col("is_recommend")))
        .withColumn("cpa1", when(col("cpa3").isNotNull, col("cpa3")).otherwise(col("cpa1")))

    val resultDF = result
      .filter(s"is_recommend = 1")
      .select("unitid", "userid", "adclass", "media", "cpa1")


    resultDF.show(10)
    resultDF
  }

}
