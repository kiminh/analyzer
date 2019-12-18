package com.cpc.spark.oCPX.oCPC.light_control.white_list

import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.oCPX.OcpcTools._


object OcpcFreePass {
  def main(args: Array[String]): Unit = {
    /*
    oCPC零门槛实验
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    println(s"parameters: date=$date, hour=$hour, version=$version")

    // 获取广告主
    // userid, category
    val user = getUserData(spark)

    // 获取广告单元
    val unit = getUnitData(date, hour, spark).cache()
    unit.show(10)

    // 获取同账户是否有历史数据
    val historyData = getHistoryData(date, spark)

    // 获取黑名单账户
    val blackList = getBlackList(spark)

    // 获取前一天ocpc消耗突破1000块的账户id
    val userCost = getUserCost(date, spark)

    // 获取白名单
    val whiteList = getWhiteList(spark)

    // oCPC补量实验
    val ocpcBuliang = ocpcBlackUsers(spark)

    // 数据关联
    val joinData = unit
        .join(user, Seq("userid"), "inner")
        .select("unitid",  "userid", "conversion_goal", "is_ocpc", "ocpc_status", "media", "adclass", "industry", "time_flag")
        .join(historyData, Seq("userid", "conversion_goal", "media"), "left_outer")
        .select("unitid",  "userid", "conversion_goal", "is_ocpc", "ocpc_status", "media", "adclass", "industry", "cost_flag", "time_flag")
        .na.fill(0, Seq("cost_flag"))
        .withColumn("flag_ratio", udfCalculateRatio()(col("conversion_goal"), col("industry"), col("media"), col("cost_flag")))
        .withColumn("random_value", udfGetRandom()())
        .join(blackList, Seq("userid"), "left_outer")
        .join(userCost, Seq("userid", "conversion_goal", "media"), "left_outer")
        .join(whiteList, Seq("unitid", "userid", "media"), "left_outer")
        .na.fill(0, Seq("user_black_flag", "user_cost_flag", "unit_white_flag"))
        .withColumn("flag", udfDetermineFlag()(col("flag_ratio"), col("random_value"), col("user_black_flag"), col("user_cost_flag"), col("unit_white_flag"), col("time_flag")))
        .join(ocpcBuliang, Seq("userid"), "left_outer")
        .na.fill(0, Seq("is_open"))
        .withColumn("flag_old", col("flag"))
        .withColumn("flag", when(col("is_open") === 1, 1).otherwise(col("flag")))

//    joinData
//        .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20191216b")

    joinData
      .select("unitid", "userid", "media", "conversion_goal", "ocpc_status", "adclass", "industry", "cost_flag", "time_flag", "flag_ratio", "random_value", "user_black_flag", "user_cost_flag", "unit_white_flag", "flag")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_auto_second_stage_light")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_auto_second_stage_light")


    val resultDF = spark
      .table("dl_cpc.ocpc_auto_second_stage_light")
      .where(s"`date` = '$date' and `hour` = '$hour' and version = '$version' and flag = 1")

    resultDF
      .select("unitid", "userid", "conversion_goal", "media")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_auto_second_stage_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_auto_second_stage_hourly")



  }

  def ocpcBlackUsers(spark: SparkSession) = {
//    val dataRaw = spark.read.textFile("/user/cpc/lixuejian/online/select_hidden_tax_user/ocpc_hidden_tax_user.list")
    val dataRaw = spark.read.textFile("/user/cpc/wangjun/ocpc/test/ocpc_hidden_tax_user.list")

    val data = dataRaw
      .select("value")
      .withColumn("userid", udfGetItem(0, " ")(col("value")))
      .withColumn("is_open", udfGetItem(1, " ")(col("value")))
      .select("userid", "is_open")
      .distinct()

    data
  }

  def ocpcBlackUnits(spark: SparkSession) = {
    // ocpc补量策略实验
    val dataRaw = spark.read.textFile("/user/cpc/lixuejian/online/select_hidden_tax_unit/ocpc_hidden_tax_unit.list")

    val data = dataRaw
      .withColumn("unitid", udfGetItem(0, " ")(col("value")))
      .withColumn("bl_flag", lit(1))
      .select("unitid", "bl_flag")
      .distinct()

    data
  }

  def udfGetItem(index: Int, splitter: String) = udf((value: String) => {
    val valueItem = value.split(splitter)(index)
    val result = valueItem.toInt
    result
  }
  )

  def udfDetermineFlag() = udf((flagRatio: Int, randomValue: Int, userBlackFlag: Int, userCostFlag: Int, unitWhiteFlag: Int, timeFlag: Int) => {
    var cmpValue = 1
    if (flagRatio > randomValue) {
      cmpValue = 1
    } else {
      cmpValue = 0
    }

    val result = (unitWhiteFlag, timeFlag, userBlackFlag, userCostFlag, cmpValue) match {
      case (1, _, _, _, _) => 1
      case (0, 0, _, _, _) => 0
      case (0, 1, 1, _, _) => 0
      case (0, 1, 0, 0, _) => 0
      case (0, 1, 0, 1, 1) => 1
      case (0, 1, 0, 1, 0) => 0
      case (_, _, _, _, _) => -1
    }

    result
  })

  def udfGetRandom() = udf(() => {
    val r = scala.util.Random
    val result = r.nextInt(100)
    result
  })

  def udfCalculateRatio() = udf((conversionGoal: Int, industry: String, media: String, costFlag: Int) => {
    val result = (conversionGoal, industry, media, costFlag) match {
      case (1, "app", "hottopic", _) => 5
      case (2, "app", "qtt", _) => 5
      case (2, "app", "novel", _) => 5
      case (_, "yihu", "qtt", 1) => 50
      case (_, "elds", _, _) => 0
      case (_, _, _, _) => 5
    }
    result
  })

  def getWhiteList(spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")
    // 增加单元层级白名单
    val ocpcWhiteUnits = conf.getString("ocpc_all.light_control.ocpc_unit_whitelist")
    val ocpcWhiteUnitList= spark
      .read
      .format("json")
      .json(ocpcWhiteUnits)
      .select("unitid", "userid", "media")
      .withColumn("unit_white_flag", lit(1))
      .distinct()
    println("ocpc unit white list for testing ocpc light:")
    ocpcWhiteUnitList.show(10)

    ocpcWhiteUnitList
  }

  def getUserCost(date: String, spark: SparkSession) = {
    // 按照前24小时的消费，过滤掉不能参与测试的单元（账户前一天oCPC日耗低于1000元）
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`date` = '$date1'"

    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    val sqlRequest1 =
      s"""
         |SELECT
         |  userid,
         |  conversion_goal,
         |  (case
         |      when media_appsid in ('80000001', '80000002') then 'qtt'
         |      when media_appsid in ('80002819', '80004944', '80004948', '80004953') then 'hottopic'
         |      else 'novel'
         |  end) as media,
         |  sum(case when isclick=1 then price else 0 end) * 0.01 as cost
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  conversion_goal > 0
         |GROUP BY
         |  userid,
         |  conversion_goal,
         |  (case
         |      when media_appsid in ('80000001', '80000002') then 'qtt'
         |      when media_appsid in ('80002819', '80004944', '80004948', '80004953') then 'hottopic'
         |      else 'novel'
         |  end)
       """.stripMargin
    println(sqlRequest1)
    val userCost = spark
      .sql(sqlRequest1)
      .withColumn("user_cost_flag", when(col("cost") >= 1000.0, lit(1)).otherwise(0))
      .select("userid", "conversion_goal", "media", "cost", "user_cost_flag")
      .distinct()

    userCost
  }

  def getBlackList(spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")

    val ocpcBlackListConf = conf.getString("ocpc_all.light_control.ocpc_black_list")
    val ocpcBlacklist = spark
      .read
      .format("json")
      .json(ocpcBlackListConf)
      .select("userid")
      .withColumn("user_black_flag", lit(1))
      .distinct()
    println("ocpc blacklist for testing ocpc light:")
    ocpcBlacklist.show(10)

    ocpcBlacklist

  }

  def getHistoryData(date: String, spark: SparkSession) = {
    /*
    从报表数据集中抽取历史消费数据，检查每个userid在不同转化目标和媒体下最近10天是否有ocpc二阶段消费
    dl_cpc.ocpc_report_base_hourly
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -11)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`date` >= '$date1' and `date` < '$date'"

    // 抽取数据
    val sqlRequest =
      s"""
         |SELECT
         |  userid,
         |  conversion_goal,
         |  media,
         |  1 as cost_flag
         |FROM
         |  dl_cpc.ocpc_report_base_hourly
         |WHERE
         |  $selectCondition
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).distinct()
    data
  }

  def getUserData(spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")

    val url = conf.getString("adv_read_mysql.new_deploy.url")
    val user = conf.getString("adv_read_mysql.new_deploy.user")
    val passwd = conf.getString("adv_read_mysql.new_deploy.password")
    val driver = conf.getString("adv_read_mysql.new_deploy.driver")
    val table = "(select id, category from adv.user where category > 0) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val resultDF = data
      .withColumn("userid", col("id"))
      .withColumn("adclass", col("category"))
      .selectExpr("userid", "cast(adclass as int) as adclass")
      .withColumn("industry", udfDetermineIndustryV2()(col("adclass")))
      .distinct()

    resultDF.show(10)
    resultDF
  }

  def getUnitData(date: String, hour: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")

    val url = conf.getString("adv_read_mysql.new_deploy.url")
    val user = conf.getString("adv_read_mysql.new_deploy.user")
    val passwd = conf.getString("adv_read_mysql.new_deploy.password")
    val driver = conf.getString("adv_read_mysql.new_deploy.driver")
    val table = "(select id, user_id, cast(conversion_goal as char) as conversion_goal, target_medias, is_ocpc, ocpc_status, create_time from adv.unit where ideas is not null and is_ocpc = 1 and ocpc_status in (0, 3)) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val rawData = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .selectExpr("unitid",  "userid", "cast(conversion_goal as int) conversion_goal", "cast(is_ocpc as int) is_ocpc", "ocpc_status", "target_medias", "create_time")
      .distinct()

    rawData.createOrReplaceTempView("raw_data")

    val deadline = date + " " + hour + ":00:00"

    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    conversion_goal,
         |    is_ocpc,
         |    ocpc_status,
         |    target_medias,
         |    cast(a as string) as media_appsid,
         |    (case when create_time >= '$deadline' then 1 else 0 end) as time_flag,
         |    create_time,
         |    '$deadline' as deadline
         |from
         |    raw_data
         |lateral view explode(split(target_medias, ',')) b as a
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .na.fill("", Seq("media_appsid"))
      .withColumn("media", udfDetermineMediaNew()(col("media_appsid")))
      .filter(s"media in ('qtt', 'hottopic', 'novel')")
      .select("unitid",  "userid", "conversion_goal", "is_ocpc", "ocpc_status", "media", "time_flag", "create_time", "deadline")
      .distinct()


    resultDF.show(10)
    resultDF
  }

  def udfDetermineMediaNew() = udf((mediaId: String) => {
    var result = mediaId match {
      case "80000001" => "qtt"
      case "80000002" => "qtt"
      case "80002819" => "hottopic"
      case "80004944" => "hottopic"
      case "80004948" => "hottopic"
      case "80004953" => "hottopic"
      case "" => "qtt"
      case "80001098" => "novel"
      case "80001292" => "novel"
      case "80001539" => "novel"
      case "80002480" => "novel"
      case "80001011" => "novel"
      case "80004786" => "novel"
      case "80004787" => "novel"
      case _ => "others"
    }
    result
  })



}