package com.cpc.spark.oCPX

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}


object OcpcTools {
  def main(args: Array[String]): Unit = {
    /*
    代码测试
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // bash: 2019-01-02 12
    val date = args(0).toString
    val hour = args(1).toString

    // 测试实时数据表和离线表
    val dataRaw1 = getBaseData(48, date, hour, spark)
    val data1 = dataRaw1
      .filter(s"isclick=1")
      .groupBy("unitid", "conversion_goal", "media")
      .agg(
        avg(col("exp_cvr")).alias("pre_cvr"),
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv")
      )
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
    data1
      .repartition(5)
      .write.mode("overwrite").saveAsTable("test.check_cv_data20190729a")

    val dataRaw2 = getRealtimeData(48, date, hour, spark)
    val data2 = dataRaw2
      .groupBy("unitid", "conversion_goal", "media")
      .agg(
        avg(col("exp_cvr")).alias("pre_cvr"),
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv")
      )
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
    data2
      .repartition(5)
      .write.mode("overwrite").saveAsTable("test.check_cv_data20190729b")
  }

  def udfAdslotTypeMapAs() = udf((adslotType: Int) => {
    var result = adslotType match {
      case 5 => 14
      case 6 => 12
      case 7 => 9
      case x => x
    }
    if (adslotType > 7 || adslotType == 0) {
      result = 0
    }
    result
  })

  def getConversionGoal(date: String, hour: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")

    val url = conf.getString("adv_read_mysql.new_deploy.url")
    val user = conf.getString("adv_read_mysql.new_deploy.user")
    val passwd = conf.getString("adv_read_mysql.new_deploy.password")
    val driver = conf.getString("adv_read_mysql.new_deploy.driver")
    val table = "(select id, user_id, ocpc_bid, cast(conversion_goal as char) as conversion_goal, is_ocpc, ocpc_status from adv.unit where ideas is not null) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val resultDF = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .withColumn("cpagiven", col("ocpc_bid"))
      .selectExpr("unitid",  "userid", "cpagiven", "cast(conversion_goal as int) conversion_goal", "is_ocpc", "ocpc_status")
      .distinct()

    resultDF.show(10)
    resultDF
  }

  def getTimeRangeSqlDay(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`day` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((`day` = '$startDate' and hour > '$startHour') " +
      s"or (`day` = '$endDate' and hour <= '$endHour') " +
      s"or (`day` > '$startDate' and `day` < '$endDate'))"
  }

  def getTimeRangeSqlDate(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`date` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((`date` = '$startDate' and hour > '$startHour') " +
      s"or (`date` = '$endDate' and hour <= '$endHour') " +
      s"or (`date` > '$startDate' and `date` < '$endDate'))"
  }

  def udfConcatStringInt(str: String) = udf((intValue: Int) => {
    val result = str + intValue.toString
    result
  })

  def getConfCPA(version: String, date: String, hour: String, spark: SparkSession) = {
    // 从配置文件读取数据
    val conf = ConfigFactory.load("ocpc")
    val suggestCpaPath = conf.getString("ocpc_all.light_control.suggest_path_v2")
    val rawData = spark.read.format("json").json(suggestCpaPath)
    val data = rawData
      .filter(s"version = '$version'")
      .groupBy("unitid", "media")
      .agg(
        min(col("cpa_suggest")).alias("cpa_suggest")
      )
      .selectExpr("unitid", "media", "cpa_suggest")

    data.show()
    data
  }

  def getExpConf(version: String, expTag: String, spark: SparkSession) = {
    // 从配置文件读取数据
    val tag = "ocpc_exp." + version + "." + expTag
    val conf = ConfigFactory.load(tag)
    conf
  }


  def getBaseData(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  adslot_type,
         |  isshow,
         |  isclick,
         |  bid_discounted_by_ad_slot as bid,
         |  price,
         |  cast(exp_cvr as double) as exp_cvr,
         |  cast(exp_ctr as double) as exp_ctr,
         |  media_appsid,
         |  (case
         |      when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |      when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |      when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |      when adclass in (110110100, 125100100) then "wzcp"
         |      else "others"
         |  end) as industry,
         |  conversion_goal,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2)


    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }

  def getBaseDataDelay(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  adslot_type,
         |  isshow,
         |  isclick,
         |  bid_discounted_by_ad_slot as bid,
         |  price,
         |  cast(exp_cvr as double) as exp_cvr,
         |  cast(exp_ctr as double) as exp_ctr,
         |  media_appsid,
         |  (case
         |      when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |      when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |      when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |      when adclass in (110110100, 125100100) then "wzcp"
         |      else "others"
         |  end) as industry,
         |  conversion_goal,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2)


    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }

  def getRealtimeData(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  isclick,
         |  exp_cvr,
         |  media,
         |  industry,
         |  conversion_goal,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_quick_click_log
         |WHERE
         |  $selectCondition
         |AND
         |  media in ('qtt', 'novel', 'hottopic')
         |AND
         |  ocpc_step in (1, 2)
         |AND
         |  adslot_type != 7
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
//      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  conversion_goal
         |FROM
         |  dl_cpc.ocpc_quick_cv_log
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark
      .sql(sqlRequest2)
      .select("searchid", "conversion_goal")
      .filter(s"conversion_goal > 0")
      .withColumn("iscvr", lit(1))
      .distinct()

    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "conversion_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }

  def getRealtimeDataDelay(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  isclick,
         |  exp_cvr,
         |  media,
         |  industry,
         |  conversion_goal,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_quick_click_log
         |WHERE
         |  $selectCondition
         |AND
         |  media in ('qtt', 'novel', 'hottopic')
         |AND
         |  ocpc_step in (1, 2)
         |AND
         |  adslot_type != 7
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
    //      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  conversion_goal
         |FROM
         |  dl_cpc.ocpc_quick_cv_log
         |WHERE
         |  `date` >= '$date1'
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark
      .sql(sqlRequest2)
      .select("searchid", "conversion_goal")
      .filter(s"conversion_goal > 0")
      .withColumn("iscvr", lit(1))
      .distinct()



    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "conversion_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }

  def udfMediaName() = udf((media: String) => {
    var result = media match {
      case "qtt" => "Qtt"
      case "hottopic" => "HT66"
      case "novel" => "Midu"
      case _ => "others"
    }
    result
  })

  def udfDetermineMedia() = udf((mediaId: String) => {
    var result = mediaId match {
      case "80000001" => "qtt"
      case "80000002" => "qtt"
      case "80002819" => "hottopic"
      case "80004944" => "hottopic"
      case _ => "novel"
    }
    result
  })

  def udfDetermineIndustry() = udf((adslotType: Int, adclass: Int) => {
    val adclassString = adclass.toString
    val adclass3 = adclassString.substring(0, 3)
    var result = "others"
    if (adclass3 == "134" || adclass3 == "107") {
      result = "elds"
    } else if (adclass3 == "100" && adslotType != 7) {
      result = "feedapp"
    } else if (adclass3 == "100" && adslotType == 7) {
      result = "yysc"
    } else if (adclass == 110110100 || adclass == 125100100) {
      result = "wzcp"
    } else {
      result = "others"
    }
    result

  })

  def udfDetermineConversionGoal() = udf((traceType: String, traceOp1: String, traceOp2: String) => {
    /*
    conversion_goal = 1: trace_op1="REPORT_DOWNLOAD_PKGADDED" and trace_type=apkdown
    conversion_goal = 2: trace_type="active_third"
    conversion_goal = 3: trace_type="active15" or trace_type="ctsite_active15"
    conversion_goal = 4: trace_op1="REPORT_USER_STAYINWX"
     */
    var result = 0
    if (traceOp1 == "REPORT_DOWNLOAD_PKGADDED") {
      result = 1
    } else if (traceType == "active_third") {
      result = 2
    } else if (traceType == "active15" || traceType == "ctsite_active15") {
      result = 3
    } else if (traceOp1 == "REPORT_USER_STAYINWX") {
      result = 4
    } else {
      result = 0
    }
    result
  })

  def udfSetExpTag(expTag: String) = udf((media: String) => {
    var result1 = expTag match {
      case "base" => media
      case _ => expTag + media
    }

    var result = result1 match {
      case "oCPColdflowMidu" => "oCPColdflowNovel"
      case "delayHT66" => "delayhottopic"
      case x => x
    }
    result
  })

  def udfCalculatePay() = udf((cost: Double, cv: Int, cpagiven: Double) => {
    var result = cost - 1.2 * 0.01 * cv * cpagiven
    if (result < 0) {
      result = 0
    }
    result
  })

}