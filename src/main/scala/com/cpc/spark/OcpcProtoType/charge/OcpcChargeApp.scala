package com.cpc.spark.OcpcProtoType.charge

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object OcpcChargeApp {
  def main(args: Array[String]): Unit = {
    /*
    根据最近四天有投放oCPC广告的广告单元各自的消费时间段的消费数据统计是否超成本和赔付数据
     */
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val dayCnt = args(4).toInt

    val ocpcOpenTime = getOcpcOpenTime(3, date, hour, spark)
    ocpcOpenTime.write.mode("overwrite").saveAsTable("test.check_ocpc_charge20190418a")
    val baseData = getOcpcData(media, dayCnt, date, hour, spark)

    val costData = assemblyData(dayCnt, baseData, ocpcOpenTime, date, hour, spark)
    costData.write.mode("overwrite").saveAsTable("test.ocpc_charge_daily20190419")
    cleanDataInMysql(3, date, hour, spark)

    val prevData = getDataFromMysql(spark)
    val data = costData
      .join(prevData, Seq("unitid"), "left_outer")
      .filter(s"flag is null")
      .select("unitid", "cost", "conversion", "pay", "ocpc_time", "cpagiven", "cpareal")

    val dataFilter = data
      .filter(s"conversion > 30")
      .filter(s"pay > 0")
      .select("unitid", "cost", "conversion", "pay", "ocpc_time", "cpagiven", "cpareal")


    dataFilter.show(10)

    saveDataToMysql(dataFilter, spark)

//    val result = data
//      .withColumn("date", lit(date))
//      .withColumn("version", lit("qtt_demo"))
//
//    result
//      .repartition(1).write.mode("overwrite").insertInto("dl_cpc.ocpc_charge_daily")
////      .repartition(1).write.mode("overwrite").saveAsTable("test.ocpc_charge_daily")

  }

  def cleanDataInMysql(dayCnt: Int, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    // 设置mysql库
    val conf = ConfigFactory.load("ocpc")
    val url = conf.getString("ocpc_pay_mysql.test.url")
    val username = conf.getString("ocpc_pay_mysql.test.user")
    val password = conf.getString("ocpc_pay_mysql.test.password")
    val driver = conf.getString("ocpc_pay_mysql.test.driver")
//    val table = "(select unit_id from adv.ocpc_compensate_app30) as tmp"
    val delSQL = s"delete from adv.ocpc_compensate_app30 where date(ocpc_charge_time) = '$date1'"

    var connection: Connection = null
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement
      val rs = statement.executeUpdate(delSQL)
      println(s"execute $delSQL success!")
    }
    catch {
      case e: Exception => e.printStackTrace
    }
    //关闭连接，释放资源
    connection.close


  }

  def saveDataToMysql(data: DataFrame, spark: SparkSession) = {
    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val mariadb_write_prop = new Properties()
//    val mariadb_write_url = conf.getString("mariadb.report2_write.url")
//    mariadb_write_prop.put("user", conf.getString("mariadb.report2_write.user"))
//    mariadb_write_prop.put("password", conf.getString("mariadb.report2_write.password"))
//    mariadb_write_prop.put("driver", conf.getString("mariadb.report2_write.driver"))

    val tableName = "adv.ocpc_compensate_app30"
    val mariadb_write_url = conf.getString("ocpc_pay_mysql.test.url")
    mariadb_write_prop.put("user", conf.getString("ocpc_pay_mysql.test.user"))
    mariadb_write_prop.put("password", conf.getString("ocpc_pay_mysql.test.password"))
    mariadb_write_prop.put("driver", conf.getString("ocpc_pay_mysql.test.driver"))

    data.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  cast(unitid as int) unit_id,
         |  cast(round(cost * 0.01, 2) as double) as cost,
         |  cast(conversion as int) as conversion,
         |  cast(round(pay * 0.01, 2) as double) as pay,
         |  cast(round(cpagiven * 0.01, 2) as double) as cpagiven,
         |  cast(round(cpareal * 0.01, 2) as double) as cpareal,
         |  ocpc_time as ocpc_charge_time
         |FROM
         |  base_data
       """.stripMargin
    println(sqlRequest)
    val result = spark.sql(sqlRequest)
    result.printSchema()

//    val result = data
//        .selectExpr("cast(unitid as int) unit_id", "cast(cost as double) as cost", "conversion", "pay", "", "cpagiven", "cpareal")

    result.write.mode(SaveMode.Append)
      .jdbc(mariadb_write_url, tableName, mariadb_write_prop)
    println(s"insert into $tableName success!")
  }

  def getDataFromMysql(spark: SparkSession) = {

    // 设置mysql库
    val conf = ConfigFactory.load("ocpc")
    val url = conf.getString("ocpc_pay_mysql.test.url")
    val user = conf.getString("ocpc_pay_mysql.test.user")
    val passwd = conf.getString("ocpc_pay_mysql.test.password")
    val driver = conf.getString("ocpc_pay_mysql.test.driver")
    val table = "(select unit_id from adv.ocpc_compensate_app30) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val base = data
      .withColumn("unitid", col("unit_id"))
      .withColumn("flag", lit(1))
      .select("unitid", "flag").distinct()

    base
  }

  def assemblyData(dayCnt: Int, rawData: DataFrame, ocpcOpenTime: DataFrame, date: String, hour: String, spark: SparkSession) = {
    // 取点击数据
    val clickData = rawData
      .join(ocpcOpenTime, Seq("unitid", "conversion_goal"), "inner")
      .select("searchid", "timestamp", "unitid", "userid", "conversion_goal", "cpagiven", "isclick", "price", "seq", "date", "hour")

//    clickData.write.mode("overwrite").saveAsTable("test.check_ocpc_charge20190418b")

    // 取转化数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)

    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
         |AND
         |  cvr_goal = 'cvr2'
       """.stripMargin
    println(sqlRequest1)
    val cvData = spark.sql(sqlRequest1)

    // 数据关联
    val baseData = clickData
        .join(cvData, Seq("searchid"), "left_outer")
        .na.fill(0, Seq("iscvr"))
        .select("searchid", "timestamp", "unitid", "userid", "conversion_goal", "cpagiven", "isclick", "price", "seq", "iscvr", "date", "hour")
        .withColumn("ocpc_time", concat_ws(" ", col("date"), col("hour")))

    baseData.createOrReplaceTempView("base_data")

    // 数据汇总
    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  sum(case when isclick=1 then price else 0 end) as cost,
         |  sum(iscvr) as cv,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpagiven
         |FROM
         |  base_data
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest2)
    val summaryData1 = spark
      .sql(sqlRequest2)
      .withColumn("pred_cost", col("cv") * col("cpagiven") * 1.2)
      .withColumn("pay", udfCalculatePay()(col("cost"), col("pred_cost")))
      .withColumn("cpareal", col("cost") * 1.0 / col("cv"))

    val summaryData2 = baseData
      .filter(s"seq = 1")
      .select("unitid", "ocpc_time")

    val summaryData = summaryData1
      .join(summaryData2, Seq("unitid"), "left_outer")
      .withColumn("conversion", col("cv"))
      .select("unitid", "cost", "conversion", "pay", "ocpc_time", "cpagiven", "cpareal")

    summaryData
  }

  def udfCalculatePay() = udf((cost: Double, pred_cost: Double) => {
    var result = 0.0
    if (cost <= pred_cost) {
      result = 0.0
    } else {
      result = cost - pred_cost
    }
    result
   })

//  def udfCmpTime() = udf((date: String, hour: String, open_date: String, open_hour: String) => {
//    var flag = 0
//    if (date < open_date) {
//      flag = 0
//    } else if (date > open_date) {
//      flag = 1
//    } else {
//      if (hour < open_hour) {
//        flag = 0
//      } else {
//        flag = 1
//      }
//    }
//    flag
//  })

  def getOcpcData(media: String, dayCnt: Int, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val conf_key1 = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key1)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  timestamp,
         |  unitid,
         |  userid,
         |  cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
         |  cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |  cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
         |  isclick,
         |  price,
         |  (case
         |        when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |        when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |        when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |        when adclass in (110110100, 125100100) then "wzcp"
         |        else "others"
         |    end) as industry,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick=1
       """.stripMargin
    println(sqlRequest)
    val rawData = spark
      .sql(sqlRequest)
      .filter(s"is_hidden = 0 and conversion_goal = 2 and industry = 'feedapp'")

    rawData.createOrReplaceTempView("raw_data")

//        .filter(s"is_hidden = 0 and conversion_goal = 3")
    val sqlRequest2 =
      s"""
         |SELECT
         |  *,
         |  row_number() over(partition by unitid order by timestamp) as seq
         |FROM
         |  raw_data
       """.stripMargin
    println(sqlRequest2)
    val data = spark.sql(sqlRequest2)
//    data.write.mode("overwrite").saveAsTable("test.check_ocpc_charge20190425b")

    data
  }

  def getOcpcOpenTime(dayCnt: Int, date: String, hour: String, spark: SparkSession) = {
    /*
    从dl_cpc.ocpc_unit_list_hourly抽取每个单元最后一次打开oCPC的时间
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    // 抽取最后打开时间
    val sqlRequest =
      s"""
         |SELECT
         |    unit_id as unitid,
         |    conversion_goal,
         |    last_ocpc_opentime,
         |    to_date(last_ocpc_opentime) as ocpc_last_open_date,
         |    hour(last_ocpc_opentime) as ocpc_last_open_hour
         |FROM
         |    qttdw.dim_unit_ds
         |WHERE
         |    dt = '$date1'
         |AND
         |    is_ocpc = 1
         |AND
         |    last_ocpc_opentime is not null
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)

    val data = rawData
      .withColumn("ocpc_last_open_hour", udfConvertHour2String()(col("ocpc_last_open_hour")))
      .select("unitid", "conversion_goal", "last_ocpc_opentime", "ocpc_last_open_date", "ocpc_last_open_hour")
      .filter(s"ocpc_last_open_date = '$date1'")

    data.show(10)
//    data.write.mode("overwrite").saveAsTable("test.check_ocpc_charge20190425a")

    data
  }

  def udfConvertHour2String() = udf((hourInt: Int) => {
    var result = ""
    if (hourInt < 10) {
      result = "0" + hourInt.toString
    } else {
      result = hourInt.toString
    }
    result
  })
}