package com.cpc.spark.OcpcProtoType.charge

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcCharge {
  def main(args: Array[String]): Unit = {
    /*
    根据最近四天有投放oCPC广告的广告单元各自的消费时间段的消费数据统计是否超成本和赔付数据
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val dayCnt = args(4).toInt

    val ocpcOpenTime = getOcpcOpenTime(3, date, hour, spark)
//    ocpcOpenTime.write.mode("overwrite").saveAsTable("test.check_ocpc_charge20190418a")
    val baseData = getOcpcData(media, dayCnt, date, hour, spark)

    val costData = assemblyData(dayCnt, baseData, ocpcOpenTime, date, hour, spark)
    costData.write.mode("overwrite").saveAsTable("test.ocpc_charge_daily20190419")

//    val prevData = getPrevData(date, hour, spark)

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
         |  cvr_goal = 'cvr3'
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
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as cpagiven
         |FROM
         |  base_data
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest2)
    val summaryData1 = spark
      .sql(sqlRequest2)
      .withColumn("pred_cost", col("cv") * col("cpagiven") * 1.2)
      .withColumn("pay", udfCalculatePay()(col("cost"), col("pred_cost")))

    val summaryData2 = baseData
      .filter(s"seq = 1")
      .select("unitid", "ocpc_time")

    val summaryData = summaryData1
      .join(summaryData2, Seq("unitid"), "left_outer")
      .select("unitid", "cost", "cv", "pay", "ocpc_time")

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
         |  row_number() over(partition by unitid order by timestamp) as seq,
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
         |AND
         |  (cast(adclass as string) like "134%" or cast(adclass as string) like "107%")
       """.stripMargin
    println(sqlRequest)
    val data = spark
        .sql(sqlRequest)
        .filter(s"is_hidden = 0 and conversion_goal = 3")

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