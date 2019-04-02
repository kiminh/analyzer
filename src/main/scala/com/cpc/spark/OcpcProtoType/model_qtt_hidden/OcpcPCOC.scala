package com.cpc.spark.OcpcProtoType.model_qtt_hidden

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{avg, col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Wang Jun
  * @date 2019/3/29 21:13
  */
object OcpcPCOC {
    def main(args: Array[String]): Unit = {
        val date = args(0).toString
        val hour = args(1).toString
        val hourInt = args(2).toInt
        val conversionGoal = args(3).toInt
        val version = args(4).toString
        val media = args(5).toString
        val spark = SparkSession.builder()
          .appName(s"OcpcPCOC date = $date, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()

        // 抽取基础数据表
        val baseData = getBaseData(media, hourInt, date, hour, spark)
        // 抽取click的基础数据
        val clickData = getClickData(baseData, date, hour, spark)
        val jfbData = clickData.select("unitid", "jfb")
        //抽取iscvr的转化数据
        val cvData = getCvData(media, hourInt, date, hour, conversionGoal, baseData, spark)
        // cvr平滑后的pcoc
        val pcocData = calculatePCOC(baseData, clickData, cvData, conversionGoal, spark)
        // 计算pcoc与jfb
        val result = calculateK(jfbData, pcocData, spark)
        val resultDF = result
          .select("identifier", "kvalue")
          .withColumn("conversion_goal", lit(conversionGoal))
          .withColumn("date", lit(date))
          .withColumn("hour", lit(hour))
          .withColumn("version", lit(version))
          .withColumn("method", lit("pcoc"))
        resultDF
          .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_k_pcoc_hourly20190402")
//          .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_k_model_hourly")
    }
    def getBaseData(media: String, hourInt: Int, date: String, hour: String, spark: SparkSession) = {
        // 抽取基础数据
        // 媒体选择
        val conf = ConfigFactory.load("ocpc")
        val conf_key = "medias." + media + ".media_selection"
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
        val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

        val sqlRequest =
            s"""
               |SELECT
               |  searchid,
               |  unitid,
               |  isshow,
               |  isclick,
               |  bid as original_bid,
               |  price,
               |  exp_cvr,
               |  ocpc_log
               |FROM
               |  dl_cpc.ocpc_base_unionlog
               |WHERE
               |  $selectCondition
               |AND
               |  $mediaSelection
       """.stripMargin
        println(sqlRequest)
        val base = spark
          .sql(sqlRequest)
          .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

        base.createOrReplaceTempView("base_table")
        val sqlRequestBase =
            s"""
               |select
               |    searchid,
               |    unitid,
               |    price,
               |    original_bid,
               |    cast(exp_cvr as double) as exp_cvr,
               |    isclick,
               |    isshow,
               |    ocpc_log,
               |    ocpc_log_dict,
               |    (case when length(ocpc_log)>0 then cast(ocpc_log_dict['dynamicbid'] as int) else original_bid end) as bid
               |from base_table
       """.stripMargin
        println(sqlRequestBase)
        val rawData = spark.sql(sqlRequestBase)

        val resultDF = rawData.select("searchid", "unitid", "price", "exp_cvr", "isclick", "isshow", "bid")
        resultDF
    }

    def getClickData(baseData: DataFrame, date: String, hour: String, spark: SparkSession) = {
        /*
        抽取click基础数据：isclick, exp_cvr, price, bid
         */

        baseData.createOrReplaceTempView("raw_table")
        // 展现数、点击数、花费
        val sqlRequest1 =
            s"""
               |SELECT
               |  unitid,
               |  SUM(case when isclick=1 then price else 0 end) as total_price,
               |  SUM(isclick) as click,
               |  SUM(case when isclick=1 then bid else 0 end) as total_bid,
               |  SUM(case when isclick=1 then exp_cvr else 0 end) as total_pcvr
               |FROM
               |  raw_table
               |GROUP BY unitid
       """.stripMargin
        println(sqlRequest1)
        val data = spark.sql(sqlRequest1)

        val resultDF = data
          .select("unitid", "total_price", "click", "total_bid", "total_pcvr")
          .withColumn("jfb", col("total_price") * 1.0 / col("total_bid"))
        resultDF.show(10)

        resultDF
    }
    def getCvData(media: String, hourInt: Int, date: String, hour: String, conversionGoal: Int, baseData: DataFrame, spark: SparkSession) = {
        // 媒体选择
        val conf = ConfigFactory.load("ocpc")
        val conf_key = "medias." + media + ".media_selection"
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
        val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

        baseData.createOrReplaceTempView("base_data")

        val cvr_goal = if(conversionGoal==1)
            "cvr1"
        else if(conversionGoal==2)
            "cvr2"
        else if(conversionGoal==3)
            "cvr3"
        else
            "cvr4"

        val sql =
            s"""
               |select unitid,
               |    sum(if(b.label is not null, 1, 0)) as cv
               |from
               |(
               |    select searchid, unitid
               |    from base_data
               |) a
               |left outer join
               |(
               |    select searchid, label
               |    from
               |        dl_cpc.ocpc_label_cvr_hourly
               |    where
               |        `date` >= '$date1'
               |    and
               |        cvr_goal = "$cvr_goal"
               |) b
               |on a.searchid = b.searchid
               |group by a.unitid
             """.stripMargin

        val resultDF = spark.sql(sql)
        resultDF.show(10)
        resultDF
    }
    def calculatePCOC(baseData: DataFrame, clickData: DataFrame, cvData: DataFrame, conversionGoal: Int, spark: SparkSession) = {
        val cvrData = clickData
          .join(cvData, Seq("unitid"), "left_outer")
          .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
          .select("unitid", "post_cvr")

        val p1 = if (conversionGoal == 1 || conversionGoal == 3) 0.8 else 0.5
        val p2 = 1 - p1

        val pcvrData = baseData
          .join(cvrData, Seq("unitid"), "left_outer")
          .withColumn("pre_cvr", col("exp_cvr") * p1 + col("post_cvr") * p2)
          .filter(s"isclick = 1")
          .select("searchid", "unitid", "pre_cvr")
          .groupBy("unitid")
          .agg(avg(col("pre_cvr")).alias("pre_cvr"))
          .select("unitid", "pre_cvr")

        val pcocData = pcvrData
          .join(cvrData, Seq("unitid"), "left_outer")
          .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))

        pcocData
    }
    def calculateK(jfbData: DataFrame, pcocData: DataFrame, spark: SparkSession) = {
        val data = jfbData
          .join(pcocData, Seq("unitid"), "outer")

        data.show(10)
        data.createOrReplaceTempView("base_data")
        val sqlRequest =
            s"""
               |SELECT
               |  cast(unitid as string) identifier,
               |  1.0 / (pcoc * jfb) as kvalue
               |FROM
               |  base_data
       """.stripMargin
        println(sqlRequest)
        val resultDF = spark.sql(sqlRequest)

        resultDF
    }
}
