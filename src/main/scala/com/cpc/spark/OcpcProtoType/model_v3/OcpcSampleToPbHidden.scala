package com.cpc.spark.OcpcProtoType.model_v3

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils.getTimeRangeSql3
import ocpc.ocpc.{OcpcList, SingleRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer


object OcpcSampleToPbHidden {
  def main(args: Array[String]): Unit = {
    /*
    pb文件格式：
    string identifier = 1;
    int32 conversiongoal = 2;
    double kvalue = 3;
    double cpagiven = 4;
    int64 cvrcnt = 5;
    对于明投广告，cpagiven=1， cvrcnt使用ocpc广告记录进行关联，k需要进行计算

    将文件从dl_cpc.ocpc_pb_result_hourly_v2表中抽出，存入pb文件，需要过滤条件：
    kvalue>0
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // bash: 2019-01-02 12 hottopic_test 1
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val isKnown = args(3).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, isKnown:$isKnown")
    val rawData = getPbData(version, date, hour, spark)
    val suggestUnit = getListFromSuggest("feedapp", 1, version, date, hour, spark)
    val result = rawData
      .join(suggestUnit, Seq("identifier", "conversion_goal"), "inner")
      .select("identifier", "conversion_goal")

    val result2raw = getNewK(date, hour, version, spark)
    val ocpcUnit = getConversionGoal(date, hour, spark)
    val result2 = result2raw
      .join(ocpcUnit, Seq("identifier", "conversion_goal"), "left_outer")
      .select("identifier", "conversion_goal", "kvalue2", "flag", "pcoc", "jfb", "cv_flag")
      .filter(s"cv_flag is not null")
      .select("identifier", "conversion_goal", "kvalue2", "flag", "pcoc", "jfb")
    println("result2")
    result2.show(10)
    val result = result1
      .join(result2, Seq("identifier", "conversion_goal"), "left_outer")
      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue1", "kvalue2", "flag", "pcoc", "jfb")
      .withColumn("kvalue", when(col("flag") === 1 && col("kvalue2").isNotNull, col("kvalue2")).otherwise(col("kvalue1")))

    println("result")
    result.show(10)
    val smoothData = result
      .filter(s"flag = 1 and kvalue2 is not null")
      .select("identifier", "pcoc", "jfb", "kvalue", "conversion_goal")

    smoothData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
//      .repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_kvalue_smooth_strat")
      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_kvalue_smooth_strat")

    val resultDF = result
      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue")

    resultDF
        .withColumn("version", lit(version))
        .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue", "version")
//        .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_prev_pb_once20190310")
        .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_prev_pb_once")

    savePbPack(resultDF, version, isKnown)
  }

  def getListFromSuggest(industry: String, conversionGoal: Int, version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    从推荐cpa表中按照版本号抽取数据：dl_cpc.ocpc_suggest_cpa_recommend_hourly_v2
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |  cast(unitid as string) identifier,
         |  cpa,
         |  conversion_goal,
         |  cost,
         |  row_number() over(partition by unitid order by cost desc) as seq
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_recommend_hourly_v2
         |WHERE
         |  `date` = '$date1'
         |AND
         |  `hour` = '06'
         |AND
         |  version = '$version'
         |AND
         |  industry = '$industry'
         |AND
         |  cv_goal = $conversionGoal
         |AND
         |  is_recommend = 1
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
        .sql(sqlRequest)
        .filter(s"seq = 1")
        .select("identifier", "cpa", "conversion_goal")

    resultDF.show(10)
    resultDF
  }


  def getPbData(version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    string identifier = 1;
    int32 conversiongoal = 2;
    double kvalue = 3;
    double cpagiven = 4;
    int64 cvrcnt = 5;
    1. 从dl_cpc.ocpc_pb_result_hourly_v2中抽取数据
    2. 按照实验配置文件给出cpagiven
     */
    // 从dl_cpc.ocpc_pb_result_hourly_v2中抽取数据
    val selectCondition = s"`date`='$date' and `hour`='$hour' and version='$version'"

    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  conversion_goal,
         |  kvalue,
         |  cvrcnt
         |FROM
         |  dl_cpc.ocpc_pb_result_hourly_v2
         |WHERE
         |  $selectCondition
         |AND
         |  kvalue > 0
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val result = data
    result.printSchema()
    result.show(10)

    val resultDF = result.select("identifier", "conversion_goal", "kvalue", "cvrcnt")


    resultDF
  }

  def savePbPack(dataset: DataFrame, version: String, isKnown: Int): Unit = {
    var list = new ListBuffer[SingleRecord]
    var filename = ""
    if (isKnown == 1) {
      filename = s"Ocpc_" + version + "_known.pb"
    } else {
      filename = s"Ocpc_" + version + "_unknown.pb"
    }
    println("size of the dataframe")
    println(dataset.count)
    println(s"filename: $filename")
    dataset.show(10)
    dataset.printSchema()
    var cnt = 0

    for (record <- dataset.collect()) {
      val identifier = record.getAs[String]("identifier")
      val cpaGiven = record.getAs[Double]("cpagiven")
      val kvalue = record.getAs[Double]("kvalue")
      val cvrCnt = record.getAs[Long]("cvrcnt")
      val conversionGoal = record.getAs[Int]("conversion_goal")

      if (cnt % 100 == 0) {
        println(s"identifier:$identifier, conversionGoal:$conversionGoal, cpaGiven:$cpaGiven, kvalue:$kvalue, cvrCnt:$cvrCnt")
      }
      cnt += 1

      //      string identifier = 1;
      //      int32 conversiongoal = 2;
      //      double kvalue = 3;
      //      double cpagiven = 4;
      //      int64 cvrcnt = 5;

      val currentItem = SingleRecord(
        identifier = identifier,
        conversiongoal = conversionGoal,
        kvalue = kvalue,
        cpagiven = cpaGiven,
        cvrcnt = cvrCnt
      )
      list += currentItem

    }
    val result = list.toArray[SingleRecord]
    val adRecordList = OcpcList(
      adrecord = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(filename))

    println("complete save data into protobuffer")

  }

}

