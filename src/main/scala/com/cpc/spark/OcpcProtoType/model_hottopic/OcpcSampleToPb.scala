package com.cpc.spark.OcpcProtoType.model_hottopic

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar
import com.cpc.spark.common.Utils.getTimeRangeSql
import com.cpc.spark.ocpc.OcpcUtils.{getTimeRangeSql2, getTimeRangeSql3}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import ocpc.ocpc.{OcpcList, SingleRecord}
import scala.collection.mutable.ListBuffer


object OcpcSampleToPb {
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

    // bash: 2019-01-02 12 novel_v2 1
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val isKnown = args(3).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, isKnown:$isKnown")
    val resultDF  = getPbData(version, date, hour, spark)

    resultDF
        .withColumn("version", lit(version))
        .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue", "version")
        .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_prev_pb_once20190215")
//        .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_prev_pb_once")

    resultDF
      .select("identifier", "cpagiven", "cvrcnt", "kvalue", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("hottopic_v3"))
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_pb_result_hourly_v2")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_result_hourly_v2")


    savePbPack(resultDF, version, isKnown)
  }

  def getPbData(version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    string identifier = 1;
    int32 conversiongoal = 2;
    double kvalue = 3;
    double cpagiven = 4;
    int64 cvrcnt = 5;
     */
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
    val result1 = spark.sql(sqlRequest)

    val result2 = getCPAgivenFromSuggest("hottopic_test", 1, date, hour, spark)

    val resultJoin = result1
        .join(result2, Seq("identifier", "conversion_goal"), "inner")
        .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue")
        .withColumn("conversion_goal", lit(0))

    val resultNew = result1
      .join(result2, Seq("identifier", "conversion_goal"), "left_outer")
      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue")
      .na.fill(1.0, Seq("cpagiven"))

    val resultDF = resultNew.union(resultJoin)

    resultDF.printSchema()
    resultDF.show(10)

    resultDF
  }

  def getCPAgivenFromSuggest(version: String, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val startdate = calendar.getTime
    val date1 = dateConverter.format(startdate)

    val sqlRequest =
      s"""
         |SELECT
         |    cast(unitid as string) as identifier,
         |    conversion_goal,
         |    cpa * 1.0 as cpagiven
         |FROM
         |    dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |WHERE
         |    date = '$date1'
         |AND
         |    `hour` = '06'
         |and is_recommend = 1
         |and version = '$version'
         |and industry = 'feedapp'
         |and conversion_goal = $conversionGoal
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
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

