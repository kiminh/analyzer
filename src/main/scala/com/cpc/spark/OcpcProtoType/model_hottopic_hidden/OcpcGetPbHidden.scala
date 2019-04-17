package com.cpc.spark.OcpcProtoType.model_hottopic_hidden

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
//import com.cpc.spark.ocpcV3.ocpc.OcpcUtils.getTimeRangeSql2
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.OcpcProtoType.model_v3.OcpcGetPbHidden._


object OcpcGetPbHidden {
  def main(args: Array[String]): Unit = {
    /*
    pb文件格式：
    string identifier = 1;
    int32 conversiongoal = 2;
    double kvalue = 3;
    double cpagiven = 4;
    int64 cvrcnt = 5;
    对于明投广告，cpagiven=1， cvrcnt使用ocpc广告记录进行关联，k需要进行计算，每个conversiongoal都需要进行计算
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // 计算日期周期
    // bash: 2019-01-02 12 1 hottopic_test hottopic
    val date = args(0).toString
    val hour = args(1).toString
    val conversionGoal = args(2).toInt
    val version = args(3).toString
    val media = args(4).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, conversionGoal=$conversionGoal, version=$version, media=$media")
    var mediaSelection = s"media_appsid in ('80000001', '80000002')"
    if (media == "qtt") {
      mediaSelection = s"media_appsid in ('80000001', '80000002')"
    } else if (media == "novel") {
      mediaSelection = s"media_appsid in ('80001098','80001292')"
    } else {
      mediaSelection = s"media_appsid = '80002819'"
    }

    // 明投：可以有重复identifier
    val base = getBaseData(mediaSelection, conversionGoal, date, hour, spark)
    val kvalue = getKvalue(conversionGoal, version, date, hour, spark)

    val result = base
      .withColumn("cvrcnt", lit(0))
      .join(kvalue, Seq("identifier"), "inner")
      .withColumn("conversion_goal", lit(conversionGoal))
      .select("identifier", "conversion_goal", "cvrcnt", "kvalue")
      .na.fill(0, Seq("cvrcnt", "kvalue"))
      .withColumn("kvalue", when(col("kvalue") > 15.0, 15.0).otherwise(col("kvalue")))


    val resultDF = result
        .withColumn("cpagiven", lit(1))
        .select("identifier", "cpagiven", "cvrcnt", "kvalue")
        .withColumn("conversion_goal", lit(conversionGoal))
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("version", lit(version))

    resultDF
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_pb_result_hourly_20190303")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_result_hourly_v2")

  }

}


