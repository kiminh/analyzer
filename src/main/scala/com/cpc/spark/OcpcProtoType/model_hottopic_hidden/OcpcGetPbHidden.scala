package com.cpc.spark.OcpcProtoType.model_hottopic_hidden
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

    val data1raw = getPcocJFB(version, 48, conversionGoal, date, hour, spark)
    val data1 = data1raw
      .withColumn("pcoc1", col("pcoc"))
      .withColumn("jfb1", col("jfb"))
      .withColumn("post_cvr1", col("post_cvr"))
      .select("identifier", "pcoc1", "jfb1", "post_cvr1")
    val data2raw = getPcocJFB(version, 72, conversionGoal, date, hour, spark)
    val data2 = data2raw
      .withColumn("pcoc2", col("pcoc"))
      .withColumn("jfb2", col("jfb"))
      .withColumn("post_cvr2", col("post_cvr"))
      .select("identifier", "pcoc2", "jfb2", "post_cvr2")

    val data = data1
      .join(data2, Seq("identifier"), "outer")
      .select("identifier", "pcoc1", "jfb1", "post_cvr1", "pcoc2", "jfb2", "post_cvr2")
      .na.fill(0.0, Seq("pcoc1", "jfb1", "post_cvr1", "pcoc2", "jfb2", "post_cvr2"))
      .withColumn("flag", when(col("pcoc1") > 0 && col("jfb1") > 0 && col("post_cvr1") > 0, 1).otherwise(2))

    val resultDF = data
        .withColumn("pcoc", udfSelectByFlag()(col("pcoc1"), col("pcoc2"), col("flag")))
        .withColumn("jfb", udfSelectByFlag()(col("jfb1"), col("jfb2"), col("flag")))
        .withColumn("post_cvr", udfSelectByFlag()(col("post_cvr1"), col("post_cvr2"), col("post_cvr")))
        .select("identifier", "pcoc", "jfb", "post_cvr")
        .withColumn("conversion_goal", lit(conversionGoal))
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("version", lit(version))

    resultDF
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_pb_result_hourly_20190303")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_result_hourly_v2")

  }

//  def udfSelectByFlag() = udf((value1: Double, value2: Double, flag: Int) => {
//    var result = 0.0
//    if (flag == 1) {
//      result = value1
//    } else {
//      result = value2
//    }
//    result
//  })
//
//  def getPcocJFB(version: String, hourInt: Int, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
//    /*
//    抽取数据
//     */
//    val realVersion = version + "_" + hourInt.toString
//    val sqlRequest =
//      s"""
//         |SELECT
//         |  identifier,
//         |  pcoc,
//         |  jfb,
//         |  post_cvr
//         |FROM
//         |  dl_cpc.ocpc_pcoc_jfb_hourly
//         |WHERE
//         |  `date` = '$date'
//         |AND
//         |  `hour` = '$hour'
//         |AND
//         |  version = '$realVersion'
//         |AND
//         |  conversion_goal = $conversionGoal
//       """.stripMargin
//    println(sqlRequest)
//    val data = spark.sql(sqlRequest)
//    data.show(10)
//
//    data
//
//  }

}


