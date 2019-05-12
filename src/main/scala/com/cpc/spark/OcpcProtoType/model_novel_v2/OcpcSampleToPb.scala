//package com.cpc.spark.OcpcProtoType.model_novel_v2
//
//import java.io.FileOutputStream
//
//import ocpcParams.ocpcParams.{OcpcParamsList,SingleItem}
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.LongType
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//import scala.collection.mutable.ListBuffer
//
//object OcpcSampleToPb {
//  def main(args: Array[String]): Unit = {
//    /*
//    pb文件格式：
//    string identifier = 1;
//    int32 conversiongoal = 2;
//    double kvalue = 3;
//    double cpagiven = 4;
//    int64 cvrcnt = 5;
//    对于明投广告，cpagiven=1， cvrcnt使用ocpc广告记录进行关联，k需要进行计算
//
//     */
//    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
//    Logger.getRootLogger.setLevel(Level.WARN)
//
//    // bash: 2019-01-02 12 qtt_demo 1
//    val date = args(0).toString
//    val hour = args(1).toString
//    val version = args(2).toString
//    val isKnown = args(3).toInt
//
//    println("parameters:")
//    println(s"date=$date, hour=$hour, version=$version, isKnown:$isKnown")
//
//    val NewK = getNewK(date, hour, version, spark)
//      .withColumn("cpagiven",lit(1.0))
//      .withColumn("cvrcnt",lit(30))
//      .withColumn("cvrcnt",col("cvrcnt").cast(LongType))
//      .select("identifier", "conversion_goal", "cpagiven", "cvrcnt", "kvalue", "flag", "pcoc", "jfb")
//
//    println("NewK")
//    println(NewK.count())
//    NewK.show(10)
//
//    val smoothData = NewK
//      .filter(s"flag = 1 and kvalue is not null")
//      .select("identifier", "pcoc", "jfb", "kvalue", "conversion_goal")
//
//    smoothData
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))
//      .withColumn("version", lit(version))
//      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_kvalue_smooth_strat")
//
//    // 获取postcvr数据
//    val cvr1 = getPostCvr(version, 1, date, hour, spark)
//    val cvr2 = getPostCvr(version, 2, date, hour, spark)
//    val cvr3 = getPostCvr(version, 3, date, hour, spark)
//    val cvr4 = getPostCvr(version, 4, date, hour, spark)
//
//    val cvrData = cvr1
//      .join(cvr2, Seq("identifier"), "outer")
//      .join(cvr3, Seq("identifier"), "outer")
//      .join(cvr4, Seq("identifier"), "outer")
//      .select("identifier", "cvr1", "cvr2", "cvr3", "cvr4")
//      .na.fill(0.0, Seq("cvr1", "cvr2", "cvr3", "cvr4"))
//
//    // 获取cali_value
//    val caliValue = getCaliValue(version, date, hour, spark)
//
//    // 组装数据
//    val result = assemblyData(cvrData, caliValue, spark)
//
//    val resultDF = result.
//
//    savePbPack(resultDF, version, isKnown)
//  }
//
//  def getConversionGoal(date: String, hour: String, spark: SparkSession) = {
//    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
//    val user = "adv_live_read"
//    val passwd = "seJzIPUc7xU"
//    val driver = "com.mysql.jdbc.Driver"
//    val table = "(select id, user_id, ideas, bid, ocpc_bid, ocpc_bid_update_time, cast(conversion_goal as char) as conversion_goal, status from adv.unit where ideas is not null) as tmp"
//
//    val data = spark.read.format("jdbc")
//      .option("url", url)
//      .option("driver", driver)
//      .option("user", user)
//      .option("password", passwd)
//      .option("dbtable", table)
//      .load()
//
//    val resultDF = data
//      .withColumn("unitid", col("id"))
//      .withColumn("userid", col("user_id"))
//      .withColumn("cv_flag", lit(1))
//      .selectExpr("cast(unitid as string) identifier",  "cast(conversion_goal as int) conversion_goal", "cv_flag")
//      .distinct()
//
//    resultDF.show(10)
//    resultDF
//  }
//
//  def getNewK(date: String, hour: String, version: String, spark: SparkSession) = {
//    /*
//    1. 从配置文件和dl_cpc.ocpc_pcoc_jfb_hourly表中抽取需要的jfb数据
//    2. 计算新的kvalue
//     */
//    // 从表中抽取数据
//    val selectCondition = s"`date` = '$date' and `hour` = '$hour'"
//    val sqlRequest =
//      s"""
//         |SELECT
//         |  identifier,
//         |  pcoc,
//         |  jfb,
//         |  1.0 / jfb as kvalue,
//         |  conversion_goal
//         |FROM
//         |  dl_cpc.ocpc_pcoc_jfb_hourly
//         |WHERE
//         |  $selectCondition
//         |AND
//         |  version = '$version'
//         |AND
//         |  pcoc > 0
//         |AND
//         |  jfb > 0
//       """.stripMargin
//
//    println(sqlRequest)
//    val data = spark.sql(sqlRequest)
//
//    val resultDF  =data
//      .select("identifier", "conversion_goal", "pcoc", "jfb", "kvalue")
//      .withColumn("flag", lit(1))
//      .select("identifier", "conversion_goal", "kvalue", "flag", "pcoc", "jfb")
//
//    resultDF
//
//  }
//
//
//  def savePbPack(dataset: DataFrame, version: String, isKnown: Int): Unit = {
//    var list = new ListBuffer[SingleItem]
//    var filename = ""
//    if (isKnown == 1) {
//      filename = s"Ocpc_" + version + "_known.pb"
//    } else {
//      filename = s"Ocpc_" + version + "_unknown.pb"
//    }
//    println("size of the dataframe")
//    println(dataset.count)
//    println(s"filename: $filename")
//    dataset.show(10)
//    dataset.printSchema()
//    var cnt = 0
//
//    for (record <- dataset.collect()) {
//      val identifier = record.getAs[String]("identifier")
//      val cpaGiven = record.getAs[Double]("cpagiven")
//      val kvalue = record.getAs[Double]("kvalue")
//      val cvrCnt = record.getAs[Long]("cvrcnt")
//      val conversionGoal = record.getAs[Int]("conversion_goal")
//
//      if (cnt % 100 == 0) {
//        println(s"identifier:$identifier, conversionGoal:$conversionGoal, cpaGiven:$cpaGiven, kvalue:$kvalue, cvrCnt:$cvrCnt")
//      }
//      cnt += 1
//
//      //      string identifier = 1;
//      //      int32 conversiongoal = 2;
//      //      double kvalue = 3;
//      //      double cpagiven = 4;
//      //      int64 cvrcnt = 5;
//
//      val currentItem = SingleItem(
//        identifier = identifier,
//        conversiongoal = conversionGoal,
//        kvalue = kvalue,
//        cpagiven = cpaGiven,
//        cvrcnt = cvrCnt
//      )
//      list += currentItem
//
//    }
//    val result = list.toArray[SingleRecord]
//    val adRecordList = OcpcList(
//      adrecord = result
//    )
//
//    println("length of the array")
//    println(result.length)
//    adRecordList.writeTo(new FileOutputStream(filename))
//
//    println("complete save data into protobuffer")
//
//  }
//  def assemblyData(cvrData: DataFrame, caliValue: DataFrame, spark: SparkSession) = {
//    /*
//      identifier      string  NULL
//      min_bid double  NULL
//      cvr1    double  NULL
//      cvr2    double  NULL
//      cvr3    double  NULL
//      min_cpm double  NULL
//      factor1 double  NULL
//      factor2 double  NULL
//      factor3 double  NULL
//      cpc_bid double  NULL
//      cpa_suggest     double  NULL
//      param_t double  NULL
//      cali_value      double  NULL
//     */
//    val data = cvrData
//      .withColumn("factor1", lit(0.5))
//      .withColumn("factor2", lit(0.5))
//      .withColumn("factor3", lit(0.5))
//      .withColumn("factor4", lit(0.5))
//      .withColumn("cpc_bid", lit(0))
//      .withColumn("cpa_suggest", lit(0))
//      .withColumn("param_t", lit(0))
//      .join(caliValue, Seq("identifier"), "left_outer")
//      .select("identifier", "cvr1", "cvr2", "cvr3","cvr4", "factor1", "factor2", "factor3", "factor4","cpc_bid", "cpa_suggest", "param_t", "cali_value")
//      .withColumn("min_bid", lit(0))
//      .withColumn("min_cpm", lit(0))
//      .na.fill(0, Seq("min_bid", "min_cpm", "cpc_bid", "cpa_suggest", "param_t"))
//      .na.fill(0.0, Seq("cvr1", "cvr2", "cvr3", "cvr4"))
//      .na.fill(1.0, Seq("cali_value"))
//      .selectExpr("identifier", "cast(min_bid as double) min_bid", "cvr1", "cvr2", "cvr3","cvr4", "cast(min_cpm as double) as min_cpm", "cast(factor1 as double) factor1", "cast(factor2 as double) as factor2", "cast(factor3 as double) factor3", "cast(factor4 as double) factor4","cast(cpc_bid as double) cpc_bid", "cpa_suggest", "param_t", "cali_value")
//
//    // 如果cali_value在1/1.3到1.3之间，则factor变成0.2
//    val result = data
//      .withColumn("factor3", udfSetFactor3()(col("factor3"), col("cali_value")))
//
//    result
//  }
//
//  def getPostCvr(version: String, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
//    val cvrType = "cvr" + conversionGoal.toString
//    val sqlRequest =
//      s"""
//         |SELECT
//         |  identifier,
//         |  post_cvr
//         |FROM
//         |  dl_cpc.ocpc_pcoc_jfb_hourly
//         |WHERE
//         |  `date` = '$date'
//         |AND
//         |  `hour` = '$hour'
//         |AND
//         |  version = '$version'
//         |AND
//         |  conversion_goal = $conversionGoal
//       """.stripMargin
//    println(sqlRequest)
//    val data = spark.sql(sqlRequest)
//
//    val resultDF = data
//      .select("identifier", "post_cvr")
//      .groupBy("identifier")
//      .agg(avg(col("post_cvr")).alias(cvrType))
//      .select("identifier", cvrType)
//
//    resultDF
//  }
//
//  def getCaliValue(version: String, date: String, hour: String, spark: SparkSession) = {
//    val sqlRequest =
//      s"""
//         |SELECT
//         |  identifier,
//         |  1.0 / pcoc as cali_value,
//         |  jfb,
//         |  kvalue,
//         |  conversion_goal,
//         |  version,
//         |  row_number() over (partition by identifier order by conversion_goal) as seq
//         |FROM
//         |  dl_cpc.ocpc_kvalue_smooth_strat
//         |WHERE
//         |  `date` = '$date'
//         |AND
//         |  `hour` = '$hour'
//         |AND
//         |  version = '$version'
//         |AND
//         |  pcoc is not null
//         |AND
//         |  jfb is not null
//       """.stripMargin
//    println(sqlRequest)
//    val rawData = spark.sql(sqlRequest)
//      .filter(s"seq = 1")
//      .select("identifier", "conversion_goal", "cali_value")
//
//    val result = rawData
//      .groupBy("identifier")
//      .agg(avg(col("cali_value")).alias("cali_value"))
//      .select("identifier", "cali_value")
//
//    result
//  }
//
//  def udfSetFactor3() = udf((factor3: Double, caliValue: Double) => {
//    var factor = 0.5
//    if (caliValue <= 1.3 && caliValue >= 0.769) {
//      factor = 0.2
//    } else if (caliValue <= 2.0 && caliValue >= 0.5) {
//      factor = 0.35
//    } else {
//      factor = 0.5
//    }
//    factor
//  })
//
//}
//
