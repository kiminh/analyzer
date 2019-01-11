package com.cpc.spark.ocpcV3.HotTopicOcpc.model

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object OcpcCPAhistory {

  def main(args: Array[String]): Unit = {
    /*
    选取cpa_history的基本策略：
    1. 抽取基础表
    2. 分别计算该广告单元在趣头条上前一天的历史cpa，在米读小说上前一天的历史cpa以及行业类别的历史cpa
    3. 根据unitid和行业类别关联相关数据
    4. 如果趣头条上至少有一个类别的转化数，给定conversion_goal，如果趣头条上一个类别的转化数都没有，按照米读小说上的转化数给定cpa，如果两类都没有，默认转化目标为1
    5. 按照如下顺序根据转化目标选取合适的cpa：趣头条cpa->米读小说cpa->行业类别cpa
    6. 输出数据
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // 按照要求生成相关基础数据表
    val baseData     = getBaseData(            date, hour, spark)
    val qttData      = getQttCPA(    baseData, date, hour, spark)
    val hottopicData = getHotTopicCPA( baseData, date, hour, spark)
    val adclassData = getAdclassCPA(baseData, date, hour, spark).select("new_adclass", "cpa1" )
    val qttAlpha    = checkCPAhistory(qttData, 0.8, "qtt", date, hour, spark )
    val hottopicAlpha  = checkCPAhistory( hottopicData, 0.8, "hottopic", date, hour, spark )

    // 数据表关联
    val data = baseData   //与cpa2有关的都去掉
      .select("unitid", "new_adclass")
      .distinct()
      .join(     qttAlpha, Seq("unitid", "new_adclass"), "left_outer")
      .join(hottopicAlpha, Seq("unitid", "new_adclass"), "left_outer")
      .join(  adclassData, Seq("new_adclass"), "left_outer")
      .select("unitid", "new_adclass", "cpa1_history_qtt", "cpa1_history_hottopic", "cpa1")

    // 按照策略挑选合适的cpa以及确定对应的conversion_goal
    val result = getResult(data, date, hour, spark)
    val tableName = "dl_cpc.ocpc_cpa_history_hourly" // 改成一张没有人用的表 dl_cpc.ocpc_cpa_history_hourly
    result.write.mode("overwrite").insertInto(tableName)
    println(s"save data into table: $tableName")
  }

  def getBaseData(date: String, hour: String, spark: SparkSession) = {
    /*
    抽取基础表，只包括前一天在米读小说和趣头条上有记录的unitid和对应adclass
    */
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd" )
    val end_date = sdf.parse(date) //解析为java.util.Date
    val calendar = Calendar.getInstance
    calendar.setTime(end_date) //设置日期为end_date
    calendar.add(Calendar.DATE, -1) //将日期往前调1天
    val start_date = calendar.getTime //end_date - 1
    val date1 = sdf.format(start_date) //date1 = end_date - 1
    calendar.add(Calendar.DATE, -2) // 将日期再往前调2天
    val start_date2 = calendar.getTime //end_date - 3
    val date2 = sdf.format(start_date2) // date2 = end_date - 3
    val selectCondition = s"`date` between '$date2' and '$date1'"

    // 消费
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  media_appsid,
         |  total_price as total_cost,
         |  total_bid as total_bid,
         |  ctr_cnt
         |FROM
         |  dl_cpc.ocpcv3_ctr_data_hourly
         |WHERE
         |  $selectCondition
           and media_appsid in ('80000001', '80000002', '80002819')
       """.stripMargin
    println(sqlRequest1)
    val costData = spark
      .sql(sqlRequest1)
      .withColumn("new_adclass", col("adclass")/1000 )
      .withColumn("new_adclass", col("new_adclass").cast(IntegerType) )
      .groupBy("unitid", "new_adclass", "media_appsid" )
      .agg(
        sum( col("total_cost") ).alias("total_cost"),
        sum( col("total_bid")  ).alias("total_bid" ),
        sum( col("ctr_cnt")    ).alias("ctrcnt"    ) )
      .select("unitid", "new_adclass", "media_appsid", "total_cost", "total_bid", "ctrcnt")

    // 非api回传类转化数 ( ml_cvr_feature_v1 )
    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  media_appsid,
         |  cvr1_cnt --ml_cvr_feature_v1中sum(label)
         |FROM
         |  dl_cpc.ocpcv3_cvr1_data_hourly
         |WHERE
         |  $selectCondition
         |and media_appsid in ('80000001', '80000002', '80002819')
       """.stripMargin
    println(sqlRequest2)
    val cvr1Data = spark
      .sql(sqlRequest2)
      .withColumn("new_adclass", col("adclass")/1000 )
      .withColumn("new_adclass", col("new_adclass").cast(IntegerType) )
      .groupBy("unitid", "new_adclass", "media_appsid" )
      .agg( sum(col("cvr1_cnt")).alias("cvr1cnt") )
      .select("unitid", "new_adclass", "media_appsid", "cvr1cnt")

    // api回传类转化数(ml_cvr_feature_v2)
    //    val sqlRequest3 =
    //      s"""
    //         |SELECT
    //         |  unitid,
    //         |  adclass,
    //         |  media_appsid,
    //         |  cvr2_cnt
    //         |FROM
    //         |  dl_cpc.ocpcv3_cvr2_data_hourly
    //         |WHERE
    //         |  $selectCondition
    //       """.stripMargin
    //    println(sqlRequest3)
    //    val cvr2Data = spark
    //      .sql(sqlRequest3)
    //      .withColumn("new_adclass", col("adclass")/1000)
    //      .withColumn("new_adclass", col("new_adclass").cast(IntegerType))
    //      .groupBy("unitid", "new_adclass", "media_appsid")
    //      .agg(sum(col("cvr2_cnt")).alias("cvr2cnt"))
    //      .select("unitid", "new_adclass", "media_appsid", "cvr2cnt")

    // 关联数据构成基础表
    val resultDF = costData
      .join(cvr1Data, Seq("unitid", "new_adclass", "media_appsid"), "left_outer")
      //      .join(cvr2Data, Seq("unitid", "new_adclass", "media_appsid"), "left_outer")
      .select("unitid", "new_adclass", "media_appsid", "total_cost", "cvr1cnt",  // "cvr2cnt",
      "total_bid", "ctrcnt" )
      .withColumn("date", lit(date))  // lit: functions中的函数
      .withColumn("hour", lit(hour))

    resultDF

  }

  def getQttCPA(base: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    抽取趣头条cpa数据
     */
    val resultDF = base
      .filter(s"media_appsid in ('80000001', '80000002')" )
      .groupBy("unitid", "new_adclass" )
      .agg(
        sum(col("total_cost")).alias("cost"),
        sum(col("cvr1cnt")).alias("cvr1cnt"),
        //  sum(col("cvr2cnt")).alias("cvr2cnt"),
        sum(col("total_bid")).alias("bid"),
        sum(col("ctrcnt"   )).alias("ctrcnt"))
      .withColumn("cpa1", col("cost") * 1.0 / col("cvr1cnt") )
      // .withColumn("cpa2", col("cost") * 1.0 / col("cvr2cnt"))
      .withColumn("avg_bid", col("bid") * 1.0 / col("ctrcnt" ) )
      .withColumn("alpha1", col("cpa1") * 1.0 / col("avg_bid") )
      //.withColumn("alpha2", col("cpa2") * 1.0 / col("avg_bid"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
    resultDF
  }



  def getHotTopicCPA(base: DataFrame, date: String, hour: String, spark: SparkSession) = {  // todo
    /*
    抽取小说cpa数据
     */
    val resultDF = base
      .filter(s"media_appsid = '80002819'" )
    .groupBy("unitid", "new_adclass")
    .agg(
      sum(col("total_cost")).alias("cost"),
      sum(col("cvr1cnt")).alias("cvr1cnt"),
      sum(col("total_bid")).alias("bid"),
      sum(col("ctrcnt")).alias("ctrcnt"))
    .withColumn("cpa1", col("cost") * 1.0 / col("cvr1cnt"))
    .withColumn("avg_bid", col("bid") * 1.0 / col("ctrcnt"))
    .withColumn("alpha1", col("cpa1") * 1.0 / col("avg_bid"))
    .withColumn("alpha2", col("cpa2") * 1.0 / col("avg_bid"))
    .withColumn("date", lit(date)) //添加一列，取值全为date
    .withColumn("hour", lit(hour))

    resultDF
  }

  def getAdclassCPA(base: DataFrame, date: String, hour: String, spark: SparkSession) = {  // 与cvr2有关的都去掉
    /*
    抽取趣头条广告的行业类别cpa
     */
    val resultDF = base
      .filter(s"media_appsid in ('80000001', '80000002')")
      .groupBy("new_adclass")
      .agg(
        sum(col("total_cost")).alias("cost"),
        sum(col("cvr1cnt")).alias("cvr1cnt")  //,
        //        sum(col("cvr2cnt")).alias("cvr2cnt")
      )
      .withColumn("cpa1", col("cost") * 1.0/col("cvr1cnt") )
      //      .withColumn("cpa2", col("cost") * 1.0 / col("cvr2cnt"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    val adclassTable = "dl_cpc.ocpcv3_cpa_history_v2_adclass_hourly"
    //    resultDF.write.mode("overwrite").saveAsTable("test.ocpcv3_cpa_history_v2_adclass_hourly")
    resultDF.write.mode("overwrite").insertInto(adclassTable)
    resultDF
  }

  def checkCPAhistory(base: DataFrame, alpha: Double, media: String, date: String, hour: String, spark: SparkSession) = {  //与cvr2有关的去掉
    /*
     * 根据给定alpha对给定数据的cpa做上限限制（防止因为cvr过低带来的异常值）
     * alpha: 上限分位数
     */
    val cvr1Data = base.select("unitid", "new_adclass", "cvr1cnt", "cpa1", "avg_bid", "alpha1" )
    cvr1Data.createOrReplaceTempView("cvr1_table" )

    // 按照alpha取分位数
    val sqlRequest1 =
      s"""
         |SELECT
         |  new_adclass,
         |  percentile(alpha1, $alpha) as alpha1_max
         |FROM
         |  cvr1_table
         |WHERE
         |  cvr1cnt > 1
         |GROUP BY new_adclass
       """.stripMargin
    println(sqlRequest1)
    val alpha1Data = spark.sql(sqlRequest1)
    val cvr1alpha  = cvr1Data
      .join(alpha1Data, Seq("new_adclass"), "left_outer")
      .select("unitid", "new_adclass", "cvr1cnt", "cpa1", "avg_bid", "alpha1", "alpha1_max")
      .withColumn("cpa1_max", col("avg_bid") * col("alpha1_max") )
      .withColumn("cpa1_history_" + media, when( col("cpa1") > col("cpa1_max") && col("cpa1_max") > 0, col("cpa1_max") ).otherwise(col("cpa1")))
    val cvr1Final = cvr1alpha
      .select("unitid", "new_adclass", "cpa1_history_" + media )

    // 关联数据表
    val resultDF = cvr1Final
      //  .join(cvr2Final, Seq("unitid", "new_adclass"), "outer")
      .select("unitid", "new_adclass", "cpa1_history_" + media  ) //,  "cpa2_history_" + media)
    resultDF
  }

  def getResult(base: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    1. 确定转化目标
    2. 根据转化目标和cpa优先级选择最终cpa
     */

    val data =   base //rawData
      .withColumn("conversion_goal" , lit(1) )
      .withColumn("cpa_src_middle", when(col("cpa1_history_qtt").isNull, "hottopic").otherwise("qtt") )
      .withColumn("cpa_src",        when(col("cpa_src_middle")==="hottopic" && col("cpa1_history_hottopic").isNull, "adclass").otherwise(col("cpa_src_middle")))
      .withColumn("cpa_history",    when(col("cpa_src")==="qtt", col("cpa1_history_qtt")).otherwise(when(col("cpa_src")==="novel", col("cpa1_history_novel")).otherwise(col("cpa1"))))
      .withColumn("cpa_history",    when(col("cpa_history") > 50000, 50000).otherwise(col("cpa_history")))

    data
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpcv3_cpa_history_v2_final_middle") //查数据用的

    val resultDF = data
      .selectExpr("cast(unitid as string) identifier", "new_adclass", "cpa_src", "cpa_history", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("hottopic"))
    resultDF

  }

}
