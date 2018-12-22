

package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import userprofile.Userprofile.UserProfile

import scala.collection.mutable.ListBuffer
import userocpc.userocpc._
import java.io.FileOutputStream

import com.cpc.spark.common.Utils.getTimeRangeSql
import com.cpc.spark.ocpc.OcpcUtils.getActData
import com.cpc.spark.ocpc.utils.OcpcUtils._
import org.apache.spark.sql.functions._



object OcpcSampleToRedis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val end_date = args(0)
    val hour = args(1)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(end_date)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val start_date = sdf.format(dt)
    val selectCondition = getTimeRangeSql(start_date, hour, end_date, hour)

    // 累积计算最近一周数据
    val sqlRequest =
      s"""
         |SELECT
         |  userid,
         |  ideaid,
         |  adclass,
         |  date,
         |  hour,
         |  SUM(cost) as cost,
         |  SUM(ctr_cnt) as ctr_cnt,
         |  SUM(cvr_cnt) as cvr_cnt,
         |  SUM(total_cnt) as total_cnt
         |FROM
         |  dl_cpc.ocpc_uid_userid_track_label2
         |WHERE $selectCondition
         |GROUP BY userid, ideaid, adclass, date, hour
       """.stripMargin
    println(sqlRequest)

    val rawBase = spark.sql(sqlRequest)

    rawBase.createOrReplaceTempView("base_table")

    val ocpcIdeas = getIdeaUpdates(spark)
//    ocpcIdeas.createOrReplaceTempView("ocpc_idea_update_time")
//    ocpcIdeas.write.mode("overwrite").saveAsTable("dl_cpc.ocpcv3_ideaid_list")
    ocpcIdeas.write.mode("overwrite").saveAsTable("test.ocpcv3_ideaid_list_" + hour)

    val base = rawBase.select("userid", "ideaid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "total_cnt")

    // 按ideaid求和
    val userData = base
      .groupBy("userid", "ideaid", "adclass")
      .agg(sum("cost").alias("cost"), sum("ctr_cnt").alias("user_ctr_cnt"), sum("cvr_cnt").alias("user_cvr_cnt"))
      .select("ideaid", "userid", "adclass", "cost", "user_ctr_cnt", "user_cvr_cnt")


//    userData.write.mode("overwrite").saveAsTable("test.ocpc_data_userdata_bak")
    userData.createOrReplaceTempView("ocpc_data_userdata")


    // 按adclass求和
    val adclassData = userData
      .groupBy("adclass")
      .agg(sum("cost").alias("adclass_cost"), sum("user_ctr_cnt").alias("adclass_ctr_cnt"), sum("user_cvr_cnt").alias("adclass_cvr_cnt"))
      .select("adclass", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt")

//    adclassData.write.mode("overwrite").saveAsTable("test.ocpc_data_adclassdata_bak")
    adclassData.createOrReplaceTempView("ocpc_data_adclassdata")


    // 关联adclass和ideaid
    val useridAdclassData = spark.sql(
      s"""
         |SELECT
         |    a.ideaid,
         |    a.userid,
         |    a.adclass,
         |    a.cost,
         |    a.user_ctr_cnt as ctr_cnt,
         |    a.user_cvr_cnt as cvr_cnt,
         |    b.adclass_cost,
         |    b.adclass_ctr_cnt,
         |    b.adclass_cvr_cnt
         |FROM
         |    ocpc_data_userdata a
         |INNER JOIN
         |    ocpc_data_adclassdata b
         |ON
         |    a.adclass=b.adclass
       """.stripMargin)

    useridAdclassData.createOrReplaceTempView("useridTable")

    // 生成中间表
    val sqlRequest2 =
      s"""
         |SELECT
         |    ideaid,
         |    userid,
         |    adclass,
         |    cost,
         |    ctr_cnt,
         |    cvr_cnt,
         |    adclass_cost,
         |    adclass_ctr_cnt,
         |    adclass_cvr_cnt,
         |    '$end_date' as date,
         |    '$hour' as hour
         |FROM
         |    useridTable
       """.stripMargin


    val userFinalData = spark.sql(sqlRequest2)
    userFinalData.write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_result_table")


    val cvr3Data = getCvr3List(end_date, hour, spark)

    val userFinalData4 = userFinalData.join(cvr3Data, Seq("ideaid", "adclass"), "left_outer")


//    userFinalData4.write.mode("overwrite").saveAsTable("test.ocpc_new_cvr_table_bak")
    userFinalData4.createOrReplaceTempView("ocpc_new_cvr_table")


    // 根据中间表加入k值
    val pidKtable = "test.ocpc_k_value_table_" + hour
    val sqlRequest3 =
      s"""
         |SELECT
         |  a.ideaid,
         |  a.userid,
         |  a.adclass,
         |  a.cost,
         |  a.ctr_cnt,
         |  a.cvr_cnt,
         |  a.adclass_cost,
         |  a.adclass_ctr_cnt,
         |  a.adclass_cvr_cnt,
         |  b.k_value,
         |  (case when a.cvr3_cnt is null then 0 else a.cvr3_cnt end) as cvr3_cnt
         |FROM
         |  (SELECT
         |    ideaid,
         |    userid,
         |    adclass,
         |    cost,
         |    ctr_cnt,
         |    cvr_cnt,
         |    adclass_cost,
         |    adclass_ctr_cnt,
         |    adclass_cvr_cnt,
         |    cast(cvr3_cnt as bigint) as cvr3_cnt
         |   FROM
         |    ocpc_new_cvr_table) a
         |LEFT JOIN
         |   (SELECT ideaid, adclass, cast(k_value as double) as k_value FROM $pidKtable) as b
         |ON
         |   a.ideaid=b.ideaid
         |and
         |   a.adclass=b.adclass
       """.stripMargin

    println(sqlRequest3)

    val userFinalData2 = spark.sql(sqlRequest3)

    userFinalData2.show(10)

    userFinalData2.createOrReplaceTempView("test_new_pb_ocpc")



    userFinalData2
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "k_value")
      .withColumn("date", lit(end_date))
      .withColumn("hour", lit(hour))
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_pb_result_table_v1_new")



    val hpcvrTable = calculateHPCVR(end_date, hour, spark)
    hpcvrTable.createOrReplaceTempView("ocpc_hpcvr_total")

    // TODO 删除cvr3cali的数据表引用
    val pcvrCaliTable = "test.ocpc_new_calibration_value_" + hour
    val pactCaliTable = "test.ocpc_new_calibration_value_cvr3_" + hour
    val sqlRequest4 =
      s"""
         |SELECT
         |  a.ideaid,
         |  a.userid,
         |  a.adclass,
         |  a.cost,
         |  a.ctr_cnt,
         |  a.cvr_cnt,
         |  a.adclass_cost,
         |  a.adclass_ctr_cnt,
         |  a.adclass_cvr_cnt,
         |  (case when a.k_value is null then 0.694 else a.k_value end) as raw_k_value,
         |  b.hpcvr,
         |  (case when c.cali_value is null or c.cali_value=0 then 1.0 else c.cali_value end) as cali_value,
         |  1.0 as cvr3_cali,
         |  a.cvr3_cnt
         |FROM
         |  test_new_pb_ocpc as a
         |INNER JOIN
         |  ocpc_hpcvr_total as b
         |ON
         |  a.ideaid=b.ideaid
         |AND
         |  a.adclass=b.adclass
         |LEFT JOIN
         |  $pcvrCaliTable as c
         |ON
         |  a.ideaid=c.ideaid
         |AND
         |  a.adclass=c.adclass
       """.stripMargin

    println(sqlRequest4)

    val regressionK = getRegressionK(end_date, hour, spark)

    val cvr3Ideaid = spark
      .table("test.ocpcv3_ideaid_list_" + hour)
      .select("ideaid", "conversion_goal")
      .distinct()

    val finalData1 = spark.sql(sqlRequest4)
//    finalData1.write.mode("overwrite").saveAsTable("test.ocpc_debug_k_values_bak")


    val finalData2 = resetK(end_date, hour, regressionK, finalData1, spark)


    // 对于刚进入ocpc阶段但是有cpc历史数据的广告依据历史转化率给出k的初值
    val selectCondition2 = getTimeRangeSql3(start_date, hour, end_date, hour)
    val ocpcHistoryData = spark
      .table("dl_cpc.ocpc_unionlog")
      .where(selectCondition2)
      .select("ideaid", "adclass")
      .withColumn("is_ocpc_flag", lit(1))
      .distinct()


    val finalData3New = finalData2
      .join(ocpcHistoryData, Seq("ideaid", "adclass"), "left_outer")
      .withColumn("k_value", when(col("is_ocpc_flag").isNull, col("cvr_cnt") * 1.0 / (col("ctr_cnt") * col("hpcvr"))).otherwise(col("k_value")))
      .join(cvr3Ideaid, Seq("ideaid"), "left_outer")
      .withColumn("k_value", when(col("conversion_goal")===2 && col("is_ocpc_flag").isNull, col("cvr3_cnt") * 1.0 / (col("ctr_cnt") * col("hpcvr"))).otherwise(col("k_value")))
      .filter("k_value!=0 and k_value is not null")


    finalData3New.createOrReplaceTempView("raw_final_data")


//    finalData3New.write.mode("overwrite").saveAsTable("test.new_pb_ocpc_with_pcvr_complete_bak_bak")
    val ocpcTable = "test.ocpcv3_ideaid_list_" + hour
    val sqlRequest5 =
      s"""
         |SELECT
         |  a.ideaid,
         |  a.userid,
         |  a.adclass,
         |  a.cost,
         |  a.ctr_cnt,
         |  a.cvr_cnt,
         |  a.adclass_cost,
         |  a.adclass_ctr_cnt,
         |  a.adclass_cvr_cnt,
         |  (case when b.conversion_goal=1 and a.k_value>3.0 then 3.0
         |        when b.conversion_goal!=1 and a.k_value>2.0 then 2.0
         |        when b.conversion_goal is null and a.k_value>2.0 then 2.0
         |        when a.k_value<0.00001 then 0.00001
         |        else a.k_value end) as k_value,
         |  a.hpcvr,
         |  a.cali_value,
         |  a.cvr3_cali,
         |  a.cvr3_cnt
         |FROM
         |  (SELECT * FROM raw_final_data WHERE k_value is not null or is_ocpc_flag is not null) as a
         |LEFT JOIN
         |  $ocpcTable as b
         |ON
         |  a.ideaid=b.ideaid
       """.stripMargin
    println(sqlRequest5)
    val finalData3 = spark.sql(sqlRequest5)

//    finalData3.write.mode("overwrite").saveAsTable("test.new_pb_ocpc_with_pcvr_complete_bak2_bak")

    val finalData = finalData3.select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "k_value", "hpcvr", "cali_value", "cvr3_cali", "cvr3_cnt")



    finalData.write.mode("overwrite").saveAsTable("dl_cpc.new_pb_ocpc_with_pcvr")


    finalData
      .withColumn("date", lit(end_date))
      .withColumn("hour", lit(hour))
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_pb_result_table_v5")

    // 保存pb文件
    savePbPack(finalData)
  }


  def savePbRedis(tableName: String, spark: SparkSession): Unit = {
    var cnt = spark.sparkContext.longAccumulator
    var changeCnt = spark.sparkContext.longAccumulator
    var succSetCnt = spark.sparkContext.longAccumulator
    var cvrResultAcc = spark.sparkContext.longAccumulator
    var ctrResultAcc = spark.sparkContext.longAccumulator
    println("###############1")
    println(s"accumulator before partition loop")
    println("total loop cnt: " + cnt.value.toString)
    println("redis retrieve cnt: " + changeCnt.value.toString)
    println("redis save cnt: " + succSetCnt.value.toString)
    println("ctrcnt: " + ctrResultAcc.value.toString)
    println("cvrcnt: " + cvrResultAcc.value.toString)
    val dataset = spark.table(tableName)
    val conf = ConfigFactory.load()
    println(conf.getString("redis.host"))
    println(conf.getInt("redis.port"))

    dataset.foreachPartition(iterator => {

      val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))

      iterator.foreach{
        record => {
          val uid = record.get(0).toString
          var key = uid + "_UPDATA"
          cnt.add(1)
          val ctrCnt = record.getLong(1)
          val cvrCnt = record.getLong(2)
          ctrResultAcc.add(ctrCnt)
          cvrResultAcc.add(cvrCnt)

          val buffer = redis.get[Array[Byte]](key).orNull
          var user: UserProfile.Builder = null
          if (buffer != null) {
            user = UserProfile.parseFrom(buffer).toBuilder
            val u = user.build()
            user = user.setCtrcnt(ctrCnt)
            user = user.setCvrcnt(cvrCnt)

            val isSuccess = redis.setex(key, 3600 * 24 * 30, user.build().toByteArray)
            if (isSuccess) {
              succSetCnt.add(1)
            }
            changeCnt.add(1)
          }
        }
      }
      redis.disconnect
    })

    println("####################2")
    println(s"accumulator after partition loop")
    println("total loop cnt: " + cnt.value.toString)
    println("redis retrieve cnt: " + changeCnt.value.toString)
    println("redis save cnt: " + succSetCnt.value.toString)
    println("ctrcnt: " + ctrResultAcc.value.toString)
    println("cvrcnt: " + cvrResultAcc.value.toString)
  }

  def testSavePbRedis(tableName: String, spark: SparkSession): Unit = {
    var cnt = spark.sparkContext.longAccumulator
    var cvrResultAcc = spark.sparkContext.longAccumulator
    var ctrResultAcc = spark.sparkContext.longAccumulator
    println("###############1")
    println(s"accumulator before partition loop")
    println("redis hit number: " + cnt.value.toString)
    println("correct ctr number: " + ctrResultAcc.value.toString)
    println("correct cvr number: " + cvrResultAcc.value.toString)
    val conf = ConfigFactory.load()
    //    redis-cli -h 192.168.80.19 -p 6379
    println(conf.getString("redis.host"))
    println(conf.getInt("redis.port"))

    val dataset = spark.table(tableName)
    dataset.foreachPartition(iterator => {

      val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))

      iterator.foreach{
        record => {
          val uid = record.get(0).toString
          var key = uid + "_UPDATA"
          val ctrCnt = record.getLong(1)
          val cvrCnt = record.getLong(2)

          val buffer = redis.get[Array[Byte]](key).orNull
          var user: UserProfile.Builder = null
          if (buffer != null) {
            cnt.add(1)
            user = UserProfile.parseFrom(buffer).toBuilder
            val currentCtr = user.getCtrcnt
            val currentCvr = user.getCvrcnt
            if (currentCtr == ctrCnt) {
              ctrResultAcc.add(1)
            }
            if (currentCvr == cvrCnt) {
              cvrResultAcc.add(1)
            }
          }
        }
      }
      redis.disconnect
    })


    println("####################2")
    println(s"accumulator after partition loop")
    println("redis hit number: " + cnt.value.toString)
    println("correct ctr number: " + ctrResultAcc.value.toString)
    println("correct cvr number: " + cvrResultAcc.value.toString)
  }

  def savePbPack(dataset: Dataset[Row]): Unit = {
    var list = new ListBuffer[SingleUser]
    val filename = s"UseridDataOcpc.pb"
    println("size of the dataframe")
    println(dataset.count)
    dataset.show(10)
    var cnt = 0

    for (record <- dataset.collect()) {
      val ideaid = record.get(0).toString
      val userId = record.get(1).toString
      val adclassId = record.get(2).toString
      val costValue = record.get(3).toString
      val ctrValue = record.getLong(4).toString
      val cvrValue = record.getLong(5).toString
      val adclassCost = record.get(6).toString
      val adclassCtr = record.getLong(7).toString
      val adclassCvr = record.getLong(8).toString
      val k = record.get(9).toString
      val hpcvr = record.getAs[Double]("hpcvr")
      val caliValue = record.getAs[Double]("cali_value")
      val cvr3Cali = record.getAs[Double]("cvr3_cali")
      val cvr3Cnt = record.getAs[Long]("cvr3_cnt")

      if (cnt % 500 == 0) {
        println(s"ideaid:$ideaid, userId:$userId, adclassId:$adclassId, costValue:$costValue, ctrValue:$ctrValue, cvrValue:$cvrValue, adclassCost:$adclassCost, adclassCtr:$adclassCtr, adclassCvr:$adclassCvr, k:$k, hpcvr:$hpcvr, caliValue:$caliValue, cvr3Cali:$cvr3Cali, cvr3Cnt:$cvr3Cnt")
      }
      cnt += 1
//      if (cvr3Cnt > 0) {
//        println("######################")
//        println(s"ideaid:$ideaid, userId:$userId, adclassId:$adclassId, costValue:$costValue, ctrValue:$ctrValue, cvrValue:$cvrValue, adclassCost:$adclassCost, adclassCtr:$adclassCtr, adclassCvr:$adclassCvr, k:$k, hpcvr:$hpcvr, caliValue:$caliValue, cvr3Cali:$cvr3Cali, cvr3Cnt:$cvr3Cnt")
//      }

      val tmpCost = adclassCost.toLong
      if (tmpCost<0) {
        println("#######################################")
        println("negative cost")
        println(record)
      } else {
        val currentItem = SingleUser(
          ideaid = ideaid,
          userid = userId,
          cost = costValue,
          ctrcnt = ctrValue,
          cvrcnt = cvrValue,
          adclass = adclassId,
          adclassCost = adclassCost,
          adclassCtrcnt = adclassCtr,
          adclassCvrcnt = adclassCvr,
          kvalue = k,
          hpcvr = hpcvr,
          calibration = caliValue,
          cvr3Cali = cvr3Cali,
          cvr3Cnt = cvr3Cnt
        )
        list += currentItem

      }
    }
    val result = list.toArray[SingleUser]
    val useridData = UserOcpc(
      user = result
    )
    println("length of the array")
    println(result.length)
    useridData.writeTo(new FileOutputStream(filename))

//    dataset.write.mode("overwrite").saveAsTable("test.new_pb_ocpc_with_pcvr")
    println("complete save data into protobuffer")

  }

  def readInnocence(spark: SparkSession) ={
    import spark.implicits._

    val filename = "/user/cpc/wangjun/ocpc_ideaid.txt"
    val data = spark.sparkContext.textFile(filename)

    val dataRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    //    dataRDD.foreach(println)

    dataRDD.toDF("ideaid", "flag").createOrReplaceTempView("innocence_list")

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  flag
         |FROM
         |  innocence_list
         |GROUP BY ideaid, flag
       """.stripMargin

    val resultDF = spark.sql(sqlRequest)

//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_innocence_idea_list_bak")
    resultDF
  }


  def calculateHPCVR(endDate: String, hour: String, spark: SparkSession) ={
    // calculate time period for historical data
    val threshold = 20
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(endDate)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val startDate = sdf.format(dt)
    val selectCondition = getTimeRangeSql(startDate, hour, endDate, hour)

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  SUM(total_cvr) * 1.0 / SUM(cnt) as hpcvr
         |FROM
         |  dl_cpc.ocpc_pcvr_history
         |WHERE $selectCondition
         |GROUP BY ideaid, adclass
       """.stripMargin
    println(sqlRequest)

    val rawTable = spark.sql(sqlRequest)

//    rawTable.write.mode("overwrite").saveAsTable("test.ocpc_hpcvr_total_bak")
    rawTable

  }

  def checkAdType(endDate: String, hour: String, spark: SparkSession): DataFrame ={
    // TODO 从广告更新表中更新广告类型
//    // 计算时间区间
//    val threshold = 20
//    val sdf = new SimpleDateFormat("yyyy-MM-dd")
//    val date = sdf.parse(endDate)
//    val calendar = Calendar.getInstance
//    calendar.setTime(date)
//    calendar.add(Calendar.DATE, -7)
//    val dt = calendar.getTime
//    val startDate = sdf.format(dt)
//    val selectCondition = getTimeRangeSql(startDate, hour, endDate, hour)
//
//    // 汇总近七天数据并找到每个ideaid，adclass的最新数据的类型
//    val sqlRequest1 =
//      s"""
//         |SELECT
//         |    ideaid,
//         |    adclass,
//         |    (case when siteid>0 then 1 else 0 end) as type_flag,
//         |    row_number() over(partition by ideaid, adclass ORDER BY timestamp DESC) as seq
//         |FROM
//         |    dl_cpc.ocpc_track_ad_type_hourly
//         |WHERE
//         |    $selectCondition
//       """.stripMargin
//
//    println(sqlRequest1)
//    val rawData = spark.sql(sqlRequest1)
//
//    val typeData = rawData.filter("seq=1").select("ideaid", "adclass", "type_flag")

    val typeData = spark
      .table("test.ocpcv3_ideaid_list_" + hour)
      .withColumn("type_flag", when(col("conversion_goal")===3, 1).otherwise(0)).select("ideaid", "type_flag")


    typeData
  }

  def filterDataByType(rawData: DataFrame, date:String, hour: String, spark:SparkSession): DataFrame ={
    // TODO 使广告行业的ctr与cvr的替换更加平滑
    val typeData = checkAdType(date, hour, spark)

    val joinData = rawData
      .join(typeData, Seq("ideaid"), "left_outer")
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "type_flag")
      .withColumn("new_type_flag", when(col("type_flag").isNull, 0).otherwise(col("type_flag")))

    joinData.createOrReplaceTempView("join_table")

    val sqlRequest1 =
      s"""
         |SELECT
         |    adclass,
         |    new_type_flag,
         |    SUM(cost) as total_cost,
         |    SUM(ctr_cnt) as total_ctr,
         |    SUM(cvr_cnt) as total_cvr
         |FROM
         |    join_table
         |GROUP BY adclass, new_type_flag
       """.stripMargin

    println(sqlRequest1)
    val groupbyData = spark.sql(sqlRequest1)

    val joinData2 = joinData
      .join(groupbyData, Seq("adclass", "new_type_flag"), "left_outer")
      .select("ideaid", "userid", "adclass", "cost", "ctr_cnt", "cvr_cnt", "adclass_cost", "adclass_ctr_cnt", "adclass_cvr_cnt", "new_type_flag", "total_cost", "total_ctr", "total_cvr")
      .withColumn("new_cost", when(col("cvr_cnt")<20 and col("new_type_flag")===1, col("total_cost")).otherwise(col("cost")))
      .withColumn("new_ctr_cnt", when(col("cvr_cnt")<20 and col("new_type_flag")===1, col("total_ctr")).otherwise(col("ctr_cnt")))
      .withColumn("new_cvr_cnt", when(col("cvr_cnt")<20 and col("new_type_flag")===1, col("total_cvr")).otherwise(col("cvr_cnt")))


//    joinData2.write.mode("overwrite").saveAsTable("test.ocpc_final_join_table_bak")
    joinData2


  }

  def getCvr3List(date: String, hour: String, spark: SparkSession) :DataFrame = {
    import spark.implicits._

    val cvr3List = spark
      .table("test.ocpcv3_ideaid_list_" + hour)
      .filter("conversion_goal=2")
      .withColumn("flag", lit(1))
      .select("ideaid", "flag")
      .distinct()

    val historyData = getActData(date, hour, 24 * 7, spark)
    val rawData = historyData
      .groupBy("ideaid", "adclass")
      .agg(sum(col("cvr_cnt")).alias("base_cvr3_cnt"))
      .select("ideaid", "adclass", "base_cvr3_cnt")

    val resultDF = cvr3List
      .join(rawData, Seq("ideaid"), "left_outer")
      .select("ideaid", "adclass", "base_cvr3_cnt")
      .withColumn("cvr3_cnt", when(col("base_cvr3_cnt").isNull, 0).otherwise(col("base_cvr3_cnt")))
      .select("ideaid", "adclass", "cvr3_cnt", "base_cvr3_cnt")

//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_cvr3_cnt_ideaids_bak")

    resultDF

  }

  def getRegressionK(date: String, hour: String, spark: SparkSession) = {
    import spark.implicits._

//    val filename = "/user/cpc/wangjun/ocpc_linearregression_k.txt"
//    val data = spark.sparkContext.textFile(filename)
//
//
//    val rawRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
//    rawRDD.foreach(println)
//
//    val rawDF = rawRDD.toDF("ideaid", "flag").distinct()

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  k_ratio2,
         |  k_ratio3
         |FROM
         |  dl_cpc.ocpc_regression_k_final
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
       """.stripMargin

    val regressionK = spark.sql(sqlRequest)

    val cvr3List = spark
      .table("test.ocpcv3_ideaid_list_" + hour)
      .filter("conversion_goal=2")
      .withColumn("cvr3_flag", lit(1))
      .select("ideaid", "cvr3_flag")
      .distinct()

//    val prevTable = spark.table("dl_cpc.new_pb_ocpc_with_pcvr")

    val finalDF = regressionK
      .join(cvr3List, Seq("ideaid"), "left_outer")
      .select("ideaid", "k_ratio2", "k_ratio3", "cvr3_flag")
      .withColumn("regression_k_value", when(col("cvr3_flag").isNull, col("k_ratio2")).otherwise(col("k_ratio3")))
      .select("ideaid", "regression_k_value")
//      .join(prevTable, Seq("ideaid"), "left_outer")
//      .withColumn("regression_k_value", when(col("k_value").isNotNull && col("original_regression_k_value").isNotNull && col("original_regression_k_value")>col("k_value"), col("k_value") + (col("original_regression_k_value") - col("k_value")) * 1.0/5.0).otherwise(col("original_regression_k_value")))
//      .select("ideaid", "adclass", "regression_k_value", "original_regression_k_value", "k_value")
//      .filter("adclass is not null")



//    finalDF.write.mode("overwrite").saveAsTable("test.ocpc_test_k_regression_list_bak")

//    val resultDF  = finalDF.select("ideaid", "adclass", "regression_k_value")

    finalDF


  }

  def resetK(date: String, hour: String, regressionK: DataFrame, pidK: DataFrame, spark: SparkSession) = {
    // todo
    var prevTable = getPrevK(date, hour, 25, spark)
    var hourCnt=25
    while (hourCnt < 36) {
      val cnt = prevTable.count()
      println(s"check prevTable Count: $cnt, at hourCnt = $hourCnt")
      if (cnt>0) {
        hourCnt = 36
      } else {
        hourCnt += 1
        prevTable = getPrevK(date, hour, hourCnt, spark)
      }

    }


    val finalData = pidK
      .join(regressionK, Seq("ideaid"), "left_outer")
      .withColumn("new_k", when(col("regression_k_value")>0, col("regression_k_value")).otherwise(col("raw_k_value")))
      .withColumn("cali_value", lit(1.0))
      .withColumn("cvr3_cali", lit(1.0))
      .join(prevTable, Seq("ideaid", "adclass"), "left_outer")
      .withColumn("k_value_middle", when(col("new_k").isNotNull && col("prev_k").isNotNull && col("new_k")>col("prev_k"), col("prev_k") + (col("new_k") - col("prev_k")) * 1.0 / 4.0).otherwise(col("new_k")))
      .withColumn("k_value", when(col("flag")===0, col("prev_k")).otherwise(col("k_value_middle")))


    finalData
  }

  def getPrevK(date: String, hour: String, hourCnt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)

    val prevK = spark
      .table("dl_cpc.ocpc_pb_result_table_v5")
      .where(s"`date`='$date1' and `hour`='$hour1'")
      .withColumn("prev_k", col("k_value"))
      .select("ideaid", "adclass", "prev_k", "date", "hour")

    val prevCtr = spark
      .table("dl_cpc.ocpc_uid_userid_track_label2")
      .where(s"`date`='$date1' and `hour`='$hour1'")
      .groupBy("ideaid", "adclass")
      .agg(sum(col("ctr_cnt")).alias("ctrcnt"))
      .select("ideaid", "adclass", "ctrcnt")

    val prevTable = prevK
      .join(prevCtr, Seq("ideaid", "adclass"), "left_outer")
      .select("ideaid", "adclass", "prev_k", "ctrcnt")
      .withColumn("flag", when(col("ctrcnt").isNotNull && col("ctrcnt")>0, 1).otherwise(0))
      .withColumn("date", lit(date1))
      .withColumn("hour", lit(hour1))

    prevTable
  }




}