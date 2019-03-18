package com.cpc.spark.OcpcProtoType.report_qtt

import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object OcpcAaAbAutomation {

  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val spark = SparkSession.builder().appName("OcpcAaAbAutomation").enableHiveSupport().getOrCreate()
    val abTestDataDF = getAbTestData(date, spark)
    compareAbTestData(abTestDataDF)
  }

  // 抽取Ab实验数据
  def getAbTestData(date: String, spark: SparkSession): DataFrame ={
    val sql =
      s"""
        |select
        |	dt,
        |	unitid,
        |	userid,
        |	ab_group,
        |	acp,
        |	acb,
        |	acb_max,
        |	cpm,
        |	cpagiven,
        |	cpareal,
        |	pre_cvr,
        |	post_cvr,
        |	kvalue,
        |	cost,
        |	show,
        |	click,
        |	cv
        |from
        |	dl_cpc.ocpc_ab_test_data
        |where
        |	`date` = '$date'
        |and
        |	dt = '$date'
        |and
        |	tag = 'yesterday'
        |order by
        |	unitid, userid, ab_group
      """.stripMargin
    val abTestDataDF = spark.sql(sql)
    abTestDataDF
  }

  // 得到ab实验数据比较结果
  def compareAbTestData(abTestDataDF: DataFrame): Unit ={
    val abTestData = abTestDataDF.collectAsList()
    var dataList: ListBuffer[ListBuffer[String]] = new ListBuffer
    var i = 0
    while(i <= abTestData.size()){
      val preList = abTestData.get(i)
      val nextList = abTestData.get(i+1)
      // 首先添加cpc的实验组数据
      var preBuffer: ListBuffer[String] = new ListBuffer
      for(j <- 0 until preList.size) preBuffer.append(preList.get(j).toString)
      dataList.append(preBuffer)
      // 判断前后两条数据是不是相同的unitid和userid
      val isCpc = "cpc".equals(preList.getAs[String](3))
      val isOcpc = "ocpc".equals(nextList.getAs[String](3))
      val isSame = preList.getAs[String](1).equals(nextList.getAs[String](1)) && preList.getAs[String](2).equals(nextList.getAs[String](2))
      if(isCpc && isOcpc && isSame){
        // 然后添加ocpc的实验组数据
        var nextBuffer: ListBuffer[String] = new ListBuffer
        for(j <- 0 until nextList.size) nextBuffer.append(nextList.get(j).toString)
        dataList.append(nextBuffer)
        // 最后添加cpc和ocpc的对比结果
        val compareBuffer: ListBuffer[String] = new ListBuffer[String]
        for(j <- 0 to 3) compareBuffer.append("")
        for(j <- 4 to 12){
          val compareResult = (nextList.getDouble(j) - preList.getDouble(j)) / preList.getDouble(j)
          compareBuffer.append(compareResult.toString)
        }
        for(j <- 13 to 16){
          val compareResult = (nextList.getDouble(j) * 3 / 7 - preList.getDouble(j)) / preList.getDouble(j)
          compareBuffer.append(compareResult.toString)
        }
        dataList.append(compareBuffer)
        i += 1
      }
      i += 1
    }
    for(list <- dataList) println(list)
  }

}
