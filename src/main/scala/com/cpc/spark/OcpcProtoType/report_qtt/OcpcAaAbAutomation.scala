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
    for(row <- abTestData){
      val temp = new ListBuffer
      for(item <- row){
        temp.append(item)
      }
    }
    var dataList: ListBuffer[Row] = new ListBuffer
    var i = 0
    while(i <= abTestData.size()){
      val preList = abTestData.get(i)
      val nextList = abTestData.get(i+1)
      // 判断前后两条数据是不是相同的unitid和userid
      val isCpc = "cpc".equals(preList(3))
      val isOcpc = "ocpc".equals(nextList(3))
      val isSame = preList(1).equals(nextList(1)) && preList(2).equals(nextList(2))
      if(isCpc && isOcpc && isSame){
        dataList.append(preList)
        dataList.append(nextList)
        var result = Row
        result.merge()
      }
    }
  }

}
