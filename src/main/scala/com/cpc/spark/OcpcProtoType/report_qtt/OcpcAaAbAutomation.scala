package com.cpc.spark.OcpcProtoType.report_qtt

import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object OcpcAaAbAutomation {

  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val spark = SparkSession.builder().appName("OcpcAaAbAutomation").enableHiveSupport().getOrCreate()
    val resultDF = getAbTestData(date, spark)
    compareAbTestData(date, resultDF)
  }

  // 抽取Ab实验数据
  def getAbTestData(date: String, spark: SparkSession): DataFrame ={
    // 首先抽取需要的字段
    val sql1 =
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
    val abTestDataDF = spark.sql(sql1)
    abTestDataDF.createOrReplaceTempView("ab_test_data")

    // 然后根据这些字段计算比较结果
    val sql2 =
      s"""
         select
         | c.dt,
         | c.unitid,
         | c.userid,
         | '' as gap,
         | c.acp_increase,
         | c.acb_increase,
         | c.acb_max_increase,
         | c.cpm_increase,
         | c.cpagiven_increase,
         | c.cpareal_increase,
         | c.pre_cvr_increase,
         | c.post_cvr_increase,
         | c.kvalue_increase,
         | c.cost_increase,
         | c.show_increase,
         | c.click_increase,
         | c.cv_increase
         |from
         |	(select
         |		a.dt,
         |		a.unitid,
         |		a.userid,
         |		a.ab_group as cpc_ab_group,
         |		a.acp as cpc_acp,
         |		a.acb as cpc_acb,
         |		a.acb_max as cpc_acb_max,
         |		a.cpm as cpc_cpm,
         |		a.cpagiven as cpc_cpagiven,
         |		a.cpareal as cpc_cpareal,
         |		a.pre_cvr as cpc_pre_cvr,
         |		a.post_cvr as cpc_post_cvr,
         |		a.kvalue as cpc_kvalue,
         |		a.cost as cpc_cost,
         |		a.show as cpc_show,
         |		a.click as cpc_click,
         |		a.cv as cpc_cv,
         |		b.ab_group as ocpc_ab_group,
         |		b.acp as ocpc_acp,
         |		b.acb as ocpc_acb,
         |		b.acb_max as ocpc_acb_max,
         |		b.cpm as ocpc_cpm,
         |		b.cpagiven as ocpc_cpagiven,
         |		b.cpareal as ocpc_cpareal,
         |		b.pre_cvr as ocpc_pre_cvr,
         |		b.post_cvr as ocpc_post_cvr,
         |		b.kvalue as ocpc_kvalue,
         |		b.cost as ocpc_cost,
         |		b.show as ocpc_show,
         |		b.click as ocpc_click,
         |		b.cv as ocpc_cv,
         |		round((b.acp - a.acp) / a.acp, 4) as acp_increase,
         |		round((b.acb - a.acb) / a.acb, 4) as acb_increase,
         |		round((b.acb_max - a.acb_max) / a.acb_max, 4) as acb_max_increase,
         |		round((b.cpm - a.cpm) / a.cpm, 4) as cpm_increase,
         |		round((b.cpagiven - a.cpagiven) / a.cpagiven, 4) as cpagiven_increase,
         |		round((b.cpareal - a.cpareal) / a.cpareal, 4) as cpareal_increase,
         |		round((b.pre_cvr - a.pre_cvr) / a.pre_cvr, 4) as pre_cvr_increase,
         |		round((b.post_cvr - a.post_cvr) / a.post_cvr, 4) as post_cvr_increase,
         |		round((b.kvalue - a.kvalue) / a.kvalue, 4) as kvalue_increase,
         |		round((b.cost * 3 / 7 - a.cost) / a.cost, 4) as cost_increase,
         |		round((b.show * 3 / 7 - a.show) / a.show, 4) as show_increase,
         |		round((b.click * 3 / 7 - a.click) / a.click, 4) as click_increase,
         |		round((b.cv * 3 / 7 - a.cv) / a.cv, 4) as cv_increase
         |	from
         |		ab_test_data a
         |	join
         |		ab_test_data b
         |	where
         |		a.unitid = b.unitid) c
         |where
         |	c.cpc_ab_group = 'cpc'
         |and
         |	c.ocpc_ab_group = 'ocpc'
         |order by
         |	c.unitid, c.userid
      """.stripMargin
    val compareIndexDF = spark.sql(sql2)
    val resultDF = abTestDataDF.union(compareIndexDF)
    resultDF
  }

  // 将ab实验数据比较结果写到本地文件夹
  def compareAbTestData(date: String, resultDF: DataFrame): Unit ={
    val list = resultDF.collectAsList()
    for(i <- 0 until list.size()) println(list.get(i))
    val csvPath = "/home/cpc/wt/test_data/ab/" + date + ".csv"
    val saveOptions = Map("header" -> "true", "path" -> csvPath)
    resultDF.coalesce(1)
      .write.mode("overwrite").format("csv")
      .options(saveOptions)
      .save()
  }

//  // 得到ab实验数据比较结果
//  def compareAbTestData(abTestDataDF: DataFrame): Unit ={
//    val abTestData = abTestDataDF.collectAsList()
//    var dataList: ListBuffer[ListBuffer[String]] = new ListBuffer
//    var i = 0
//    while(i <= abTestData.size()){
//      val preList = abTestData.get(i)
//      val nextList = abTestData.get(i+1)
//      // 首先添加cpc的实验组数据
//      var preBuffer: ListBuffer[String] = new ListBuffer
//      for(j <- 0 until preList.size) preBuffer.append(preList.get(j).toString)
//      dataList.append(preBuffer)
//      // 判断前后两条数据是不是相同的unitid和userid
//      val isCpc = "cpc".equals(preList(3))
//      val isOcpc = "ocpc".equals(nextList(3))
//      println(preList)
//      println(nextList)
//      println(preList(1) + "  " + preList(2) + "  " + preList(3))
//      val isSame = (preList(1) == nextList(1)) && (preList(2) == nextList(2))
//      if(isCpc && isOcpc && isSame){
//        // 然后添加ocpc的实验组数据
//        var nextBuffer: ListBuffer[String] = new ListBuffer
//        for(j <- 0 until nextList.size) nextBuffer.append(nextList.get(j).toString)
//        dataList.append(nextBuffer)
//        // 最后添加cpc和ocpc的对比结果
//        val compareBuffer: ListBuffer[String] = new ListBuffer[String]
//        for(j <- 0 to 3) compareBuffer.append("")
//        for(j <- 4 to 12){
//          val compareResult = (nextList.getDouble(j) - preList.getDouble(j)) / preList.getDouble(j)
//          compareBuffer.append(compareResult.toString)
//        }
//        for(j <- 13 to 16){
//          val compareResult = (nextList.getDouble(j) * 3 / 7 - preList.getDouble(j)) / preList.getDouble(j)
//          compareBuffer.append(compareResult.toString)
//        }
//        dataList.append(compareBuffer)
//        i += 1
//      }
//      i += 1
//    }
//    for(list <- dataList) println(list)
//  }

}
