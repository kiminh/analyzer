package com.cpc.spark.ml.recall

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.tools.CalcMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object unit_auc {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("unit auc calculation")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val cal1 = Calendar.getInstance()
    cal1.add(Calendar.DATE, -1)
    val tardate = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    val testset_result = spark.read.parquet("hdfs://emr-cluster/user/cpc/result/cvrtrain").withColumn("score", $"prediction" * 1000000).
      select(expr("cast (unitid as string)").alias("unitid"), expr("cast (label as int)").alias("label"), expr("cast (round(score, 0) as int)").alias("score"))
    val dauData = spark.sql(
      s"""
         |select unitid,max(industry) as industry from dl_cpc.slim_union_log where dt='$tardate' group by unitid
      """.stripMargin).repartition(5000).cache()

    val result = testset_result.join(dauData, Seq("unitid")).select($"unitid", $"industry", $"label", $"score")
    //计算单元的auc
    val DetailAucListBuffer = scala.collection.mutable.ListBuffer[DetailAuc]()
    val id_tag = "unitid"
    //分广告主
    val unitIdAucList = CalcMetrics.getGauc(spark,result,"unitid").rdd.map(x => {
        val unitid = x.getAs[String]("name")
        val auc = x.getAs[Double]("auc")
        val sum = x.getAs[Double]("sum")
        (unitid,auc,sum)
      }).collect()
    for(unitIdAuc <- unitIdAucList) {
      DetailAucListBuffer += DetailAuc(
        id = unitIdAuc._1,
        auc = unitIdAuc._2,
        count = unitIdAuc._3,
        day = tardate,
        tag = id_tag)
    }
    val unitidAuc = DetailAucListBuffer.toList.toDF()

    //计算单元的auc
    val DetailAucListBuffer1 = scala.collection.mutable.ListBuffer[DetailAuc1]()
    //分广告主
    val unitIdAucList1 = CalcMetrics.getGauc(spark,result,"industry").rdd.map(x => {
      val unitid = x.getAs[String]("name")
      val auc = x.getAs[Double]("auc")
      val sum = x.getAs[Double]("sum")
      (unitid,auc,sum)
    }).collect()
    for(unitIdAuc <- unitIdAucList1) {
      DetailAucListBuffer1 += DetailAuc1(
        id = unitIdAuc._1,
        auc = unitIdAuc._2,
        count = unitIdAuc._3,
        day = tardate,
        tag = id_tag)
    }
    val adclassAuc = DetailAucListBuffer1.toList.toDF().select($"id", $"auc".alias("adclassAuc"), $"count".alias("adclassCount"))

    val detailAuc = unitidAuc.join(dauData, $"id"===$"unitid").
      select($"unitid", $"industry", $"auc", $"count").join(adclassAuc, $"industry"===$"id").select($"unitid", $"industry", $"auc", $"count", $"adclassAuc", $"adclassCount")

    detailAuc.repartition(100).createOrReplaceTempView("unitid_table")
    spark.sql(
      s"""
        |insert overwrite table dl_cpc.cpc_id_bscvr_auc partition (day='$tardate', tag='$id_tag')
        |select *,case when (auc*1.0/adclassAuc>0.85 or auc>0.75) and auc>0.65 then 1 else 0 end from unitid_table
      """.stripMargin)
  }
  case class DetailAuc(var id:String = "",
                       var auc:Double = 0,
                       var count:Double = 0,
                       var day:String = "",
                       var tag:String = "")

  case class DetailAuc1(var id:String = "",
                       var auc:Double = 0,
                       var count:Double = 0,
                       var day:String = "",
                       var tag:String = "")
}
