package com.cpc.spark.ml.recall

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.tools.CalcMetrics
import org.apache.spark.sql.SparkSession

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
    val testset_result = spark.read.parquet("hdfs://emr-cluster/user/cpc/result/cvrtrain")
    val DetailAucListBuffer = scala.collection.mutable.ListBuffer[DetailAuc]()
    val id_tag = "unitid"

    //分广告主
    val unitIdAucList = CalcMetrics.getGauc(spark,testset_result,"unitid").rdd
      .map(x => {
        val unitid = x.getAs[String]("unitid")
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

    val detailAuc = DetailAucListBuffer.toList.toDF()

    detailAuc.createOrReplaceTempView("unitid_table")
    spark.sql(
      s"""
        |insert overwrite table dl_cpc.cpc_id_bscvr_auc partition (day='$tardate', tag='$id_tag')
        |select id, auc, count from unitid_table
      """.stripMargin)
  }
  case class DetailAuc(var id:String = "",
                       var auc:Double = 0,
                       var count:Double = 0,
                       var day:String = "",
                       var tag:String = "")
}
