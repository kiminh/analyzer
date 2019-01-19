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
    val cal1 = Calendar.getInstance()
    cal1.add(Calendar.DATE, -1)
    val tardate = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    val testset_result = spark.read.parquet("hdfs://emr-cluster/user/cpc/result/cvrTrain")
    val DetailAucListBuffer = scala.collection.mutable.ListBuffer[DetailAuc]()

    //分广告主
    val unitIdAucList = CalcMetrics.getGauc(spark,testset_result,"unitid").rdd
      .map(x => {
        val unitid = x.getAs[String]("unitid")
        val auc = x.getAs[Double]("auc")
        val sum = x.getAs[Double]("sum")
        (unitid,auc,sum)
      })
      .collect()
    for(unitIdAuc <- unitIdAucList) {
      DetailAucListBuffer += DetailAuc(
        name = unitIdAuc._1,
        auc = unitIdAuc._2,
        sum = unitIdAuc._3,
        day = tardate,
        tag = "unitid")
    }
    val time5=System.currentTimeMillis()
  }
  case class DetailAuc(var name:String = "",
                       var auc:Double = 0,
                       var sum:Double = 0,
                       var day:String = "",
                       var tag:String = "")
}
