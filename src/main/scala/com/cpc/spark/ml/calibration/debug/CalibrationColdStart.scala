package com.cpc.spark.ml.calibration.debug

import java.io.FileInputStream

import com.cpc.spark.common.Utils
import com.cpc.spark.common.Utils.sendMail
import com.google.protobuf.CodedInputStream
import mlmodel.mlmodel.{IRModel, PostCalibrations}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, udf, _}
import com.cpc.spark.common.Murmur3Hash.stringHash64
//import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQtt.computeCalibration

/**
  * author: wangyao
  * date: 5/14/19
  */
object CalibrationColdStart{
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val dt = args(0)
//    val hour = args(2)
//    val modelName = args(3)


    println(s"dt=$dt")
//    println(s"hour=$hour")
//    println(s"modelName=$modelName")


    val session = Utils.buildSparkSession("calibration_check")
    import session.implicits._

    val dnn_data = spark.read.parquet("hdfs://emr-cluster/user/cpc/wy/dnn_prediction/adcvr-v1wzjf/result-*")
      .toDF("id","prediction","num")
    dnn_data.printSchema()
    dnn_data.show(10)

    println("sum is %d".format(dnn_data.count()))
    // get union log


    val sql = s"""
                 |select a.searchid, substring(a.adclass,1,6) as adclass,
                 |a.ideaid,coalesce(c.iscvr,0) as iscvr
                 |from dl_cpc.cpc_basedata_union_events a
                 |join dl_cpc.dw_unitid_conversion_target b
                 |    on a.unitid = b.unitid
                 |    and b.day = '$dt'
                 |    and b.conversion_target[0] not in ('none','site_uncertain')
                 |left join
                 |  (
                 |    select distinct aa.searchid,aa.ideaid,1 as iscvr,
                 |       bb.conversion_target[0] as real_target
                 |    from dl_cpc.cpc_conversion aa
                 |    join dl_cpc.dw_unitid_conversion_target bb
                 |      on aa.unitid=bb.unitid
                 |      and bb.day = '$dt'
                 |      and bb.conversion_target[0] not in ('none','site_uncertain')
                 |      and array_contains(aa.conversion_target,bb.conversion_target[0])
                 |      where aa.day = '$dt'
                 |      ) c
                 |  on a.searchid = c.searchid and a.ideaid=c.ideaid
                 |  where a.day ='$dt'
                 |  and a.media_appsid in ('80000001','80000002') and a.isclick = 1
                 |  and a.adsrc = 1
                 |  AND (charge_type IS NULL OR charge_type = 1)
       """.stripMargin
    println(s"sql:\n$sql")
    val basedata = session.sql(sql)
      .withColumn("id",hash64(0)(col("searchid")))
      .join(dnn_data,Seq("id"),"inner")

    basedata.printSchema()
    basedata.show(10)
    println("sum is %d".format(basedata.count()))

  }

  def hash64(seed:Int)= udf {
    x:String =>  stringHash64(x,seed)}


  def binarySearch(num: Double, boundaries: Seq[Double]): Int = {
    var index = 0
    var min = 0
    var max = boundaries.size - 1
    while (min <= max) {
      index = (min + max)/ 2
      if (num > boundaries(index)){
        min = index + 1
      }
      else if (num < boundaries(index)) {
        max = index - 1
      }
      else {
        return index
      }
    }
    return min
  }

  def computeCalibration(prob: Double, irModel: IRModel): Double = {
    if (prob <= 0) {
      return 0.0
    }
    var index = binarySearch(prob, irModel.boundaries)
    if (index == 0) {
      return  Math.min(1.0, irModel.predictions(0) * prob/ irModel.boundaries(0))
    }
    if (index == irModel.boundaries.size) {
      index = index - 1
    }
    return Math.max(0.0, Math.min(1.0, irModel.predictions(index-1) +
      (irModel.predictions(index) - irModel.predictions(index-1))
        * (prob - irModel.boundaries(index-1))
        / (irModel.boundaries(index) - irModel.boundaries(index-1))))
  }
}
