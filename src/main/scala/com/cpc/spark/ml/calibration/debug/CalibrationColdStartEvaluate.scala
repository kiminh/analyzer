package com.cpc.spark.ml.calibration.debug

import java.io.FileInputStream

import com.cpc.spark.common.Murmur3Hash.stringHash64
import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQttCvrV3.LogToPb
import com.cpc.spark.ml.calibration.debug.CalibrationCheckOnMiduCvr.{computeCalibration, searchMap}
import com.cpc.spark.tools.CalcMetrics
import com.google.protobuf.CodedInputStream
import mlmodel.mlmodel.PostCalibrations
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
//import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQtt.computeCalibration

/**
  * author: wangyao
  * date: 5/14/19
  */
object CalibrationColdStartEvaluate{
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    val dt = args(0)
    val modelName = args(1)
    val task = args(2)
    val modelPath = args(3)

    println(s"modelPath=$modelPath")
    println(s"dt=$dt")
    println(s"modelName=$modelName")

    val calimap = new PostCalibrations().mergeFrom(CodedInputStream.newInstance(new FileInputStream(modelPath))).caliMap
    val modelset=calimap.keySet

    val dnn_data = spark.sql(s"select * from dl_cpc.cpc_pscore where dt='$dt' and " +
      s"hour='00' and pt='daily' and task='$task'")
      .withColumn("id",col("searchid_hash"))
      .withColumn("prediction",col("pscore"))
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
    val basedata = spark.sql(sql)
      .withColumn("id",hash64(0)(col("searchid")))
      .join(dnn_data,Seq("id"),"inner")
      .withColumn("isclick",col("iscvr"))
      .withColumn("ectr",col("prediction")*1e6d)

    basedata.show(10)
    println("sum is %d".format(basedata.count()))

    val log = basedata
      .withColumn("group3",concat_ws("_",col("adclass"),col("ideaid")))
      .withColumn("group4",concat_ws("_",col("adclass")))
      .withColumn("group",when(searchMap(modelset)(col("group2")),col("group2")).otherwise(col("0")))
      .withColumn("group",when(searchMap(modelset)(col("group1")),col("group1")).otherwise(col("group")))
      .withColumn("len",length(col("group")))
      .select("ideaid","ectr","searchid","group","group1","group2","isclick")
    log.printSchema()

    log.show(50)
    println("total data:%d".format(log.count()))
    log.repartition(5).write.mode("overwrite").saveAsTable("test.wy01")

    val data = log.filter("length(group)>0")
    println("calibration data:%d".format(data.count()))
    val result = data.rdd.map( x => {
      val ectr = x.getInt(1).toDouble
      val group = x.getString(3)
      val irModel = calimap.get(group).get
      val calibrated = computeCalibration(ectr, irModel.ir.get)
      (1.0, ectr, calibrated, 1.0)
    }).reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
    val ectr = result._2 / result._4
    val calibrated_ctr = result._3 / result._4
    println(s"impression: ${result._4}")
    println(s"ectr: $ectr")
    println(s"calibrated_ctr: $calibrated_ctr")

    //    raw data
    val result2 = data.rdd.map( x => {
      val ideaid = x.getLong(0)
      val ectr = x.getInt(1).toDouble
      val group = x.getString(3)
      val label = x.getInt(6)
      val irModel = calimap.get(group).get
      val calibrated = computeCalibration(ectr, irModel.ir.get)
      (ideaid, ectr, calibrated,label)
    }).toDF("ideaid","ectr","calibrated","label")
    val modelData = result2.selectExpr("cast(label as Int) label","cast(ectr as Int) prediction","ideaid")
    calculateAuc(modelData,"original",spark)

    //    cold calibration
    val calibData = result2.selectExpr("cast(label as Int) label","cast(calibrated as Int) prediction","ideaid")
    calculateAuc(calibData,"online",spark)
  }

  def calculateAuc(data:DataFrame,cate:String,spark: SparkSession): Unit = {
    val testData = data.selectExpr("cast(label as Int) label", "cast(prediction as Int) score")
    val auc = CalcMetrics.getAuc(spark, testData)
    println("%s auc:%f".format(cate, auc))
    val p1 = data.groupBy().agg(avg(col("label")).alias("ctr"), avg(col("prediction")).alias("ectr"))
    val ctr = p1.first().getAs[Double]("ctr")
    val ectr = p1.first().getAs[Double]("ectr")
    println("%s calibration: ctr:%f,ectr:%f,ectr/ctr:%f".format(cate, ctr, ectr, ectr / ctr))
  }

  def hash64(seed:Int)= udf {
    x:String =>  stringHash64(x,seed)}

}
