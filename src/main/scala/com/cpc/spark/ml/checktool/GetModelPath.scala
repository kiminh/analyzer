package com.cpc.spark.ml.checktool

import com.cpc.spark.common.Murmur3Hash.stringHash64
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
//import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQtt.computeCalibration

/**
  * author: wangyao
  * date: 12/14/19
  */
object GetModelPath{
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val dt = args(0)
    val hour = args(1)
    val modelType = args(2)
    val modelName = args(3)

    println(s"dt=$dt hour=$hour")
    println(s"modelName=$modelName")

    // get union log
    var condition = "ctr_model_name"
    var raw = "raw_ctr"
    var action = "isshow"
    if(modelType=="cvr"){
      condition = "cvr_model_name"
      raw = "raw_cvr"
      action = "isclick"
    }

    val sql = s"""
                 |select a.searchid,a.$raw,model_id
                 |from dl_cpc.cpc_basedata_union_events a
                 |join
                 |  dl_cpc.cpc_snapshot_v2 c
                 |  on a.searchid = c.searchid and a.ideaid=c.ideaid
                 |  and c.dt = '$dt' and c.hour = "$hour" and c.model_name = '$modelName'
                 |  where a.day ='$dt' and a.hour='$hour'
                 |  and a.$condition = '$modelName'
                 |  and a.adsrc in (1,28) and a.$action = 1
       """.stripMargin
    println(s"sql:\n$sql")
    val basedata = spark.sql(sql)
    basedata.show(10)
      basedata.createOrReplaceTempView("tmp")

    val sql2 =
      s"""
         |select model_id,count(*) as count
         |from tmp
         |group by model_id
         |order by count desc
         |""".stripMargin

    val select_model_id = spark.sql(sql2).first().getAs[String]("model_id")


  }

  def hash64(seed:Int)= udf {
    x:String =>  stringHash64(x,seed)}

}
