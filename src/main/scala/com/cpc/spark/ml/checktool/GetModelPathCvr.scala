package com.cpc.spark.ml.checktool

import java.io._
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.sys.process._

/**
  * author: wangyao
  * date: 12/14/19
  */
object GetModelPathCvr{
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
                 |select a.searchid,a.$raw as raw,'' as model_id,a.$condition as model,day
                 |from dl_cpc.cpc_basedata_union_events a
                 |join
                 |  dl_cpc.cpc_snapshot_v2 c
                 |  on a.searchid = c.searchid and a.ideaid=c.ideaid
                 |  and c.dt = '$dt' and c.hour = '$hour' and c.model_name = '$modelName'
                 |  where a.day ='$dt' and a.hour='$hour'
                 |  and a.$condition = '$modelName'
                 |  and a.adsrc in (1,28) and a.$action = 1
       """.stripMargin
    println(s"sql:\n$sql")
    val basedata = spark.sql(sql)
    basedata.show(10)

    basedata.repartition(5).write.mode("overwrite").insertInto("dl_cpc.dnn_model_score_online")

    val model_path = "1580678920-113469"
    println(model_path)

    val file = s"model_path_${modelName}_${dt}.txt"
    val writer = new PrintWriter(new File(file))
    writer.write(s"$model_path")
    writer.close()

   s"hdfs dfs -put -f $file hdfs://emr-cluster/user/cpc/wy/dnn_lastmodel_path/$file" !


  }

}
