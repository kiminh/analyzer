package com.cpc.spark.ml.checktool

import java.io._
import sys.process._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Properties

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
                 |select a.searchid,a.$raw as raw,model_id,a.$condition as model,day
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

    val select_model_id = basedata.groupBy("model_id").count().orderBy(desc("count"))
      .first().getAs[String]("model_id")
    basedata.filter(s"model_id = 'select_model_id'")
      .write.mode("overwrite").saveAsTable("dl_cpc.dnn_model_score_online")

    val jdbcProp = new Properties()
    val jdbcUrl = "jdbc:mysql://rm-2ze0566kl6tl9zp5w.mysql.rds.aliyuncs.com"
    jdbcProp.put("user", "model_info_r")
    jdbcProp.put("password", "PHPymz8ERZeujN6L")
    jdbcProp.put("driver", "com.mysql.jdbc.Driver")

    val table=s"(select job_name from dl_scheduler.model_info where pack_id = '$select_model_id') as tmp"
    val model_path = spark.read.jdbc(jdbcUrl, table, jdbcProp).first().getAs[String]("job_name")
    println(model_path)

    val file = s"model_path_${modelName}_${dt}.txt"
    val writer = new PrintWriter(new File(file))
    writer.write(s"$model_path")
    writer.close()

   s"hdfs dfs -put -f $file hdfs://emr-cluster/user/cpc/wy/dnn_lastmodel_path/$file" !


  }

}
