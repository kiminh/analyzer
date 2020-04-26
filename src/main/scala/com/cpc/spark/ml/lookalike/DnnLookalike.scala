package com.cpc.spark.ml.lookalike

import com.cpc.spark.common.Murmur3Hash.stringHash64
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * author: duanguangdong
  * date: 26/04/20
  */
object DnnLookalike{
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val dt = args(0)
    val task = args(1)

    println(s"dt=$dt")
    println(s"dt=$task")

    val dnn_data = spark.read.parquet(s"hdfs://emr-cluster/user/cpc/wy/dnn_model_score_offline/$task/$dt/result-*")
      .toDF("id","prediction","num")

    dnn_data.show(10)

    println("sum is %d".format(dnn_data.count()))
    // get union log

    dnn_data.createOrReplaceTempView("dnn_lookalike")

    val sql = s"""
                 |select distinct tb.uid from
                 |(select searchid_hash, tuid from dl_cpc.cpc_sample_v2 where dt='${dt}' and hour='00' and pt='daily' and task='$task') ta
                 |join
                 |(select tuid,md5(did) as uid from qttdw.dwd_adl_tuid_did_mapping_di where dt='${dt}' group by tuid,did) tb
                 | on ta.tuid=tb.tuid
                 |join
                 |(select id from dnn_lookalike where prediction>0) tc
                 |on ta.searchid_hash=tc.id
       """.stripMargin
    println(s"sql:\n$sql")
    spark.sql(sql).rdd.map{
      r =>
        r.getAs[String]("uid")
    }.repartition(1).saveAsTextFile(s"hdfs://emr-cluster/user/cpc/wy/dnn_model_score_offline/$task/$dt/total_result.txt")
  }
}