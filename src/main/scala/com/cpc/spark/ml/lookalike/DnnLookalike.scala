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
    val start = args(0)
    val end = args(1)
    val ml_ver = args(2)
    val tag_name = args(3)
    val task = "adcvr-" + ml_ver

    println(s"start=$start")
    println(s"task=$task")

    val dnn_data = spark.read.parquet(s"hdfs://algo1/cpcalgo/user/cpc/wy/dnn_model_score_offline/$task/$end/result-*")
      .toDF("id","prediction","num")

    dnn_data.show(10)

    println("sum is %d".format(dnn_data.count()))
    // get union log

    dnn_data.createOrReplaceTempView("dnn_lookalike")

    val sql = s"""
                 |insert overwrite table algo_cpc.cpc_dnn_lookalike partition(day='${start}', model='$ml_ver')
                 |select distinct tb.uid from
                 |(select searchid_hash, tuid from algo_cpc.cpc_sample_v2 where dt='${start}' and hour='00' and pt='daily' and task='$tag_name') ta
                 |join
                 |(select tuid,did as uid from qttdw.dwd_adl_tuid_did_mapping_di where dt='${start}' group by tuid,did) tb
                 | on ta.tuid=tb.tuid
                 |join
                 |(select id from dnn_lookalike where prediction>0 group by id) tc
                 |on ta.searchid_hash=tc.id
       """.stripMargin
    println(s"sql:\n$sql")
    spark.sql(sql)

//    if (start == end){
//      spark.sql(
//        s"""
//           |select uid from algo_cpc.cpc_dnn_lookalike where model='$ml_ver' group by uid
//           |""".stripMargin).rdd.map{
//        r =>
//          r.getAs[String]("uid")
//      }.repartition(1).saveAsTextFile(s"hdfs://algo1/cpcalgo/user/cpc/wy/dnn_model_score_offline/$task/$end/total_result")
//    }

  }
}