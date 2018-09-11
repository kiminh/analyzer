package com.cpc.spark.log.ftrlsample

import org.apache.spark.sql.SparkSession

object FtrlSnapshotJoinUnionLog {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val dt = args(0)
    val hour = args(1)


    val snapshot1 = spark.table("dl_cpc.ml_snapshot_from_show")
      .filter(s"`date` = '$dt' and hour = '$hour'")

    val unionlog1 = spark.table("dl_cpc.cpc_union_log")
      .filter(s"`date` = '$dt' and hour = '$hour'")

    val join = unionlog1.join(snapshot1, Seq("searchid"), "left")
        .filter("feature_vector is not null")

    println(s"snapshot1 count = ${snapshot1.count()}")
    println(s"unionlog1 count = ${unionlog1.count()}")
    println(s"join count = ${join.count()}")

  }

}
