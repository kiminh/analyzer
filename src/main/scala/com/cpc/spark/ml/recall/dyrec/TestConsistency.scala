package com.cpc.spark.ml.recall.dyrec

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object TestConsistency {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TestConsistency").enableHiveSupport().getOrCreate()
    import spark.implicits._

    //hdfs://emr-cluster/user/cpc/aiclk_dataflow/realtime-30/adlist-v4user-profile-features/2019-09-11/09/part*
    //hdfs://emr-cluster/user/cpc/aiclk_dataflow/realtime-30/adlist-v4-up-show-click/2019-09-11/09/part*
    val originPath = "hdfs://emr-cluster/user/cpc/aiclk_dataflow/realtime-30/adlist-v4user-profile-features/2019-09-11/09/part*"
    //hdfs://emr-cluster2ns2/user/cpc_tensorflow_example_half/2019-09-11/11/1/part*
    //hdfs://emr-cluster/user/cpc/aiclk_dataflow/realtime/adlist-v4-up-show-click/2019-09-11/11/1/part*
    val newPath = "hdfs://emr-cluster2ns2/user/cpc_tensorflow_example_half/2019-09-11/10/1/part*"

    val originTfrecords = spark.read.format("tfrecords").option("recordType", "Example").load(originPath)
    val originNum = originTfrecords.count()
    originTfrecords.select($"sample_idx", $"id_arr", $"dense").createOrReplaceTempView("origin_table")

    val newTfrecords = spark.read.format("tfrecords").option("recordType", "Example").load(newPath)
    val newNum = newTfrecords.count()
    newTfrecords.select($"sample_idx", $"id_arr", $"dense").createOrReplaceTempView("new_table")

    val sql =
      """
        |select
        |    count(origin_idx) as all_num,
        |    count(new_idx) as new_num,
        |    sum(case when a_id_arr = b_id_arr then 1 else 0 end) as id_num,
        |    sum(case when a_dense = b_dense then 1 else 0 end) as dense_num
        |from
        |(
        |    select
        |        a.sample_idx as origin_idx,
        |        b.sample_idx as new_idx,
        |        a.id_arr as a_id_arr,
        |        b.id_arr as b_id_arr,
        |        a.dense as a_dense,
        |        b.dense as b_dense
        |    from
        |        origin_table a
        |    left join
        |        new_table b
        |    on
        |        a.sample_idx = b.sample_idx
        |) tmp
        |""".stripMargin
    println(sql)
    val rows = spark.sql(sql).collect()
    println("origin num: " + originNum + ", new num: " + newNum)
    for(row <- rows){
      println(row)
    }
  }
}
