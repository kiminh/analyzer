package com.cpc.spark.ml.dnn

import com.cpc.spark.common.Murmur3Hash
import org.apache.spark.sql.SparkSession

/**
  * 生成的tfrecord格式数据一致性检查
  * created time : 2018/11/30 15:40
  *
  * @author zhj
  * @version 1.0
  *
  */
object ConsistCheck {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("hash", hash _)

    //val path = "/user/cpc/dnn/daily_v6_list/dnntrain-2018-11-29"
    val path = "/user/cpc/zhj/daily_v3_list/dnntrain-2018-11-29"

    val data = spark.read.format("tfrecords")
      .option("recordType", "Example")
      .load(path + "/part-r-00001")
      .take(1)

    val sample_idx = data(0).getAs[Long]("sample_idx")
    val ideaid = data(0).getAs[Seq[Long]]("dense")(11)

    println("sample_idx:" + sample_idx)
    println("label:" + data(0).getAs[Seq[Int]]("label"))
    println("dense:" + data(0).getAs[Seq[Long]]("dense"))
    println("idx0:" + data(0).getAs[Seq[Long]]("idx0"))
    println("idx1:" + data(0).getAs[Seq[Long]]("idx1"))
    println("idx2:" + data(0).getAs[Seq[Long]]("idx2"))
    println("id_arr:" + data(0).getAs[Seq[Long]]("id_arr"))

    println("ud features : ")
    spark.read.parquet("").filter(s"hash('uid',uid)=$sample_idx")
      .show(false)

    println("ad features : ")
    spark.read.parquet("").filter(s"hash('f11#',ideaid)=$ideaid")
      .show(false)

  }

  def hash(prefix: String, v: String) = {
    Murmur3Hash.stringHash64(prefix + v, 0)
  }
}
