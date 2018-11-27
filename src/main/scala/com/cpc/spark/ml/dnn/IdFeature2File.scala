package com.cpc.spark.ml.dnn

import java.io.{File, FileOutputStream, PrintWriter}

import com.cpc.spark.common.Murmur3Hash
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import mlmodel.mlmodel.{ID2idx, ad_idx}

/**
  * 生成广告title分词hash后文件供上线使用
  * ideaid
  * created time : 2018/11/27 14:01
  *
  * @author zhj
  * @version 1.0
  *
  */
object IdFeature2File {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val path = args(0)

    spark.udf.register("hashSeq", hashSeq4Hive _)

    val ideaid_sql =
      """
        |select id as ideaid,
        |       hashSeq(split(tokens,' ')) as m16
        |from dl_cpc.ideaid_title
      """.stripMargin

    println("-----------------------------")
    println(ideaid_sql)
    println("-----------------------------")

    var arr_idx = Seq[ID2idx]()

    spark.sql(ideaid_sql)
      .rdd
      .map { x =>
        ID2idx(x.getAs[Int]("ideaid").toString, x.getAs[Seq[Long]]("m16"))
      }
      .collect()
      .foreach(x => arr_idx = arr_idx :+ x)

    ad_idx(arr_idx).writeTo(new FileOutputStream(path))

  }

  def hashSeq4Hive(values: Seq[String]): Seq[Long] = {
    if (values.isEmpty) Seq(Murmur3Hash.stringHash64("m16", 0)) else
      for (v <- values) yield Murmur3Hash.stringHash64(v, 0)
  }
}
