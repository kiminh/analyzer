package com.cpc.spark.ml.novel.retrieval

import com.cpc.spark.ml.novel.retrieval.DssmRetrieval._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.array

/**
  * author: huazhenhao
  * date: 11/13/18
  */
object DssmAdGen {
  Logger.getRootLogger.setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dssm-ad-gen")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0)

    val adInfo = getData(spark, date)

    val n = adInfo.count()
    println("Ad Info：total = %d".format(n))

    adInfo.repartition(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/wy/novel/dssm/ad-info-v0/" + date)
  }

  def getData(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._

    val sql =
      s"""
         |select
         |  ideaid,
         |  max(unitid) as unitid,
         |  max(adtype) as adtype,
         |  max(interaction) as interaction,
         |  max(bid) as bid,
         |  max(planid) as planid,
         |  max(userid) as userid,
         |  max(ext['adclass'].int_value) as adclass,
         |  max(ext_int['siteid']) as site_id
         |from dl_cpc.cpc_union_log where `date` = '$date'
         |  and isshow = 1 and ideaid > 0
         |  and media_appsid in ("80001098", "80001292")
         |  and length(uid) in (14, 15, 36)
         |group by ideaid
      """.stripMargin

    //广告分词
    val title_sql =
      """
        |select id as ideaid,
        |       split(tokens,' ') as words
        |from dl_cpc.ideaid_title
      """.stripMargin

    println("--------------------------------")
    println(sql)
    println("--------------------------------")

    val ad_features = spark.sql(title_sql)
      .select($"ideaid",
        hashSeq("am1", "string")($"words").alias("am1"))

    val re =
      spark.sql(sql).join(ad_features, Seq("ideaid"), "outer")

    re.select(
      $"ideaid",

      hash("a1")($"ideaid").alias("a1"),
      hash("a2")($"unitid").alias("a2"),
      hash("a3")($"adtype").alias("a3"),
      hash("a4")($"interaction").alias("a4"),
      hash("a5")($"bid").alias("a5"),
      hash("a6")($"planid").alias("a6"),
      hash("a7")($"userid").alias("a7"),
      hash("a8")($"adclass").alias("a8"),
      hash("a9")($"site_id").alias("a9"),

      // ad multi-hot
      array($"am1").alias("ad_sparse_raw")
      )
      .select(
        $"ideaid",
        array($"a1", $"a2", $"a3", $"a4", $"a5", $"a6", $"a7", $"a8", $"a9").alias("ad_dense"),
        mkSparseFeature_ad($"ad_sparse_raw").alias("ad_sparse")
      )
      .select(
        $"ideaid",
        $"ad_dense",
        $"ad_sparse".getField("_1").alias("ad_idx0"),
        $"ad_sparse".getField("_2").alias("ad_idx1"),
        $"ad_sparse".getField("_3").alias("ad_idx2"),
        $"ad_sparse".getField("_4").alias("ad_id_arr")
      )
      .rdd.zipWithUniqueId()
      .map { x =>
        (x._2,
          x._1.getAs[Number]("ideaid").toString,
          x._1.getAs[Seq[Long]]("ad_dense"),
          x._1.getAs[Seq[Int]]("ad_idx0"),
          x._1.getAs[Seq[Int]]("ad_idx1"),
          x._1.getAs[Seq[Int]]("ad_idx2"),
          x._1.getAs[Seq[Long]]("ad_id_arr")
        )
      }
      .toDF("sample_idx", "ideaid", "ad_dense", "ad_idx0", "ad_idx1", "ad_idx2", "ad_id_arr")
  }
}
