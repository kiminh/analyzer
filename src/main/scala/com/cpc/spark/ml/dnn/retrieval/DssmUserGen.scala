package com.cpc.spark.ml.dnn.retrieval

import com.cpc.spark.ml.dnn.retrieval.DssmRetrieval._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.{DataFrame, SparkSession}

object DssmUserGen {
  Logger.getRootLogger.setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dssm-user-gen")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0)
    val firstTime = args(1).toBoolean

    val userInfo = getData(spark, date)

    println("DAU User count = %d".format(userInfo.count()))

    val finalOutput = if (!firstTime) {
      import spark.implicits._

      val keyedUser = userInfo.rdd.map(x => (x.getAs[String]("uid"), x))

      val allUserInfo = spark.read.parquet("/user/cpc/hzh/dssm/all-user-info")
      allUserInfo.rdd.map(x => (x.getAs[String]("uid"), x))
        .cogroup(keyedUser)
        .map {
          x => {
            val row =
              if (x._2._2 != null && x._2._2.iterator != null && x._2._2.iterator.hasNext) {
                x._2._2.iterator.next()
              } else if (x._2._1 != null && x._2._1.iterator != null && x._2._1.iterator.hasNext) {
                x._2._1.iterator.next()
              } else {
                null
              }
            if (row != null) {
              ( row.getAs[Number]("sample_idx").longValue(),
                row.getAs[String]("uid"),
                row.getAs[Seq[Long]]("u_dense"),
                row.getAs[Seq[Int]]("u_idx0"),
                row.getAs[Seq[Int]]("u_idx1"),
                row.getAs[Seq[Int]]("u_idx2"),
                row.getAs[Seq[Long]]("u_id_arr"))
            } else {
              (-1L, "", Seq(0L), Seq(0), Seq(0), Seq(0), Seq(0L))
            }
          }
        }
        .toDF("sample_idx", "uid",
          "u_dense", "u_idx0", "u_idx1", "u_idx2", "u_id_arr")
        .filter(row => row.getAs[Long]("sample_idx") > 0)
    } else {
      userInfo
    }

    val n = finalOutput.count()
    println("Final user count = %d".format(n))

    finalOutput.repartition(100)
      .write
      .mode("overwrite")
      .parquet("/user/cpc/hzh/dssm/all-user-info")

    finalOutput.repartition(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/hzh/dssm/user-info-v0/" + date)
  }

  def getData(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._

    val userAppIdx = getUidApp(spark, date)
      .select($"uid", hashSeq("um1", "string")($"pkgs").alias("um1"))

    val userHistory = getHistoryFeature(spark, date)

    val sql =
      s"""
         |select 
         |  uid,
         |  max(os) as os,
         |  max(network) as network,
         |  max(ext['phone_price'].int_value) as phone_price,
         |  max(ext['brand_title'].string_value) as brand,
         |  max(province) as province,
         |  max(city) as city,
         |  max(ext['city_level'].int_value) as city_level,
         |  max(age) as age,
         |  max(sex) as sex
         | from dl_cpc.cpc_union_log
         |  where `date` = '$date'
         |  and isshow = 1 and ideaid > 0 and adslot_type = 1
         |  and media_appsid in ("80000001", "80000002")
         |  and length(uid) > 1
         |group by uid
      """.stripMargin
    println("--------------------------------")
    println(sql)
    println("--------------------------------")

    val re =
      spark.sql(sql)
        .join(userAppIdx, Seq("uid"), "leftouter")
        .join(userHistory, Seq("uid"), "leftouter")

    re.select(
      // user index
      $"uid",

      // user feature
      hash("u1")($"os").alias("u1"),
      hash("u2")($"network").alias("u2"),
      hash("u3")($"phone_price").alias("u3"),
      hash("u4")($"brand").alias("u4"),
      hash("u5")($"province").alias("u5"),
      hash("u6")($"city").alias("u6"),
      hash("u7")($"city_level").alias("u7"),
      hash("u8")($"age").alias("u8"),
      hash("u9")($"sex").alias("u9"),

      // user multi-hot
      array($"um1", $"um2", $"um3", $"um4", $"um5", $"um6", $"um7", $"um8", $"um9", $"um10",
        $"um11", $"um12", $"um13", $"um14", $"um15"
      ).alias("u_sparse_raw"))
      .select(
        $"uid",
        array($"u1", $"u2", $"u3", $"u4", $"u5", $"u6", $"u7", $"u8", $"u9").alias("u_dense"),
        mkSparseFeature_u($"u_sparse_raw").alias("u_sparse")
      )
      .select(
        $"uid",
        $"u_dense",
        $"u_sparse".getField("_1").alias("u_idx0"),
        $"u_sparse".getField("_2").alias("u_idx1"),
        $"u_sparse".getField("_3").alias("u_idx2"),
        $"u_sparse".getField("_4").alias("u_id_arr")
      )
      .rdd.zipWithUniqueId()
      .map { x =>
        (x._2,
          x._1.getAs[String]("uid"),
          x._1.getAs[Seq[Long]]("u_dense"),
          x._1.getAs[Seq[Int]]("u_idx0"),
          x._1.getAs[Seq[Int]]("u_idx1"),
          x._1.getAs[Seq[Int]]("u_idx2"),
          x._1.getAs[Seq[Long]]("u_id_arr")
        )
      }
      .toDF("sample_idx", "uid",
        "u_dense", "u_idx0", "u_idx1", "u_idx2", "u_id_arr")
  }
}