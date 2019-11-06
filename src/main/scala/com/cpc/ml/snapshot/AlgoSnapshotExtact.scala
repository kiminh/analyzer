package com.cpc.ml.snapshot

import mlmodel.mlmodel.ModelType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * created by xiongyao on 2019/10/29
  */
object AlgoSnapshotExtact {

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .appName("snapshot-example")
      .enableHiveSupport()
      .getOrCreate()

    var day = args(0).toString
    var hour = args(1).toString
    var minute = args(2).toString

    println("day:",day)
    println("hour:",hour)
    println("minute:",minute)

    val sql =
      s"""
         |select
         | mediaappsid,
         | adslottype,
         | insertionid,
         | userid,
         | ideaid,
         | uid,
         | modeltype,
         | adslotid,
         | feature_name,
         | feature_type,
         | feature_str_offset,
         | feature_str_list,
         | feature_int_offset,
         | feature_int_list,
         | feature_float_offset,
         | feature_float_list,
         | feature_int64_offset,
         | feature_int64_list,
         | day,
         | hour,
         | minute
         |from
         |algo_cpc.cpc_snapshot
         |where day = '2019-11-05'
         |and hour = '16'
         |and minute = '00'
         |limit 1000
      """.stripMargin

    var Rdd = spark.sql(sql).rdd

    val rawDataFromSnapshotLog = Rdd.map(
      x => {
        val mediaappsid = x.getAs[Int]("mediaappsid").toString
        val adslottype = x.getAs[Int]("adslottype")
        val feature_int64_offset = x.getAs[Seq[Int]]("feature_int64_offset").toArray
        val feature_name = x.getAs[Seq[String]]("feature_name").toArray
        val feature_str_offset = x.getAs[Seq[Int]]("feature_str_offset").toArray
        val feature_float_list = x.getAs[Seq[Float]]("feature_float_list").toArray
        val userid = x.getAs[Int]("userid")
        val insertionid = x.getAs[String]("insertionid")
        val ideaid = x.getAs[Int]("ideaid")
        val feature_int64_list = x.getAs[Seq[Long]]("feature_int64_list").toArray
        val feature_int_list = x.getAs[Seq[Int]]("feature_int_list").toArray
        val feature_type = x.getAs[Seq[Int]]("feature_type")
        val uid = x.getAs[String]("uid")
        val feature_float_offset = x.getAs[Seq[Int]]("feature_float_offset").toArray
        val modeltype = x.getAs[Int]("modeltype")
        val adslotid = x.getAs[Int]("adslotid").toString
        val feature_int_offset = x.getAs[Seq[Int]]("feature_int_offset").toArray
        val feature_str_list = x.getAs[Seq[String]]("feature_str_list").toArray
        val day = x.getAs[String]("day")
        val hour = x.getAs[String]("hour")
        val minute = x.getAs[String]("minute")
        val model_type = {
          if (modeltype == ModelType.MTYPE_CTR) {
            "qtt"
          } else if (modeltype == ModelType.MTYPE_CVR) {
            "qtt-cvr"
          } else {
            "unknown"
          }
        }

        val snapshotEvent = CpcSnapshotEvent(
          searchid = insertionid,
          media_appsid = mediaappsid,
          uid = uid,
          ideaid = ideaid,
          userid = userid,
          adslotid = adslotid,
          adslot_type = adslottype,
          model_type = model_type,
          day = day,
          hour = hour,
          minute = minute
        )
        snapshotEvent.setFeatures(feature_name, feature_str_offset, feature_str_list, feature_int_offset, feature_int_list, feature_int64_offset, feature_int64_list)

      }
    ).filter(x => x != null)

    val snapshotDataToGo = spark.createDataFrame(rawDataFromSnapshotLog)

    snapshotDataToGo.show(10,false)
    snapshotDataToGo.createOrReplaceTempView("snapshotDataToGo")

    val snapshotDataAsDataFrame = spark.sql(
      s"""
         |select
         |  searchid
         |  , media_appsid
         |  , uid
         |  , ideaid
         |  , userid
         |  , adslotid
         |  , adslot_type
         |  , model_type
         |  , content
         |  , feature_str
         |  , feature_int32
         |  , feature_int64
         |  , val_rec as val_rec
         |from snapshotDataToGo
       """.stripMargin)
      .repartition(100).write.mode(SaveMode.Overwrite).parquet(s"hdfs://emr-cluster2/warehouse/dl_cpc.db/cpc_snapshot_v2/dt=$day/hour=$hour/minute=$minute")

        spark.sql(
          s"""
             |ALTER TABLE dl_cpc.cpc_snapshot_v2
             | add if not exists PARTITION(`dt` = "$day", `hour` = "$hour", `minute` = "$minute")
             | LOCATION 'hdfs://emr-cluster2/warehouse/dl_cpc.db/cpc_snapshot_v2/dt=$day/hour=$hour/minute=$minute'
      """
            .stripMargin.trim)

    println("-- write to hive successfully -- ")
  }
}
