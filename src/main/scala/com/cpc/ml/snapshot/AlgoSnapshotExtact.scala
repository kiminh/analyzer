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
    var intMinute = args(2).toString.toInt
    var minute = getMinute(intMinute)

    println("day=",day)
    println("hour=",hour)
    println("minute=",minute)

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
         |where day = '$day'
         |and hour = '$hour'
         |and minute = '$minute'
      """.stripMargin

    println(sql)
    var Rdd = spark.sql(sql).rdd
    var count = Rdd.count()
    println("data count:",count)

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
          if (modeltype == ModelType.MTYPE_CTR.value) {
            "qtt"
          } else if (modeltype == ModelType.MTYPE_CVR.value) {
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
         |  , content as contentstr
         |  , content['model_name'][0] as model_name
         |  , content['model_id'][1] as model_id
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

  def getMinute(intMinute : Int): String = {
    var minute = ""
    if (intMinute >= 0 && intMinute <5){
      minute = "00"
    } else if (intMinute >= 5 && intMinute <10){
      minute = "05"
    }  else if (intMinute >= 10 && intMinute <15){
      minute = "10"
    }
    else if (intMinute >= 15 && intMinute <20){
      minute = "15"
    }
    else if (intMinute >= 20 && intMinute <25){
      minute = "20"
    }
    else if (intMinute >= 25 && intMinute <30){
      minute = "25"
    }
    else if (intMinute >= 30 && intMinute <35){
      minute = "30"
    }
    else if (intMinute >= 35 && intMinute <40){
      minute = "35"
    }
    else if (intMinute >= 40 && intMinute <45){
      minute = "40"
    }
    else if (intMinute >= 45 && intMinute <50){
      minute = "45"
    }
    else if (intMinute >= 50 && intMinute <55){
      minute = "50"
    } else {
      minute = "55"
    }
    return minute
  }
}
