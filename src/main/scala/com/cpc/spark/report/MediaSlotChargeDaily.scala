package com.cpc.spark.report

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by zhy (refined by fym) on 2019-02-16.
  *
  * this is an intermediate-level table, descending from trident (cpc_basedata_union_events).
  * contents: media charge and miscellaneous indices.
  */

object MediaSlotChargeDaily {
  def main(args: Array[String]): Unit = {
    val day = args(0)

    val spark = SparkSession.builder()
      .appName("[trident] media charge and miscellaneous indices daily")
      .enableHiveSupport()
      .getOrCreate()

    val sql =
      s"""
         |select *
         |from dl_cpc.cpc_basedata_union_events
         |where day='$day' and adslotid >0
       """.stripMargin


    val qtt_media_id = Array[Int](80000001, 80000002, 80000006, 80000064, 80000066, 80000062, 80000141, 80002480)
    val midu_media_id = Array[Int](80001539, 80002397, 80002477, 80002555, 80003172, 80001098, 80001292)
    val duanzi_media_id = Array[Int](80002819)

    val data = spark.sql(sql)
      .repartition(1000)
      .rdd
      .map { x =>
        val isclick = x.getAs[Int]("isclick")
        val spam_click = x.getAs[Int]("spam_click")
        val chargeType = x.getAs[Int]("charge_type")
        var charge_fee = chargeType match {
          case 1 => x.getAs[Int]("price") //cpc
          case 2 => x.getAs[Int]("price") / 1000 //cpm
          case _ => 0
        }
        if (charge_fee > 10000 || charge_fee < 0) { //handle dirty data
          charge_fee = 0
        }

        val media_id = x.getAs[String]("media_id").toInt
        val media_name = media_id match {
          case m if qtt_media_id.contains(m) => "qtt"
          case m if midu_media_id.contains(m) => "midu"
          case m if duanzi_media_id.contains(m) => "duanzi"
          case _ => "other"
        }

        val regex = "^[0-9]*$".r
        val uid = x.getAs[String]("uid")
        val uid_type = uid match {
          case u if u == "" => "empty"
          case u if u.contains(".") => "ip"
          case u if u.contains("000000") => "zero_device"
          case u if (u.length == 15 || u.length == 16 || u.length == 17) && (regex.findFirstMatchIn(u) != None) => "imei"
          case u if (u.length == 36) => "idfa"
          case _ => "other"
        }

        val charge = MediaSlotCharge(
          media_id = media_id,
          media_type = x.getAs[Int]("media_type"),
          media_name = media_name,
          adslot_id = x.getAs[Int]("adslot_id"),
          adslot_type = x.getAs[Int]("adslot_type"),
          idea_id = x.getAs[Int]("idea_id"),
          unit_id = x.getAs[Int]("unit_id"),
          plan_id = x.getAs[Int]("plan_id"),
          user_id = x.getAs[Int]("user_id"),
          uid = uid,
          uid_type = uid_type,
          adclass = x.getAs[Int]("adclass"),
          adtype = x.getAs[Int]("adtype"),
          dsp = x.getAs[Int]("adsrc"),
          charge_type = x.getAs[Int]("charge_type"),
          ctr_model_name = x.getAs[String]("ctr_model_name"),
          cvr_model_name = x.getAs[String]("cvr_model_name"),

          request = 1,
          fill = x.getAs[Int]("fill"),
          impression = x.getAs[Int]("impression"),
          click = isclick + spam_click,
          charged_click = isclick,
          spam_click = spam_click,
          cost = charge_fee,
          date = day
        )
        (charge.key, charge)
      }
      .reduceByKey((x, y) => x.sum(y))
      .map { x =>
        val mediaSlotCharge = x._2
        val click = mediaSlotCharge.click
        val impression = mediaSlotCharge.impression
        val cost = mediaSlotCharge.cost


        val ctr = (click / impression).toDouble
        val cpm = (cost / impression * 1000).toDouble
        val acp = (cost / click).toDouble
        mediaSlotCharge.copy(ctr = ctr, cpm = cpm, acp = acp)
        (mediaSlotCharge.idea_id, mediaSlotCharge)
      }

    //count distinct uid for each ideaid
    val usersRDD = spark.sql(sql)
      .select("ideaid", "uid")
      .groupBy("ideaid")
      .agg(expr("count(distinct uid)").alias("idea_uids"))
      .rdd
      .map { r =>
        (r.getAs[Int]("ideaid"), r.getAs[Long]("idea_uids"))
      }

    //count cvr for each ideaid
    val conversion_info_flow_sql =
      s"""
         |select ideaid, count(*) as click, sum(label2) as conv
         |from dl_cpc.ml_cvr_feature_v1
         |where date='$day' and label_type not in (8,9,10,11)
         |group by ideaid
       """.stripMargin

    val conversion_api_sql =
      s"""
         |select ideaid, count(*) as click, sum(label) as conv
         |from dl_cpc.ml_cvr_feature_v2
         |where date='$day'
         |group by ideaid
       """.stripMargin

    val cvrRDD = (spark.sql(conversion_info_flow_sql)).union(spark.sql(conversion_api_sql))
      .rdd
      .map { r =>
        val ideaid = r.getAs[Int]("ideaid")
        val click = r.getAs[Long]("click")
        val conv = r.getAs[Long]("conv")
        val cvr = (conv / click).toDouble
        (ideaid, cvr)
      }


    val resultRDD = data.join(usersRDD).join(cvrRDD)
      .map { r =>
        val mediaSlotCharge = r._2._1._1
        val idea_uids = r._2._1._2
        val cvr = r._2._2
        val cost = mediaSlotCharge.cost
        val arpu = (cost / idea_uids).toDouble
        mediaSlotCharge.copy(arpu = arpu, cvr = cvr)
      }

    spark.createDataFrame(resultRDD)
      .repartition(10)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"hdfs://emr-cluster2/warehouse/dl_cpc.db/")

    spark.sql(
      s"""
         |alter table dl_cpc.xx if not exists add partitions(day = "$day")
         |location 'hdfs://emr-cluster2/warehouse/dl_cpc.db/'
       """.stripMargin)

    println("done.")

  }
}

