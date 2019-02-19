package com.cpc.spark.report

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.cpc.spark.common.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Partitioner
import org.apache.spark.sql.expressions.Window
import util.Random
import org.apache.spark.sql.functions.{col, expr, row_number}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

/**
  * Created by zhy (refined by fym) on 2019-02-16.
  *
  * this is an intermediate-level table, descending from trident (cpc_basedata_union_events).
  * contents: media charge and miscellaneous indices.
  *
  * <TODO> table name and/or columns TBD. 和数据分析同学沟通
  */

object MediaSlotChargeDaily {
  def main(args: Array[String]): Unit = {
    val date = args(0)

    val numPartitionsForSkewedData = 500

    val spark = SparkSession.builder()
      .appName("[trident] media charge and miscellaneous indices daily")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val sql =
      s"""
         |select *
         |from dl_cpc.cpc_basedata_union_events
         |where day='$date' and hour=3 and adslot_id<>""
       """.stripMargin

    val qtt_media_id = Array[Int](80000001, 80000002, 80000006, 80000064, 80000066, 80000062, 80000141, 80002480)
    val midu_media_id = Array[Int](80001539, 80002397, 80002477, 80002555, 80003172, 80001098, 80001292)
    val duanzi_media_id = Array[Int](80002819)

    val data = spark.sql(sql)
      .repartition(1000)
      .rdd
      .map { x =>
        val is_click = x.getAs[Int]("isclick")
        val spam_click = x.getAs[Int]("spam_click")
        val charge_type = x.getAs[Int]("charge_type")
        var charge_fee = charge_type match {
          case 1 => x.getAs[Int]("price") // cpc
          case 2 => x.getAs[Int]("price") / 1000 // cpm
          case _ => 0
        }
        if (charge_fee > 10000 || charge_fee < 0) { // handle dirty data
          charge_fee = 0
        }

        val media_id = x.getAs[String]("media_appsid").toInt
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
          case u if (u.length >= 15 && u.length <= 17) && (regex.findFirstMatchIn(u) != None) => "imei"
          case u if (u.length == 36) => "idfa"
          case _ => "other"
        }

        val charge = MediaSlotCharge(
          media_id = media_id,
          media_type = x.getAs[Int]("media_type"),
          media_name = media_name,
          adslot_id = x.getAs[String]("adslot_id"),
          adslot_type = x.getAs[Int]("adslot_type"),
          idea_id = x.getAs[Int]("ideaid"),
          unit_id = x.getAs[Int]("unitid"),
          plan_id = x.getAs[Int]("planid"),
          user_id = x.getAs[Int]("userid"),
          uid = uid, // => remove duplicate -> DAU
          uid_type = uid_type,
          adclass = x.getAs[Int]("adclass"),
          adtype = x.getAs[Int]("adtype"),
          dsp = x.getAs[Int]("adsrc"),
          charge_type = x.getAs[Int]("charge_type"),
          ctr_model_name = x.getAs[String]("ctr_model_name"),
          cvr_model_name = x.getAs[String]("cvr_model_name"),

          request = 1,
          fill = x.getAs[Int]("isfill"),
          impression = x.getAs[Int]("isshow"),
          click = is_click + spam_click,
          charged_click = is_click,
          spam_click = spam_click,
          cost = charge_fee,
          day = date
        )
        (charge.key, charge)
      }
      .reduceByKey((x, y) => x.sum(y))
      .map { x =>
        val mediaSlotCharge = x._2
        val click = mediaSlotCharge.click
        val impression = mediaSlotCharge.impression
        val cost = mediaSlotCharge.cost


        val ctr = if (impression==0) 0 else (click / impression).toDouble
        val cpm = if (impression==0) 0 else (cost / impression * 1000).toDouble
        val acp = if (click==0) 0 else (cost / click).toDouble

        (mediaSlotCharge.idea_id, mediaSlotCharge.copy(ctr = ctr, cpm = cpm, acp = acp))
      }

    //count distinct uid for each ideaid
    val usersRDD = spark.sql(sql)
      .select(
        col("ideaid"),
        col("uid"))
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
         |where `date`='$date' and hour=3 and label_type not in (8,9,10,11)
         |group by ideaid
       """.stripMargin

    val conversion_api_sql =
      s"""
         |select ideaid, count(*) as click, sum(label) as conv
         |from dl_cpc.ml_cvr_feature_v2
         |where `date`='$date' and hour=3
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

    val randomSeed = new Random()

    val mediaDataWithoutZero = data
        .filter( x => {
          x._2.idea_id != 0
        })

    val mediaDataWithZero = data
      .filter( x => {
        x._2.idea_id == 0
      })
      .map (x => { // partition.
        (randomSeed.nextInt(numPartitionsForSkewedData), x._2)
      })
      .persist()




    println(usersRDD.count())
    println(cvrRDD.count())

    val resultRDD = mediaDataWithoutZero
      .join(usersRDD, numPartitionsForSkewedData)
      .join(cvrRDD, numPartitionsForSkewedData)
      .map { r =>
        val mediaSlotCharge = r._2._1._1
        val idea_uids = r._2._1._2
        val cvr = r._2._2
        val cost = mediaSlotCharge.cost
        val arpu = (cost / idea_uids).toDouble

        mediaSlotCharge.copy(arpu = arpu, cvr = cvr)
      }

    println(resultRDD.count())

    for (i <- 0 until numPartitionsForSkewedData) {
      val mediaDataWithZeroAndIndex = mediaDataWithZero
        .filter( x => {
          x._1 == i
        })
      println("partial %s %s".format(i, mediaDataWithZeroAndIndex.count()))

      val partialJoinResult = mediaDataWithZeroAndIndex
        .join(usersRDD, numPartitionsForSkewedData)
        .join(cvrRDD, numPartitionsForSkewedData)
        .map { r =>
          val mediaSlotCharge = r._2._1._1
          val idea_uids = r._2._1._2
          val cvr = r._2._2
          val cost = mediaSlotCharge.cost
          val arpu = (cost / idea_uids).toDouble

          mediaSlotCharge.copy(arpu = arpu, cvr = cvr)
        }

      resultRDD.union(partialJoinResult)
      println("count %s %s".format(i, resultRDD.count()))
    }


    resultRDD
      .toDF()
      .repartition(100)
      .write
      //.partitionBy("day")
      .mode(SaveMode.Append) // 修改为Append
      .parquet(s"hdfs://emr-cluster2/warehouse/dl_cpc.db/temp/trident_media_charge")

    /*spark.sql(
      s"""
         |alter table dl_cpc.xx if not exists add partitions(day = "$date")
         |location 'hdfs://emr-cluster2/warehouse/dl_cpc.db/temp/trident_media_charge'
       """.stripMargin)*/

    println("done.")

  }
}

