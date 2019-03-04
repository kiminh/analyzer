package com.cpc.spark.report

import com.cpc.spark.common.{CalcMetrics, Utils}

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/*
  by fym on 2019-02-25.
 */

object CalcCtrAucGaucHourlyWithTrident {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val hour = args(1)

    val whitelist = CvrCtrAucGaucWhitelist.wl

    val spark = SparkSession.builder()
      .appName("[trident] calculate ctr/auc/gauc for various models")
      .enableHiveSupport()
      .getOrCreate()

    // val influxdb = InfluxDB.connect("http://192.168.80.163", 8086)

    import spark.implicits._
    val model = "ctrmodel"

    val keyFromDsp = spark.sql(
      s"""
         |select
         |  searchid
         |from dl_cpc.cpc_basedata_search_dsp
         |where day='%s'
         |  and hour=%s
         |  and src=1
         |  and adnum>1
       """
        .format(date, hour)
        .stripMargin)

    val keyFromTrident = spark.sql(
      s"""
         |select
         |  searchid
         |from dl_cpc.cpc_basedata_union_events
         |where day='%s'
         |  and hour=%s
         |  and adsrc=1
       """
        .format(date, hour)
        .stripMargin)

    val keyCombined = keyFromDsp
      .union(keyFromTrident)

    val rawDataFromTrident = spark.sql(s"""
        |select
        |  searchid
        |  , ctr_model_name
        |  , uid
        |  , case when isclick is null then 0 else 1 end as label
        |  , exp_ctr as score
        |  , adslot_type
        |from dl_cpc.cpc_basedata_union_events
        |where
        |  day='%s'
        |  and hour=%s
        |  and isshow=1
        |  and exp_ctr is not null
        |  and media_appsid in ('80000001', '80000002', '80001098', '80001292')
        |  and (antispam_score is null or antispam_score>=10000)
        |  and ideaid>0
        |  and userid>0
        |  and (charge_type IS NULL OR charge_type=1)
       """
      .format(
        date,
        hour
      )
      .stripMargin)
      .join(keyCombined, Seq("searchid"), "inner")
    // .withColumn("model_name", Utils.trimModelName(col("ctr_model_name")))
    // .drop("exptags")

    /*val ctrData = spark.sql(
      s"""
         |select
         |  searchid,
         |  label as label
         |from dl_cpc.ml_ctr_feature_v1
         |where `date` = '%s' and hour = %s
     """
        .format(date, hour)
        .stripMargin
    )*/

    val tridentDataWithctr = rawDataFromTrident
      .withColumn("model_name", Utils.trimModelName(col("ctr_model_name")))
      // .join(ctrData, Seq("searchid"))
      .filter("model_name<>''")
      .cache()

    val modelNames = tridentDataWithctr
      .select("model_name")
      .distinct()
      .collect()
      .map(x => x.getAs[String]("model_name"))
    //println(exptag.mkString(" "))

    val adSlotTypes = tridentDataWithctr
      .select("adslot_type")
      .distinct()
      .collect()
      .map(x => {
        x.getAs[Int]("adslot_type")
      })

    val aucGaucBuffer = ListBuffer[AucGauc]()

    for (adslotType <- adSlotTypes) {
      for (model <- modelNames) {

        val tridentDataWithctrFiltered = tridentDataWithctr
          .filter(s"model_name='%s' and adslot_type=%s".
            format(
              model,
              adslotType.toString()
            ))
          .coalesce(400)
          .cache()

        val countOne = tridentDataWithctrFiltered
          .where("label=1")
          .count()
        val countZero = tridentDataWithctrFiltered
          .where("label=0")
          .count()

        if (countOne > 0 && countZero > 0) {
          val aucROC = CalcMetrics
            .getAuc(spark, tridentDataWithctrFiltered, "label")

          val gaucList = CalcMetrics
            .getGauc(spark, tridentDataWithctrFiltered, "uid")
            .collect()

          val ctrValue = tridentDataWithctrFiltered
            .agg(
              expr("sum(label)/count(*)").alias("ctr")) // calculate ctr.
            .select("ctr")
            .rdd
            .map(x => {
              x.getAs[Double]("ctr")
            })
            .collect()(0)

          val gaucFiltered = gaucList
            .filter(x => {
              x.getAs[Double]("auc") != -1
            })

          var gaucROC = 0.0

          try {
            val gaucFilteredOnceMore = gaucFiltered
              .map(x => {
                (x.getAs[Double]("auc") * x.getAs[Double]("sum"),
                  x.getAs[Double]("sum"))
              })
              .reduce((x, y) => {
                (x._1 + y._1, x._2 + y._2)
              })

            gaucROC = gaucFilteredOnceMore._1 * 1.0 / gaucFilteredOnceMore._2
          } catch {
            case e: Exception =>
              println("-- error(s) occurred while calculating auc/gauc(s) --")
          } finally {
            aucGaucBuffer += AucGauc(
              auc = aucROC,
              gauc = gaucROC,
              ctr = ctrValue,
              adslot_type = adslotType,
              model = model,
              day = date,
              hour = hour
            )
          } // try

          tridentDataWithctrFiltered.unpersist()
        } // if 1
      } // for 2
    } // for 1

    val aucGauc = aucGaucBuffer
      .toList
      .toDF()
      .persist()

    /*val aucGaucFiltered = aucGauc
      .filter( x => {
        val model = x.getAs[String]("model")
        val adslotType = x.getAs[Int]("adslot_type")

        (!whitelist.keys.toArray.contains(model)) || (whitelist.keys.toArray.contains(model) && !whitelist(model).contains(adslotType))
      })
      .filter( x => {
        x.getAs[Double]("auc") < 0.7
      })*/

    aucGauc
      .repartition(1)
      .write
      .partitionBy("day", "hour")
      .mode(SaveMode.Append)
      .parquet("hdfs://emr-cluster2/warehouse/dl_cpc.db/ctr_auc_gauc_hourly/")

    spark.sql(
      s"""
         |ALTER TABLE dl_cpc.ctr_auc_gauc_hourly
         | add if not exists PARTITION(`day` = "$date", `hour` = "$hour")
         | LOCATION 'hdfs://emr-cluster2/warehouse/dl_cpc.db/ctr_auc_gauc_hourly/day=$date/hour=$hour'
      """.stripMargin.trim)
    println(" -- successfully generated partition: day=%s/hour=%s -- ")

    /*aucGaucFiltered
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("hdfs://emr-cluster2/warehouse/dl_cpc.db/ctr_auc_gauc_hourly_togo/")*/

    println("-- insert into hdfs successfully --")

    spark.stop()

    /*try {
      val aucGauc = aucGaucBuffer
        .toList
        .toDF()
        .persist()

      val aucGaucFiltered = aucGauc
        .filter( x => {
          val model = x.getAs[String]("model")
          val adslotType = x.getAs[Int]("adslot_type")

          (!whitelist.keys.toArray.contains(model)) || (whitelist.keys.toArray.contains(model) && !whitelist(model).contains(adslotType))
        })
        .filter( x => {
          x.getAs[Double]("auc") < 0.7
        })

      aucGauc
        .repartition(1)
        .write
        .partitionBy("day", "hour")
        .mode(SaveMode.Append)
        .parquet("hdfs://emr-cluster2/warehouse/dl_cpc.db/ctr_auc_gauc_hourly/")

      spark.sql(
        s"""
           |ALTER TABLE dl_cpc.ctr_auc_gauc_hourly
           | add if not exists PARTITION(`day` = "$date", `hour` = "$hour")
           | LOCATION 'hdfs://emr-cluster2/warehouse/dl_cpc.db/ctr_auc_gauc_hourly/day=$date/hour=$hour'
      """.stripMargin.trim)
      println(" -- successfully generated partition: day=%s/hour=%s -- ")

      aucGaucFiltered
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .parquet("hdfs://emr-cluster2/warehouse/dl_cpc.db/ctr_auc_gauc_hourly_togo/")

      println("-- insert into hdfs successfully --")
    } catch {
      case e: Exception =>
        println("-- error(s) occurred while writing row(s) --")
    } finally {
      spark.stop()
    }

    // .insertInto("dl_cpc.cpc_qtt_ctr_auc_gauc_hourly")
    // println("insert into dl_cpc.cpc_qtt_ctr_auc_gauc_hourly success!")

    /*val reportTable = "report2.cpc_qtt_ctr_auc_gauc_hourly"
    val delSQL = s"delete from $reportTable where `date` = '$date' and hour = '$hour'"
    OperateMySQL.update(delSQL) //先删除历史数据
    OperateMySQL.insert(aucGauc, reportTable) //插入数据*/

    spark.stop()*/
  }

  case class AucGauc(
                      var auc : Double = 0.0,
                      var gauc : Double = 0.0,
                      var ctr : Double = 0.0,
                      var adslot_type : Int = 0,
                      var label_type : Int = 0,
                      var model : String = "",
                      var day : String = "",
                      var hour : String = ""
                    )
}
