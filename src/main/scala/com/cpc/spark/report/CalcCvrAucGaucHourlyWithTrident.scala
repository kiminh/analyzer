package com.cpc.spark.report

import com.cpc.spark.common.{CalcMetrics, Utils}

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import hivemall.evaluation.AUCUDAF
/*
  by fym on 2019-02-22.
 */

object CalcCvrAucGaucHourlyWithTrident {

  def main(args: Array[String]): Unit = {
    val date = args(0)
    val hour = args(1)

    val whitelist = CvrCtrAucGaucWhitelist.wl

    val spark = SparkSession.builder()
      .appName("[trident] calculate cvr/auc/gauc for various models")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // spark.udf.register("auc", AUCUDAF)
    val auc = new hivemall.evaluation.AUCUDAF

    /*val rawDataFromTrident = spark.sql(
      s"""
         |select
         |  cvr_model_name
         |  , adslot_type
         |  , auc(exp_cvr, conversion)
         |  , sum(conversion)
         |  , count(*)
         |  , avg(exp_cvr)
         |  , sum(conversion)/count(*)
         |from (
         |  select
         |    coalesce(b.conversion, 0) as conversion
         |    , a.raw_cvr/1000000 as exp_cvr
         |    , a.cvr_model_name
         |  from dl_cpc.cpc_basedata_union_events a
         |  left join (
         |    select
         |      searchid
         |      , ideaid
         |      , case
         |          when adclass in (110110100, 125100100) then if(label_type in(1,2,3), label2, 0)
         |          when (adslot_type<>7 and adclass like '100%') then if(label_type in (4, 5), label2, 0)
         |          when (adslot_type=7 and adclass like '100%') then if(label_type=12, label2, 0)
         |          else if(label_type=6, label2, 0)
         |      end as conversion
         |      , label2 as conversion_for_novel
         |    from dl_cpc.ml_cvr_feature_v1
         |    where `date`='$date'
         |      and hour=$hour
         |    union
         |    select
         |      searchid
         |      , ideaid
         |      , label as conversion
         |      , label as conversion_for_label
         |    from dl_cpc.ml_cvr_feature_v2
         |    where date='$date'
         |      and hour=$hour) b
         |    on a.searchid=b.searchid
         |    and a.ideaid=b.ideaid
         |  where a.day='$date'
         |    and a.media_appsid in ('80000001', '80000002', '80001098', '80001292')
         |    and a.hour=$hour
         |    and a.isclick=1
         |);
       """.stripMargin
    )*/

    val rawDataFromTrident = spark.sql(
      s"""
         |select
         |  a.cvr_model_name
         |  , a.adslot_type
         |  , coalesce(b.conversion, 0) as conversion
         |  , a.raw_cvr/1000000 as exp_cvr
         |  , a.day
         |  , a.hour
         |from dl_cpc.cpc_basedata_union_events a
         |left join (
         |  select
         |    searchid
         |    , ideaid
         |    , case
         |        when adclass in (110110100, 125100100) then if(label_type in(1,2,3), label2, 0)
         |        when (adslot_type<>7 and adclass like '100%') then if(label_type in (4, 5), label2, 0)
         |        when (adslot_type=7 and adclass like '100%') then if(label_type=12, label2, 0)
         |        else if(label_type=6, label2, 0)
         |    end as conversion
         |    , label2 as conversion_for_novel
         |  from dl_cpc.ml_cvr_feature_v1
         |  where `date`='$date'
         |    and hour=$hour
         |  union
         |  select
         |    searchid
         |    , ideaid
         |    , label as conversion
         |    , label as conversion_for_label
         |  from dl_cpc.ml_cvr_feature_v2
         |  where date='$date'
         |    and hour=$hour) b
         |  on a.searchid=b.searchid
         |  and a.ideaid=b.ideaid
         |where a.day='$date'
         |  and a.media_appsid in ('80000001', '80000002', '80001098', '80001292')
         |  and a.hour=$hour
         |  and a.isclick=1
       """.stripMargin
    )

    val dataToGo = rawDataFromTrident
      .groupBy(
        col("cvr_model_name"),
        col("adslot_type"),
        col("day"),
        col("hour")
      )
      .agg(
        expr("auc(exp_cvr, conversion)").alias("auc")
      )
      .write
      .saveAsTable("test.fym_607")

    /*val keyFromDsp = spark.sql(
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
        |  , ideaid
        |  , uid
        |  , cvr_model_name
        |  , raw_cvr as score
        |  , adslot_type
        |from dl_cpc.cpc_basedata_union_events
        |where
        |  day='%s'
        |  and hour=%s
        |  and isshow=1
        |  and isclick=1
        |  and exp_cvr is not null
        |  and media_appsid in ('80000001', '80000002', '80001098', '80001292')
        |  and antispam_score>=10000
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
      .withColumn("model_name", Utils.trimModelName(col("cvr_model_name")))
      .cache()
      // .drop("exptags")

    val cvrDataFromV1 = spark.sql(
      s"""
       |select
       |  searchid
       |  , ideaid
       |  , label_type
       |  , case
       |      when adclass in (110110100, 125100100) then if(label_type in (1, 2, 3), label2, 0)
       |      when (adslot_type<>7 and adclass like "100%") then if(label_type in (4, 5), label2, 0)
       |      when (adslot_type=7 and adclass like "100%") then if(label_type=12, label2, 0)
       |      else if(label_type=6, label2, 0)
       |  end as label
       |  , label2 as label_for_novel
       |from dl_cpc.ml_cvr_feature_v1
       |where `date` = '$date' and hour = $hour
     """
      .stripMargin
    )
      .drop("label_type")

    val cvrDataFromV2 = spark.sql(
      s"""
         |select
         |  searchid
         |  , ideaid
         |  , label as label
         |  , 0 as label_for_novel
         |from dl_cpc.ml_cvr_feature_v2
         |where `date` = '%s' and hour = %s
     """
        .format(date, hour)
        .stripMargin
    )

    val cvrData = cvrDataFromV1.union(cvrDataFromV2)

    val tridentDataWithCvr = rawDataFromTrident
      // .withColumn("model_name", Utils.trimModelName(col("cvr_model_name")))
      .join(cvrData, Seq("searchid", "ideaid"))
      .filter("model_name<>''")
      .cache()

    val modelNames = tridentDataWithCvr
      .select("model_name")
      .distinct()
      .collect()
      .map(x => x.getAs[String]("model_name"))
    //println(exptag.mkString(" "))

    val adSlotTypes = tridentDataWithCvr
      .select("adslot_type")
      .distinct()
      .collect()
      .map(x => {
        x.getAs[Int]("adslot_type")
      })

    println(modelNames.size)

    val aucGaucBuffer = ListBuffer[AucGauc]()

    for (adslotType <- adSlotTypes) {
      for (model <- modelNames) {

        val tridentDataWithCvrFiltered = tridentDataWithCvr
          .filter(s"model_name='%s' and adslot_type=%s".
            format(
              model,
              adslotType.toString()
            ))
          .coalesce(400)
          .cache()

        val countOne = tridentDataWithCvrFiltered
          .where("label=1")
          .count()
        val countZero = tridentDataWithCvrFiltered
          .where("label=0")
          .count()

        if (countOne > 0 && countZero > 0) {
          var aucROC = 0.0

          if (model.contains("novel")) {
            aucROC = auc(
                tridentDataWithCvrFiltered,
                "label_for_novel"
              )
          } else {
            aucROC = CalcMetrics
              .getAuc(
                spark,
                tridentDataWithCvrFiltered,
              "label"
              )
          }


          val gaucList = CalcMetrics
            .getGauc(spark, tridentDataWithCvrFiltered, "uid")
            .collect()

          val cvrValue = tridentDataWithCvrFiltered
            .agg(
              expr("sum(label)/count(*)").alias("cvr")) // calculate cvr.
            .select("cvr")
            .rdd
            .map(x => {
              x.getAs[Double]("cvr")
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
              cvr = cvrValue,
              adslot_type = adslotType,
              model = model,
              day = date,
              hour = hour
            )
          } // try

          tridentDataWithCvrFiltered.unpersist()
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

    */

    /*rawDataFromTrident
    //aucGauc
      .repartition(1)
      .write
      .partitionBy("day", "hour")
      .mode(SaveMode.Append)
      .parquet("hdfs://emr-cluster2/warehouse/dl_cpc.db/cvr_auc_gauc_hourly/")

    spark.sql(
      s"""
         |ALTER TABLE dl_cpc.cvr_auc_gauc_hourly
         | add if not exists PARTITION(`day` = "$date", `hour` = "$hour")
         | LOCATION 'hdfs://emr-cluster2/warehouse/dl_cpc.db/cvr_auc_gauc_hourly/day=$date/hour=$hour'
      """.stripMargin.trim)
    println(" -- successfully generated partition: day=%s/hour=%s -- ")
*/
    spark.stop()

    /*aucGaucFiltered
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("hdfs://emr-cluster2/warehouse/dl_cpc.db/cvr_auc_gauc_hourly_togo/")*/

    /*try {



      println("-- insert into hdfs successfully --")
    } catch {
      case e: Exception =>
        println("-- error(s) occurred while writing row(s) --")
    } finally {
      spark.stop()
    }*/

    // .insertInto("dl_cpc.cpc_qtt_cvr_auc_gauc_hourly")
    // println("insert into dl_cpc.cpc_qtt_cvr_auc_gauc_hourly success!")

    /*val reportTable = "report2.cpc_qtt_cvr_auc_gauc_hourly"
    val delSQL = s"delete from $reportTable where `date` = '$date' and hour = '$hour'"
    OperateMySQL.update(delSQL) //先删除历史数据
    OperateMySQL.insert(aucGauc, reportTable) //插入数据*/

    spark.stop()
  }

  def getExptag = udf((exptags:String) => {
    val s = exptags.toString.split(",")
    var exptag = ""
    for (str <- s) {
      if (str.contains("cvrmodel")) {
        val i = str.indexOf("=")
        exptag = str.substring(i + 1).trim
      }
    }
    exptag
  })

  case class AucGauc(
                      var auc : Double = 0.0,
                      var gauc : Double = 0.0,
                      var cvr : Double = 0.0,
                      var adslot_type : Int = 0,
                      var model : String = "",
                      var day : String = "",
                      var hour : String = ""
                    )
}
