package com.cpc.spark.ml.calibration.exp

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import com.cpc.spark.common.Utils
import scala.collection.mutable.{ListBuffer, WrappedArray}
import com.cpc.spark.ml.calibration.exp.LrCalibrationOnQtt.calculateAuc
import com.cpc.spark.ocpc.OcpcUtils._
import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.time.LocalDateTime
import org.apache.spark.ml.{Pipeline, PipelineStage}
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import com.cpc.spark.common.Utils
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.functions._

import scala.collection.mutable

object LinearRegressionOnQttCvrCalibration {
  def main(args: Array[String]): Unit = {
    // new calibration
    val endDate = args(0)
    val endHour = args(1)
    val hourRange = args(2).toInt
    val media = args(3)
//    val model = args(4)
//    val calimodel = args(5)
//    val k = args(6)
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)


    val endTime = LocalDateTime.parse(s"$endDate-$endHour", DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
    val startTime = endTime.minusHours(Math.max(hourRange - 1, 0))
    val startDate = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val startHour = startTime.format(DateTimeFormatter.ofPattern("HH"))

    println(s"endDate=$endDate")
    println(s"endHour=$endHour")
    println(s"hourRange=$hourRange")
    println(s"startDate=$startDate")
    println(s"startHour=$startHour")
    // parse and process input
    val model = "qtt-cvr-dnn-rawid-v1wzjf-aibox"
    val calimodel ="qtt-cvr-dnn-rawid-v1wzjf-aibox"

    // build spark session
    val spark = Utils.buildSparkSession("hourlyCalibration")
    val selectCondition1 = getTimeRangeSql(startDate, startHour, endDate, endHour)
    val selectCondition2 = getTimeRangeSql4(startDate, startHour, endDate, endHour)
    import spark.implicits._

    // get union log
//    val sql = s"""
//                      |select a.searchid, cast(b.raw_cvr as bigint) as rawcvr, substring(a.adclass,1,6) as adclass,
//                      |b.cvr_model_name as model, b.adslot_id as adslotid, a.ideaid,user_show_ad_num, exp_cvr,
//                      |unitid,userid,click_count,click_unit_count,
//                      |if(c.iscvr is not null,1,0) iscvr,if(hour>$endHour,hour-$endHour,hour+24-$endHour) hourweight
//                      |from
//                      |(select searchid,ideaid,unitid,userid,adclass,hour
//                      |  from dl_cpc.cpc_basedata_click_event
//                      |  where $selectCondition2
//                      |  and $mediaSelection and isclick = 1
//                      |  and adsrc in (1,28)
//                      |  and antispam_score = 10000
//                      |  )a
//                      |  join
//                      |  (select searchid,ideaid,user_show_ad_num,conversion_goal,raw_cvr,cvr_model_name,adslot_id,exp_cvr
//                      |  ,click_count,click_unit_count
//                      |  from
//                      |  dl_cpc.cpc_basedata_adx_event
//                      |  where  $selectCondition2
//                      |  and $mediaSelection
//                      |  and cvr_model_name in ('$calimodel','$model')
//                      |  AND bid_mode = 0
//                      |  and conversion_goal>0) b
//                      |    on a.searchid = b.searchid and a.ideaid = b.ideaid
//                      | left join
//                      | (select distinct searchid,conversion_goal,1 as iscvr
//                      |  from dl_cpc.ocpc_quick_cv_log
//                      |  where  $selectCondition1) c
//                      |  on a.searchid = c.searchid and b.conversion_goal=c.conversion_goal
//       """.stripMargin
//
//    println(s"sql:\n$sql")
    val data= spark.sql("select *,rawcvr as raw_cvr from dl_cpc.wy_calibration_sample_2019_10_10")
    data.show(10)

    val dataDF = data.groupBy("ideaid").count()
      .withColumn("tag",when(col("count")>60,1).otherwise(0))
      .join(data,Seq("ideaid"),"left")
      .withColumn("label",col("iscvr"))
      .withColumn("ideaid",when(col("tag")===1,col("ideaid")).otherwise(9999999))
      .select("searchid","ideaid","user_show_ad_num","adclass","adslotid","label","unitid","raw_cvr","exp_cvr")
    dataDF.show(10)

    val categoricalColumns = Array("ideaid","adclass","adslotid")

    val stagesArray = new ListBuffer[PipelineStage]()
    for (cate <- categoricalColumns) {
      val indexer = new StringIndexer().setInputCol(cate).setOutputCol(s"${cate}Index")
      val encoder = new OneHotEncoder().setInputCol(indexer.getOutputCol).setOutputCol(s"${cate}classVec")
      stagesArray.append(indexer,encoder)
    }

    val numericCols = Array("user_show_ad_num","raw_cvr")
    val assemblerInputs = categoricalColumns.map(_ + "classVec") ++ numericCols
    /**使用VectorAssembler将所有特征转换为一个向量*/
    val assembler = new VectorAssembler().setInputCols(assemblerInputs).setOutputCol("features")
    stagesArray.append(assembler)

    val pipeline = new Pipeline()
    pipeline.setStages(stagesArray.toArray)
    /**fit() 根据需要计算特征统计信息*/
    val pipelineModel = pipeline.fit(dataDF)
    /**transform() 真实转换特征*/
    val trainingDF = pipelineModel.transform(dataDF)
    trainingDF.show(10)

    val adslotidArray = trainingDF.select("adslotid","adslotidclassVec").distinct()
      .rdd.map(x=>(x.getAs[String]("adslotid"),x.getAs[WrappedArray[Int]]("adslotidclassVec"))).collect()
      .toMap
    val ideaidArray = trainingDF.select("ideaid","ideaidclassVec").distinct()
      .rdd.map(x=>(x.getAs[Int]("ideaid"),x.getAs[WrappedArray[Int]]("ideaidclassVec"))).collect()
      .toMap
    val adclassArray = trainingDF.select("adclass","adclassclassVec").distinct()
      .rdd.map(x=>(x.getAs[String]("adclass"),x.getAs[WrappedArray[Int]]("adclassclassVec"))).collect()
      .toMap

    val testData = spark.sql("select *,rawcvr as raw_cvr from dl_cpc.wy_calibration_sample_2019_10_11")
        .rdd.map {
      r =>
        val label = r.getAs[Long]("iscvr").toInt
        val raw_ctr = r.getAs[Long]("raw_cvr").toDouble / 1e6d
        val adslotid = r.getAs[String]("adslotid")
        val adclass = r.getAs[String]("adclass")
        val ideaid = r.getAs[Int]("ideaid")
        val user_show_ad_num = r.getAs[Long]("user_show_ad_num")
        var adslotidclassVec = adslotidArray.get("9999999")
        if(adslotidArray.contains(adslotid)){
          adslotidclassVec = adslotidArray.get(adslotid)
        }
        var ideaidclassVec = ideaidArray.get(9999999)
        if(ideaidArray.contains(ideaid)){
          ideaidclassVec = ideaidArray.get(ideaid)
        }
        var adclassclassVec = adclassArray.get("9999999")
        if(adclassArray.contains(adclass)){
          adclassclassVec = adclassArray.get(adclass)
        }
        (label, raw_ctr, user_show_ad_num, adslotidclassVec, ideaidclassVec,adclassclassVec, ideaid)
    }.toDF("label","raw_ctr","user_show_ad_num","adslotidclassVec", "ideaidclassVec","adclassclassVec","ideaid")


    val testDF: DataFrame = assembler.transform(testData)
//    test

    println(s"trainingDF size=${trainingDF.count()},testDF size=${testDF.count()}")
    val lrModel = new LinearRegression().setFeaturesCol("features")
//        .setWeightCol("hourweight")
        .setLabelCol("label").setRegParam(1e-7).setElasticNetParam(0.1).fit(trainingDF)
    val predictions = lrModel.transform(testDF).select("label", "features", "prediction","unitid")
      predictions.show(5)

    println("coefficients:" +lrModel.coefficients)
    println("intercept:" +lrModel.intercept)

    // 输出逻辑回归的系数和截距
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    //获取训练模型的相关信息
    val trainingSummary = lrModel.summary
    //模型残差
    trainingSummary.residuals.show()
    //模型均方差
    println("mse:" + trainingSummary.meanSquaredError)
    //模型均方根误差
    println("r-squared:" + trainingSummary.rootMeanSquaredError)

    val result = lrModel.transform(testDF).rdd.map{
      x =>
        val exp_cvr = x.getAs[Double]("prediction")
        val raw_cvr = x.getAs[Double]("raw_cvr").toDouble
        val unitid = x.getAs[String]("unitid")
        val iscvr = x(0).toString.toInt
        (exp_cvr,iscvr,raw_cvr,unitid)
    }.toDF("exp_cvr","isclick","raw_cvr","coin_origin")


        //   lr calibration
    calculateAuc(result,"lr",spark)
    //    raw data
    val modelData = result.selectExpr("cast(iscvr as Int) label","cast(raw_cvr as Int) prediction","unitid")
    calculateAuc(modelData,"original",spark)

//    online calibration
    val calibData = result.selectExpr("cast(iscvr as Int) label","cast(exp_cvr as Int) prediction","unitid")
    calculateAuc(calibData,"online",spark)

  }

 def SparseFeature(profile_num: Int)
  = udf {
    els: Seq[Row] =>
      val new_els: Seq[(Int, Double)] = els.map(x => {
        (x.getInt(0), x.getDouble(1))
      })
      Vectors.sparse(profile_num, new_els)
  }
}
