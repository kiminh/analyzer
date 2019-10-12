package com.cpc.spark.ml.calibration.exp

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import com.cpc.spark.common.Utils
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
    val data= spark.sql("select * from dl_cpc.wy_calibration_sample_2019-10-10")

    val dataDF = data.groupBy("ideaid").count()
      .withColumn("tag",when(col("count")>60,1).otherwise(0))
      .join(data,Seq("ideaid"),"left")
      .withColumn("label",col("iscvr"))
      .withColumn("ideaid",when(col("tag")===1,col("ideaid")).otherwise(9999999))
      .select("searchid","ideaid","user_show_ad_num","adclass","adslotid","label","unitid","raw_cvr","exp_cvr")

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

//    val wgold = trainingDF.rdd.map{
//      x =>
//        val key = x.getAs[Int]("coin_origin").toString
//        val coin_index=x.getAs[WrappedArray[Double]]("coin").toArray.toList.indexOf(1.0f)
//        val Wgold = lrModel.coefficients.toArray(9+coin_index)
//        (key,Wgold*1e6*1e6)
//    }.distinct().toDF("key","value")
//    wgold.show


    val testdata = spark.sql("select * from dl_cpc.wy_calibration_sample_2019-10-11")
    val testDF = pipelineModel.transform(testdata)

    //test

    println(s"trainingDF size=${trainingDF.count()},testDF size=${testDF.count()}")
    val lrModel = new LinearRegression().setFeaturesCol("features")
//        .setWeightCol("hourweight")
        .setLabelCol("label").setRegParam(1e-7).setElasticNetParam(0.1).fit(trainingDF)
    val predictions = lrModel.transform(testDF).select("label", "features", "prediction","unitid")
      predictions.show(5)

    println("coefficients:" +lrModel.coefficients)
    println("intercept:" +lrModel.intercept)

    // 输出逻辑回归的系数和截距
    //    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    //获取训练模型的相关信息
    val trainingSummary = lrModel.summary
    //模型残差
    trainingSummary.residuals.show()
    //模型均方差
    println("mse:" + trainingSummary.meanSquaredError)
    //模型均方根误差
    println("r-squared:" + trainingSummary.rootMeanSquaredError)

    lrModel.transform(testDF).rdd.map{
      x =>
        val exp_ctr = x.getAs[Double]("prediction")
        val raw_ctr = x(2).toString.toDouble
        val coin_origin = x(3).toString.toInt
        val isclick = x(0).toString.toInt
        (exp_ctr,isclick,raw_ctr,coin_origin)
    }.toDF("exp_ctr","isclick","raw_ctr","coin_origin").createOrReplaceTempView("result")





    //    //   lr calibration
//    calculateAuc(result2,"lr",spark)
//    //    raw data
//    val modelData = testsample.selectExpr("cast(isclick as Int) label","cast(raw_ctr as Int) prediction","ideaid")
//    calculateAuc(modelData,"original",spark)
//
////    online calibration
//    val calibData = testsample.selectExpr("cast(isclick as Int) label","cast(exp_ctr as Int) prediction","ideaid")
//    calculateAuc(calibData,"online",spark)

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
