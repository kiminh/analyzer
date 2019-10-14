package com.cpc.spark.ml.calibration.exp

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import com.cpc.spark.common.Utils

import scala.collection.mutable.{ListBuffer, WrappedArray}
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
import com.cpc.spark.tools.CalcMetrics
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

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
    val spark = SparkSession.builder()
      .appName("[trident] extract as event")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

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
    val sql1 =
    s"""
       |select *,rawcvr as raw_cvr,case
       |    when user_show_ad_num = 0 then '0'
       |    when user_show_ad_num = 1 then '1'
       |    when user_show_ad_num = 2 then '2'
       |    when user_show_ad_num in (3,4) then '4'
       |    when user_show_ad_num in (5,6,7) then '7'
       |    else '8' end as show_num
       |    from dl_cpc.wy_calibration_sample_2019_10_10
       """.stripMargin

    val sql2 =
      s"""
         |select *,rawcvr as raw_cvr,case
         |    when user_show_ad_num = 0 then '0'
         |    when user_show_ad_num = 1 then '1'
         |    when user_show_ad_num = 2 then '2'
         |    when user_show_ad_num in (3,4) then '4'
         |    when user_show_ad_num in (5,6,7) then '7'
         |    else '8' end as show_num
         |    from dl_cpc.wy_calibration_sample_2019_10_11
       """.stripMargin
    val data = spark.sql(sql1)
    data.show(10)

    val defaultideaid = data.groupBy("ideaid").count()
      .withColumn("ideaidtag",when(col("count")>20,1).otherwise(0))
      .filter("ideaidtag=1")
    val defaultunitid = data.groupBy("unitid").count()
      .withColumn("unitidtag",when(col("count")>20,1).otherwise(0))
      .filter("unitidtag=1")
    val defaultuserid = data.groupBy("userid").count()
      .withColumn("useridtag",when(col("count")>20,1).otherwise(0))
      .filter("useridtag=1")

    val df1 = data
      .join(defaultideaid,Seq("ideaid"),"left")
      .join(defaultunitid,Seq("unitid"),"left")
      .join(defaultuserid,Seq("userid"),"left")
      .withColumn("label",col("iscvr"))
      .withColumn("ideaid",when(col("ideaidtag")===1,col("ideaid")).otherwise(9999999))
      .withColumn("unitid0",when(col("unitidtag")===1,col("unitid")).otherwise(9999999))
      .withColumn("userid",when(col("useridtag")===1,col("userid")).otherwise(9999999))
      .withColumn("sample",lit(1))
      .select("searchid","ideaid","user_show_ad_num","adclass","adslotid","label","unitid","raw_cvr",
        "exp_cvr","sample","hourweight","userid","conversion_from","click_unit_count","show_num","hour","unitid0")
    df1.show(10)

    val df2 = spark.sql(sql2)
      .withColumn("label",col("iscvr"))
      .join(defaultideaid,Seq("ideaid"),"left")
      .join(defaultunitid,Seq("unitid"),"left")
      .join(defaultuserid,Seq("userid"),"left")
      .withColumn("sample",lit(0))
      .withColumn("ideaid",when(col("ideaidtag")===1,col("ideaid")).otherwise(9999999))
      .withColumn("unitid0",when(col("unitidtag")===1,col("unitid")).otherwise(9999999))
      .withColumn("userid",when(col("useridtag")===1,col("userid")).otherwise(9999999))
      .select("searchid","ideaid","user_show_ad_num","adclass","adslotid","label","unitid","raw_cvr",
        "exp_cvr","sample","hourweight","userid","conversion_from","click_unit_count","show_num","hour","unitid0")

    val dataDF = df1.union(df2)

    val categoricalColumns = Array("ideaid","adclass","adslotid","unitid0","userid","conversion_from","click_unit_count")

    val stagesArray = new ListBuffer[PipelineStage]()
    for (cate <- categoricalColumns) {
      val indexer = new StringIndexer().setInputCol(cate).setOutputCol(s"${cate}Index")
      val encoder = new OneHotEncoder().setInputCol(indexer.getOutputCol).setOutputCol(s"${cate}classVec")
      stagesArray.append(indexer,encoder)
    }

    val numericCols = Array("raw_cvr")
    val assemblerInputs = categoricalColumns.map(_ + "classVec") ++ numericCols
    /**使用VectorAssembler将所有特征转换为一个向量*/
    val assembler = new VectorAssembler().setInputCols(assemblerInputs).setOutputCol("features")
    stagesArray.append(assembler)

    val pipeline = new Pipeline()
    pipeline.setStages(stagesArray.toArray)
    /**fit() 根据需要计算特征统计信息*/
    val pipelineModel = pipeline.fit(dataDF)
    /**transform() 真实转换特征*/
    val dataset = pipelineModel.transform(dataDF)
    dataset.show(10)
    dataset.select("label","features").show(10)
    dataset.printSchema()

//    val adslotidArray = dataset.select("adslotid","adslotidclassVec").distinct()
//      .rdd.map(x=>(x.getAs[String]("adslotid"),x.getAs[org.apache.spark.ml.linalg.SparseVector]("adslotidclassVec").toDense)).collect()
//      .toMap
//    val ideaidArray = trainingDF.select("ideaid","ideaidclassVec").distinct()
//      .rdd.map(x=>(x.getAs[Int]("ideaid"),x.getAs[org.apache.spark.ml.linalg.SparseVector]("ideaidclassVec").toDense)).collect()
//      .toMap
//    val adclassArray = trainingDF.select("adclass","adclassclassVec").distinct()
//      .rdd.map(x=>(x.getAs[String]("adclass"),x.getAs[org.apache.spark.ml.linalg.SparseVector]("adclassclassVec").toDense)).collect()
//      .toMap
//
//    val testData = spark.sql("select *,rawcvr as raw_cvr from dl_cpc.wy_calibration_sample_2019_10_11")
//        .rdd.map {
//      r =>
//        val label = r.getAs[Long]("iscvr").toInt
//        val raw_cvr = r.getAs[Long]("raw_cvr").toDouble / 1e6d
//        val adslotid = r.getAs[String]("adslotid")
//        val adclass = r.getAs[String]("adclass")
//        val ideaid = r.getAs[Int]("ideaid")
//        val user_show_ad_num = r.getAs[Long]("user_show_ad_num")
//        var adslotidclassVec = adslotidArray.get("9999999").toVector
//        if(adslotidArray.contains(adslotid)){
//          adslotidclassVec = adslotidArray.get(adslotid).toVector
//        }
//        var ideaidclassVec = ideaidArray.get(9999999).toVector
//        if(ideaidArray.contains(ideaid)){
//          ideaidclassVec = ideaidArray.get(ideaid).toVector
//        }
//        var adclassclassVec = adclassArray.get("9999999").toVector
//        if(adclassArray.contains(adclass)){
//          adclassclassVec = adclassArray.get(adclass).toVector
//        }
//        (label, raw_cvr, user_show_ad_num, adslotidclassVec, ideaidclassVec,adclassclassVec, ideaid)
//    }.toDF("label","raw_cvr","user_show_ad_num","adslotidclassVec", "ideaidclassVec","adclassclassVec","ideaid")
//    testData.printSchema()
//
//
//    val testDF: DataFrame = assembler.transform(testData)
//    test

//    val Array(trainingDF, testDF) = dataset.filter("sample = 1").randomSplit(Array(0.8, 0.2), seed = 12345)
    val trainingDF= dataset.filter("sample=1")
    val validationDF = dataset.filter("sample = 0")
    println(s"trainingDF size=${trainingDF.count()},validationDF size=${validationDF.count()}")
    val lrModel = new LinearRegression().setFeaturesCol("features")
//        .setWeightCol("hourweight")
        .setLabelCol("label").setRegParam(1e-7).setElasticNetParam(0.1).fit(trainingDF)
    val predictions = lrModel.transform(trainingDF).select("label", "features", "prediction","unitid")
      predictions.show(5)

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

    val result1 = lrModel.transform(trainingDF).rdd.map{
      x =>
        val exp_cvr = x.getAs[Double]("prediction")*1e6d
        val raw_cvr = x.getAs[Long]("raw_cvr").toDouble
        val unitid = x.getAs[Int]("unitid")
        val iscvr = x.getAs[Int]("label")
        (exp_cvr,iscvr,raw_cvr,unitid)
    }.toDF("exp_cvr","iscvr","raw_cvr","unitid")

    val result2 = lrModel.transform(validationDF).rdd.map{
      x =>
        val exp_cvr = x.getAs[Double]("prediction")*1e6d
        val raw_cvr = x.getAs[Long]("raw_cvr").toDouble
        val unitid = x.getAs[Int]("unitid")
        val iscvr = x.getAs[Int]("label")
        (exp_cvr,iscvr,raw_cvr,unitid)
    }.toDF("exp_cvr","iscvr","raw_cvr","unitid")


        //   lr calibration
//    val lrData1 = result1.selectExpr("cast(iscvr as Int) label","cast(raw_cvr as Int) prediction","unitid")
//    calculateAuc(lrData1,"train original",spark)

    val lrData2 = result1.selectExpr("cast(iscvr as Int) label","cast(exp_cvr as Int) prediction","unitid")
    calculateAuc(lrData2,"train calibration",spark)
    //    raw data
//    val modelData = result2.selectExpr("cast(iscvr as Int) label","cast(raw_cvr as Int) prediction","unitid")
//    calculateAuc(modelData,"test original",spark)

//    online calibration
    val calibData = result2.selectExpr("cast(iscvr as Int) label","cast(exp_cvr as Int) prediction","unitid")
    calculateAuc(calibData,"test calibration",spark)

  }

  def calculateAuc(data:DataFrame,cate:String,spark: SparkSession): Unit ={
    val testData = data.selectExpr("cast(label as Int) label","cast(prediction as Int) score")
    val auc = CalcMetrics.getAuc(spark,testData)
    println("###      %s auc:%.4f".format(cate,auc))
    val p1= data.groupBy().agg(avg(col("label")).alias("cvr"),avg(col("prediction")/1e6d).alias("ecvr"))
    val cvr = p1.first().getAs[Double]("cvr")
    val ecvr = p1.first().getAs[Double]("ecvr")
    println("%s: cvr:%.4f,ecvr:%.4f,ecvr/cvr:%.3f".format(cate, cvr, ecvr, ecvr/cvr))

    testData.createOrReplaceTempView("data")
    val abs_error_sql =
      s"""
         |select
         |sum(if(iscvr>0,
         |if(sum_exp_cvr/iscvr/1000000>1,sum_exp_cvr/iscvr/1000000,iscvr*1000000/sum_exp_cvr),1)*imp)/sum(imp) abs_error
         |from
         |(
         |    select round(score/1000,0) as label,sum(score) sum_exp_cvr,sum(label) iscvr,count(*) as imp
         |    from data
         |    group by round(score/1000,0)
         |    )
       """.stripMargin
    val abs_error = spark.sql(abs_error_sql).first().getAs[Double]("abs_error")
    println("abs_error is %.3f".format(abs_error))

    val p2 = data.groupBy("unitid")
      .agg(
        avg(col("label")).alias("cvr"),
        avg(col("prediction")/1e6d).alias("ecvr"),
        count(col("label")).cast(DoubleType).alias("cvrnum")
      )
      .withColumn("pcoc",col("ecvr")/col("cvr"))
      .filter("cvrnum > 10")

    p2.createOrReplaceTempView("unit")
    val sql =
      s"""
         |select unitid,cvr,ecvr,cvrnum,pcoc,ROW_NUMBER() OVER (ORDER BY cvrnum DESC) rank
         |from unit
       """.stripMargin
    val p3 = spark.sql(sql).filter(s"rank<${p2.count()*0.8}")
//    p3.show(10)
    val cvr2 = p2.groupBy().agg(avg(col("cvr")).alias("cvr2")).first().getAs[Double]("cvr2")
    val ecvr2 = p2.groupBy().agg(avg(col("ecvr")).alias("ecvr2")).first().getAs[Double]("ecvr2")
    val pcoc = p2.groupBy().agg(avg(col("pcoc")).alias("avgpcoc")).first().getAs[Double]("avgpcoc")
    val allnum = p3.count().toDouble
    val rightnum = p3.filter("pcoc<1.1 and pcoc>0.9").count()
    val greaternum = p3.filter("pcoc>1.1").count()
    println("%s by unitid:unitid sum:%d,avgcvr:%.4f,avgecvr:%.4f,avgpcoc:%.3f,all:%.0f,right:%d,pcoc>1.1:%d,ratio of pcoc in (0.9,1,1):%.3f,ratio of pcoc>1.1:%.3f".format(cate, p2.count(),cvr2, ecvr2, pcoc,allnum,rightnum,greaternum,rightnum/allnum,greaternum/allnum))
  }
}
