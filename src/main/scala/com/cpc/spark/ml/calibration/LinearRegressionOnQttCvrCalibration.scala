package com.cpc.spark.ml.calibration

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.ml.calibration.HourlyCalibration.localDir
import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.tools.CalcMetrics
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel.{CalibrationFeature, CalibrationModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object LinearRegressionOnQttCvrCalibration {
  def main(args: Array[String]): Unit = {
    // new calibration
    val endDate = args(0)
    val endHour = args(1)
    val hourRange = args(2).toInt
    val media = args(3)
    val model = args(4)
    val calimodel = args(5)
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

    // build spark session
    val spark = SparkSession.builder()
      .appName(s"cvr calibration  $endDate - $endHour")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

    val selectCondition1 = getTimeRangeSql(startDate, startHour, endDate, endHour)
    val selectCondition2 = getTimeRangeSql4(startDate, startHour, endDate, endHour)
    import spark.implicits._

    // get union log
    val sql = s"""
                      |select a.searchid, cast(raw_cvr/10000 as double) as raw_cvr, substring(adclass,1,6) as adclass,
                      |cvr_model_name, adslot_id, a.ideaid,exp_cvr,unitid,userid,click_unit_count,conversion_from, hour,
                      |if(c.iscvr is not null,1,0) iscvr,round(if(hour>$endHour,hour-$endHour,hour+24-$endHour)/12 + 1) hourweight
                      |from
                      |  (select * from
                      |  dl_cpc.cvr_calibration_sample_all
                      |  where $selectCondition2
                      |  and $mediaSelection
                      |  and cvr_model_name in ('$calimodel','$model')
                      |  and is_ocpc = 1) a
                      | left join
                      | (select distinct searchid,conversion_goal,1 as iscvr
                      |  from dl_cpc.ocpc_quick_cv_log
                      |  where  $selectCondition1) c
                      |  on a.searchid = c.searchid and a.conversion_goal = c.conversion_goal
       """.stripMargin

    println(s"sql:\n$sql")
    val data = spark.sql(sql)

    val defaultideaid = data.groupBy("ideaid").count()
      .withColumn("ideaidtag",when(col("count")>40,1).otherwise(0))
      .filter("ideaidtag=1")
    val defaultunitid = data.groupBy("unitid").count()
      .withColumn("unitidtag",when(col("count")>40,1).otherwise(0))
      .filter("unitidtag=1")
    val defaultuserid = data.groupBy("userid").count()
      .withColumn("useridtag",when(col("count")>40,1).otherwise(0))
      .filter("useridtag=1")


    val dataDF = data
      .join(defaultideaid,Seq("ideaid"),"left")
//      .join(defaultunitid,Seq("unitid"),"left")
//      .join(defaultuserid,Seq("userid"),"left")
      .withColumn("label",col("iscvr"))
      .withColumn("ideaid",when(col("ideaidtag")===1,col("ideaid")).otherwise("default"))
//      .withColumn("unitid",when(col("unitidtag")===1,col("unitid")).otherwise("default"))
//      .withColumn("userid",when(col("useridtag")===1,col("userid")).otherwise("default"))
      .withColumn("sample",lit(1))
      .withColumn("click_unit_count",when(col("click_unit_count")<10
        ,col("click_unit_count")).otherwise("default"))
      .select("searchid","ideaid","adclass","adslot_id","label","unitid","raw_cvr",
        "exp_cvr","sample","hourweight","userid","conversion_from","click_unit_count","hour")
    dataDF.show(10)

    val categoricalColumns = Array("ideaid","adclass","adslot_id","unitid","userid","click_unit_count")
    val sampleidx = Map("ideaid" -> 11,"adclass" -> 16,"adslot_id" -> 5,"unitid" -> 12 ,"userid" -> 14,"conversion_from" -> 73,
      "click_unit_count" -> 35)

    val stagesArray = new ListBuffer[PipelineStage]()
    for (cate <- categoricalColumns) {
      val indexer = new StringIndexer().setInputCol(cate).setOutputCol(s"${cate}Index")
      val encoder = new OneHotEncoder().setInputCol(indexer.getOutputCol).setOutputCol(s"${cate}classVec").setDropLast(false)
      stagesArray.append(indexer,encoder)
    }

    val numericCols = Array("raw_cvr")
    val assemblerInputs = numericCols ++ categoricalColumns.map(_ + "classVec")
    /**使用VectorAssembler将所有特征转换为一个向量*/
    val assembler = new VectorAssembler().setInputCols(assemblerInputs).setOutputCol("features")
    stagesArray.append(assembler)

    val pipeline = new Pipeline()
    pipeline.setStages(stagesArray.toArray)
    /**fit() 根据需要计算特征统计信息*/
    val pipelineModel = pipeline.fit(dataDF)
    /**transform() 真实转换特征*/
    val dataset = pipelineModel.transform(dataDF)

    val trainingDF= dataset
    trainingDF.show(5)
    println(s"trainingDF size=${trainingDF.count()}")
    val lrModel = new LinearRegression().setFeaturesCol("features")
        .setWeightCol("hourweight")
        .setLabelCol("label").setRegParam(0.018).setElasticNetParam(0.01).fit(trainingDF)
//    val predictions = lrModel.transform(trainingDF).select("label", "features", "prediction","unitid")

    // 输出逻辑回归的系数和截距
    println(s"Coefficients: ${lrModel.coefficients}")
    println(s"Intercept: ${lrModel.intercept}")
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
        val searchid = x.getAs[String]("searchid")
        val exp_cvr = x.getAs[Double]("prediction")*1e6d
        val raw_cvr = x.getAs[Double]("raw_cvr")*1e4d
        val unitid = x.getAs[Int]("unitid")
        val iscvr = x.getAs[Int]("label")
        (searchid,exp_cvr,iscvr,raw_cvr,unitid)
    }.toDF("searchid","exp_cvr","iscvr","raw_cvr","unitid")


    var dimension = 0
    var defaultnum = 0
    var featuregroup = scala.collection.mutable.ArrayBuffer[CalibrationFeature]()
    var featuremap = scala.collection.mutable.Map[String,Double]()
    for (cate <- categoricalColumns ){
      var value_op = 0
      var classify_type = CalibrationFeature.ClassifyType.PrefixLen.apply(0)
      if (cate == "adclass"){
        value_op = 1
        classify_type = CalibrationFeature.ClassifyType.PrefixLen.apply(6)
      }

      val featureid = CalibrationFeature(
        asIdx = sampleidx.get(cate).getOrElse(-1),
        prefix = cate + "#",
        types = 1,
        valueOp = value_op,
        classifyType = classify_type
      )
      featuregroup += featureid

      val featurevalue = cate + "value"
      val featurevec = cate + "classVec"
      val f1= trainingDF.groupBy(s"$cate",s"$featurevec")
        .count()
        .selectExpr(s"cast($cate as string) $cate",s"$featurevec",s"count")
        .rdd.map { x =>
        {
          val cateid = x.getAs[String](cate)
          val featurevecid = x.getAs[org.apache.spark.ml.linalg.SparseVector](featurevec).toArray
          val featurecoe = lrModel.coefficients.toArray(dimension + 1 + featurevecid.indexOf(1.0f))
          val key = s"$cate" + "#" + cateid
          val count = x.getAs[Long]("count")
          (key, (featurecoe, count))
        }}.toLocalIterator.toMap[String,(Double,Long)]
          .map{
            x =>
              val key= x._1
              val featurecoe = x._2._1
              val count = x._2._2
              println(s"$key coefficient:$featurecoe")
              featuremap += ((key, featurecoe))
              (key,(featurecoe,count))
          }
      if (!featuremap.keySet.contains(s"$cate" + "#default")) {
        val key = s"$cate" + "#default"
        defaultnum += 1
        val featurecoe = f1.map(x => x._2._1 * x._2._2).sum /f1.map(_._2._2).sum
        println(s"$key coefficient:$featurecoe")
        featuremap += ((key, featurecoe))
      }
      dimension = featuremap.size - defaultnum
    }

    val w_rawvalue = lrModel.coefficients.toArray(0)*1e2d
    println(s"w_rawvalue :$w_rawvalue")

    val LRoutput = CalibrationModel(
      feature = featuregroup,
      featuremap = featuremap.toMap,
      wRawvalue = w_rawvalue,
      intercept = lrModel.intercept,
      min = 0.00001
    )

    val localPath = saveProtoToLocal(calimodel, LRoutput)
    saveFlatTextFileForDebug(calimodel, LRoutput)

    //   lr calibration
    val lrData1 = result1.selectExpr("cast(iscvr as Int) label","cast(raw_cvr as Int) prediction","unitid")
    calculateAuc(lrData1,"train original",spark)

    val lrData2 = result1.selectExpr("cast(iscvr as Int) label","cast(exp_cvr as Int) prediction","unitid")
    calculateAuc(lrData2,"train calibration",spark)

  }

  def output(coefficients:org.apache.spark.ml.linalg.DenseVector, dimension: Int)
  = udf { value: org.apache.spark.ml.linalg.DenseVector =>
    val a = dimension + value.toArray.indexOf(1.0f)
    println(a)
    coefficients.toArray(dimension + value.toArray.toList.indexOf(1.0f))
  }

  def saveProtoToLocal(modelName: String, config: CalibrationModel): String = {
    val filename = s"LR-calibration-$modelName.mlm"
    val localPath = localDir + filename
    val outFile = new File(localPath)
    outFile.getParentFile.mkdirs()
    config.writeTo(new FileOutputStream(localPath))
    return localPath
  }

  def saveFlatTextFileForDebug(modelName: String, config: CalibrationModel): Unit = {
    val filename = s"LR-calibration-flat-$modelName.txt"
    val localPath = localDir + filename
    val outFile = new File(localPath)
    outFile.getParentFile.mkdirs()
    new PrintWriter(localPath) { write(config.toString); close() }
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
        sum(col("label")).cast(DoubleType).alias("cvrnum")
      )
      .withColumn("pcoc",col("ecvr")/col("cvr"))
      .filter("cvrnum > 20")

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
