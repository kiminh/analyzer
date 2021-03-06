package com.cpc.spark.ml.calibration.exp

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.common.Utils
import com.cpc.spark.tools.CalcMetrics
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{avg, col, count, udf}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

object LrCalibrationOnQtt {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.KryoSerializer.buffer.max", "2047MB")
      .appName("prepare lookalike sample".format())
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    // parse and process input
    val endDate = args(0)
    val endHour = args(1)
    val hourRange = args(2).toInt
    val model = "qtt-list-dnn-rawid-v4"
    val calimodel ="qtt-list-dnn-rawid-v4-postcali"


    val endTime = LocalDateTime.parse(s"$endDate-$endHour", DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
    val startTime = endTime.minusHours(Math.max(hourRange - 1, 0))

    val startDate = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val startHour = startTime.format(DateTimeFormatter.ofPattern("HH"))

    println(s"endDate=$endDate")
    println(s"endHour=$endHour")
    println(s"hourRange=$hourRange")
    println(s"startDate=$startDate")
    println(s"startHour=$startHour")

    // build spark session
    val session = Utils.buildSparkSession("hourlyCalibration")

    val timeRangeSql = Utils.getTimeRangeSql_3(startDate, startHour, endDate, endHour)

    // get union log
    val sql = s"""
                 |select isclick, raw_ctr, adslotid, ideaid,user_req_ad_num, hour
                 | from dl_cpc.slim_union_log
                 | where $timeRangeSql
                 | and media_appsid in ('80000001', '80000002') and isshow = 1 and adslot_type = 1
                 | and ctr_model_name in ('$model','$calimodel')
                 | and ideaid > 0 and adsrc = 1 AND userid > 0
                 | AND (charge_type IS NULL OR charge_type = 1)
       """.stripMargin
    println(s"sql:\n$sql")
    val log= session.sql(sql)

    val adslotidArray = log.select("adslotid").distinct().collect()
    val ideaidArray = log.select("ideaid").distinct().collect()
    val hourArray = log.select("hour").distinct().collect()

    val adslotidID = mutable.Map[String,Int]()
    var idxTemp = 0
    val adslotid_feature = adslotidArray.map{r => adslotidID.update(r.getAs[String]("adslotid"), idxTemp); idxTemp += 1; (("adslotid"+ r.getAs[String]("adslotid")), idxTemp -1)}

    val ideaidID = mutable.Map[Long,Int]()
    var idxTemp1 = 0
    val ideaid_feature = ideaidArray.map{r => ideaidID.update(r.getAs[Long]("ideaid"), idxTemp1); idxTemp1 += 1; (( "ideaid"+ r.getAs[Long]("ideaid")), idxTemp1 -1)}

    val hourID = mutable.Map[String,Int]()
    var idxTemp2 = 0
    val hourid_feature = hourArray.map{r => hourID.update(r.getAs[String]("hour"), idxTemp2); idxTemp2 += 1; (("hour" + r.getAs[String]("hour")), idxTemp2 -1)}

    val feature_profile = adslotid_feature ++ ideaid_feature

    val adslotid_sum = adslotidID.size
    val ideaid_sum = ideaidID.size
    val hour_sum = hourID.size
    val profile_num = adslotid_sum + ideaid_sum + hour_sum +2
    val sample = log.rdd.map {
      r =>
        val label = r.getAs[Long]("isclick").toInt
        val raw_ctr = r.getAs[Long]("raw_ctr").toDouble / 1e6d
        val adslotid = r.getAs[String]("adslotid")
        val ideaid = r.getAs[Long]("ideaid")
        val user_req_ad_num = r.getAs[Long]("user_req_ad_num").toDouble
        val hour = r.getAs[String]("hour")
        var els = Seq[(Int, Double)]()
        if (adslotid != null) {
          els = els :+ (adslotidID(adslotid), 1.0)
        }
        if (ideaid != null) {
          els = els :+ (ideaidID(ideaid) + adslotid_sum , 1.0)
        }
        if (hour != null) {
          els = els :+ (adslotid_sum + ideaid_sum + hourID(hour), 1.0)
        }
        if (raw_ctr != null) {
          els = els :+ (adslotid_sum + ideaid_sum + hour_sum , raw_ctr)
        }
        if (user_req_ad_num != null) {
          els = els :+ (adslotid_sum + ideaid_sum + hour_sum+1 , user_req_ad_num)
        }
        (label,els,ideaid)
    }.filter(_ != null).toDF("label","els","ideaid")
      .select($"label", SparseFeature(profile_num)($"els").alias("features"),$"ideaid")
    sample.show(5)

    val sql2 = s"""
                 |select isclick, raw_ctr, adslotid, ideaid,user_req_ad_num,exp_ctr,hour
                 | from dl_cpc.slim_union_log
                 | where dt = '2019-05-19'  and hour = '22'
                 | and media_appsid in ('80000001', '80000002') and isshow = 1 and adslot_type = 1
                 | and ctr_model_name in ('$calimodel')
                 | and ideaid > 0 and adsrc = 1 AND userid > 0
                 | AND (charge_type IS NULL OR charge_type = 1)
       """.stripMargin
    println(s"sql:\n$sql2")
    val testsample = session.sql(sql2)
    val test= testsample.rdd.map {
      r =>
        val label = r.getAs[Long]("isclick").toInt
        val raw_ctr = r.getAs[Long]("raw_ctr").toDouble / 1e6d
        val adslotid = r.getAs[String]("adslotid")
        val ideaid = r.getAs[Long]("ideaid")
        val user_req_ad_num = r.getAs[Long]("user_req_ad_num").toDouble
        val hour = r.getAs[String]("hour")
        var els = Seq[(Int, Double)]()
        if (adslotid != null) {
          if (adslotidID.contains(adslotid)){
            els = els :+ (adslotidID(adslotid), 1.0)
          }
        }
        if (ideaid != null) {
          if (ideaidID.contains(ideaid)){
            els = els :+ (ideaidID(ideaid) + adslotid_sum , 1.0)
          }
        }
        if (hour != null) {
          els = els :+ (adslotid_sum + ideaid_sum + hourID(hour), 1.0)
        }
        if (raw_ctr != null) {
          els = els :+ (adslotid_sum + ideaid_sum + hour_sum , raw_ctr)
        }
        if (user_req_ad_num != null) {
          els = els :+ (adslotid_sum + ideaid_sum + hour_sum+1 , user_req_ad_num)
        }
        (label,els,ideaid)
    }.filter(_ != null).toDF("label","els","ideaid")
      .select($"label", SparseFeature(profile_num)($"els").alias("features"),$"ideaid")
    test.show(5)

      val Array(trainingDF, testDF) = sample.randomSplit(Array(0.8, 0.2), seed = 1)
      println(s"trainingDF size=${trainingDF.count()},testDF size=${testDF.count()}")
      val lrModel = new LogisticRegression().
        setLabelCol("label").
        setFeaturesCol("features").
        setMaxIter(200).
        setThreshold(0.5).
        setRegParam(0.15).
        fit(trainingDF)
      val predictions = lrModel.transform(testDF).select("label", "features","rawPrediction", "probability", "prediction","ideaid")
      predictions.show(5)

        //使用BinaryClassificationEvaluator来评价我们的模型
        val evaluator = new BinaryClassificationEvaluator()
        evaluator.setMetricName("areaUnderROC")
        val auc = evaluator.evaluate(predictions)
      println("model auc:%f".format(auc))
    val newprediction = lrModel.transform(test).select("label","probability","ideaid")

    //取出预测为1的probability
    val result2 = newprediction.map(line => {
      val label = line.get(line.fieldIndex("label")).toString.toInt
      val dense = line.get(line.fieldIndex("probability")).asInstanceOf[org.apache.spark.ml.linalg.DenseVector]
      val y = dense(1).toString.toDouble * 1e6d.toInt
      val ideaid = line.get(line.fieldIndex("ideaid")).toString
      (label,y,ideaid)
    }).toDF("label","prediction","ideaid")
    //   lr calibration
    calculateAuc(result2,"lr",spark)
    //    raw data
    val modelData = testsample.selectExpr("cast(isclick as Int) label","cast(raw_ctr as Int) prediction","ideaid")
    calculateAuc(modelData,"original",spark)

//    online calibration
    val calibData = testsample.selectExpr("cast(isclick as Int) label","cast(exp_ctr as Int) prediction","ideaid")
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

  def calculateAuc(data:DataFrame,cate:String,spark: SparkSession): Unit ={
    val testData = data.selectExpr("cast(label as Int) label","cast(prediction as Int) score")
    val auc = CalcMetrics.getAuc(spark,testData)
    println("%s auc:%f".format(cate,auc))
    val p1= data.groupBy().agg(avg(col("label")).alias("ctr"),avg(col("prediction")/1e6d).alias("ectr"))
    val ctr = p1.first().getAs[Double]("ctr")
    val ectr = p1.first().getAs[Double]("ectr")
    println("%s calibration: ctr:%f,ectr:%f,ectr/ctr:%f".format(cate, ctr, ectr, ectr/ctr))

    testData.createOrReplaceTempView("data")
    val abs_error_sql =
      s"""
         |select
         |sum(if(click>0,
         |if(sum_exp_ctr/click/1000000>1,sum_exp_ctr/click/1000000,click*1000000/sum_exp_ctr),1)*imp)/sum(imp) abs_error
         |from
         |(
         |    select round(score/1000,0) as label,sum(score) sum_exp_ctr,sum(label) click,count(*) as imp
         |    from data
         |    group by round(score/1000,0)
         |    )
       """.stripMargin
    val abs_error = spark.sql(abs_error_sql).first().getAs[Double]("abs_error")
    println("abs_error is %f".format(abs_error))

    val p2 = data.groupBy("ideaid")
      .agg(
        avg(col("label")).alias("ctr"),
        avg(col("prediction")/1e6d).alias("ectr"),
        count(col("label")).cast(DoubleType).alias("ctrnum")
      )
      .withColumn("pcoc",col("ectr")/col("ctr"))
    println("ideaid sum:%d".format(p2.count()))
    p2.createOrReplaceTempView("idea")
    val sql =
      s"""
         |select ideaid,ctr,ectr,ctrnum,pcoc,ROW_NUMBER() OVER (ORDER BY ctrnum DESC) rank
         |from idea
       """.stripMargin
    val p3 = spark.sql(sql).filter(s"rank<${p2.count()*0.8}")
    p3.show(10)
    val ctr2 = p2.groupBy().agg(avg(col("ctr")).alias("ctr2")).first().getAs[Double]("ctr2")
    val ectr2 = p2.groupBy().agg(avg(col("ectr")).alias("ectr2")).first().getAs[Double]("ectr2")
    val pcoc = p2.groupBy().agg(avg(col("pcoc")).alias("avgpcoc")).first().getAs[Double]("avgpcoc")
    val allnum = p3.count().toDouble
    val rightnum = p3.filter("pcoc<1.1 and pcoc>0.9").count().toDouble
    println("%s calibration by ideaid: avgctr:%f,avgectr:%f,avgpcoc:%f,all:%f,right:%f,ratio of 0.1 error:%f".format(cate, ctr2, ectr2, pcoc,allnum,rightnum,rightnum/allnum))
  }
}
