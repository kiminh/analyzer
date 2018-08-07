package com.cpc.spark.qukan.userprofile

import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.common.Utils
import ml.dmlc.xgboost4j.scala.spark.XGBoostEstimator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession


/**
  * 训练年龄分段 [0,22],(22,~）
  * 数据来源：支付宝年龄数据 + qtt阅读特征关键字 + app安装列表
  *
  * @author zhj
  * @version 1.0
  *          2018-07-30
  */

object TrainStudent_v1 {
  private val cur_year = new SimpleDateFormat("yyyy").format(new Date()).toInt

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("getAge", getAge _)

    import spark.implicits._

    val version = "v1"
    val date = args(0)
    val startdate = args(1)

    //年龄
    spark.sql(
      """
        |select member_id,getAge(info) as label
        |from gobblin.qukan_member_zfb_log
        |where update_time >= "2018-01-01"
        | and member_id > 0
      """.stripMargin)
      .createOrReplaceTempView("age_data")

    //安装列表
    spark.sql(
      s"""
         |select uid,pkgs
         |from(select uid,pkgs,
         |           row_number() over(partition by uid order by load_date desc)rn
         |     from dl_cpc.cpc_user_installed_apps
         |     where load_date>='$startdate') x
         |where x.rn=1
      """.stripMargin)
      .createOrReplaceTempView("apps")


    //训练和测试数据
    val join_data = spark.sql(
      s"""
         |select b.label ,
         |       a.features['u_dy_5_readkeyword'].stringarrayvalue as words,
         |       c.pkgs
         |from dl_cpc.cpc_user_features_from_algo a
         |join age_data b
         |  on a.member_id=b.member_id
         |  and b.label>=0
         |join apps c
         |  on a.device_id=c.uid
         |where a.load_date='$date'
         |  and a.features['u_dy_5_readkeyword'] is not null
      """.stripMargin)
    join_data.persist()

    var w_n = 0

    val word_ratio = join_data.flatMap { x =>
      for (word <- x.getAs[Seq[String]]("words")) yield (word, (1, x.getAs[Int]("label")))
    }.rdd
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .filter(_._2._1 > 10)
      .map(x => (x._1, x._2._2.toDouble / x._2._1))
      .collect()
      .map { x =>
        w_n = w_n + 1
        (x._1, (w_n, x._2))
      }.toMap

    val w_ratio = spark.sparkContext.broadcast(word_ratio)

    var a_n = 0
    val app_ratio = join_data.flatMap { x =>
      for (word <- x.getAs[Seq[String]]("pkgs")) yield (word, (1, x.getAs[Int]("label")))
    }.rdd
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .filter(_._2._1 > 5000)
      .map(x => (x._1, x._2._2.toDouble / x._2._1))
      .collect()
      .map { x =>
        a_n = a_n + 1
        (x._1, (a_n, x._2))
      }
      .toMap
    val p_ratio = spark.sparkContext.broadcast(app_ratio)

    //训练和测试数据特征
    val alldata = join_data.map {
      x =>
        val label = x.getAs[Int]("label").toDouble
        val words = x.getAs[Seq[String]]("words")
        val w_els = getFeatures(words, w_ratio.value, w_n + 1)
        val pkgs = x.getAs[Seq[String]]("pkgs")
        val p_els = getFeatures(pkgs, p_ratio.value, a_n + 1)
        (label, w_els, p_els)
    }.toDF("label", "word_vec", "pkg_vec")

    //预测数据
    val pdata = spark.sql(
      s"""
         |select a.device_id as uid,
         |       a.features['u_dy_5_readkeyword'].stringarrayvalue as words,
         |       b.pkgs
         |from dl_cpc.cpc_user_features_from_algo a
         |join dl_cpc.cpc_user_installed_apps b
         |   on a.device_id=b.uid
         |   and b.load_date='$date'
         |left join age_data c
         |   on a.member_id = c.member_id
         |where a.load_date='$date'
         |   and a.features['u_dy_5_readkeyword'] is not null
         |   and c.member_id is null
      """.stripMargin)
      .map { x =>
        (x.getAs[String]("uid"),
          getFeatures(x.getAs[Seq[String]]("words"), w_ratio.value, w_n + 1),
          getFeatures(x.getAs[Seq[String]]("pkgs"), w_ratio.value, a_n + 1))
      }.toDF("uid", "word_vec", "pkg_vec")

    val ass = new VectorAssembler()
      .setInputCols(Array("word_vec", "pkg_vec"))
      .setOutputCol("features")

    //保存预测数据到hdfs
    ass.transform(pdata).select("uid", "features")
      .coalesce(200)
      .write.mode("overwrite")
      .parquet(s"/home/cpc/zhj/crowd_bag/age/$version/data/predictdata")

    //保存训练和测试数据
    val Array(traindata, testdata) = ass.transform(alldata).select("label", "features").randomSplit(Array(0.8, 0.2), 1030L)
    traindata.coalesce(50)
      .write.mode("overwrite")
      .parquet(s"/home/cpc/zhj/crowd_bag/age/$version/data/traindata")
    testdata.coalesce(50)
      .write.mode("overwrite")
      .parquet(s"/home/cpc/zhj/crowd_bag/age/$version/data/testdata")

    traindata.coalesce(1).write.format("libsvm").mode("overwrite").save(s"/home/cpc/zhj/crowd_bag/age/$version/data/traindata_python")
    testdata.coalesce(1).write.format("libsvm").mode("overwrite").save(s"/home/cpc/zhj/crowd_bag/age/$version/data/testdata_python")
    /* //lr模型
     val lrmodel = new LogisticRegression().setFeaturesCol("features").setLabelCol("label").fit(traindata)
     val res = lrmodel.transform(testdata)

     val metrics = new BinaryClassificationEvaluator().setRawPredictionCol("rawPrediction").setLabelCol("label")
     println("areaUnderPR : " + metrics.setMetricName("areaUnderPR").evaluate(res))
     println("areaUnderROC : " + metrics.setMetricName("areaUnderROC").evaluate(res))*/

    val params = Map(
      "objective" -> "reg:logistic",
      "num_round" -> 300,
      "num_class" -> 2,
      "max_depth" -> 6
    )
    val xgb = new XGBoostEstimator(params)
    val model = xgb.train(traindata)
    model.write.overwrite().save(s"/home/cpc/zhj/crowd_bag/age/$version/model/xgboost")

    val predictions = model.transform(testdata)
      .map { x => (x.getAs[Float]("prediction").toDouble, x.getAs[Double]("label")) }

  }

  //获取特征hash后的向量
  def getFeatures(raw_vec: Seq[String], ratio_map: Map[String, (Int, Double)] = null, n: Int): Vector = {
    var els = Seq[(Int, Double)]()
    for (i <- raw_vec) {
      if (ratio_map.contains(i))
        els = els :+ ratio_map.getOrElse(i, (0, 0D))
    }
    Vectors.sparse(n, els)
  }


  //获取支付宝年龄信息
  def getAge(info: String): Int = {
    try {
      val reg = ".*\"person_birthday\":\"([0-9]{8})\",.*".r
      val reg(birth) = info
      val age = cur_year - birth.substring(0, 4).toInt
      if (age <= 22) 1 else 0
    } catch {
      case _: Exception => -1
    }
  }

  /*def main(args: Array[String]): Unit = {
    val str = "{\"alipay_user_info_share_response\":{\"code\":\"10000\",\"msg\":\"Success\",\"city\":\"\\u6885\\u5dde\\u5e02\",\"gender\":\"m\",\"is_certified\":\"T\",\"is_student_certified\":\"F\",\"person_birthday\":\"19740626\",\"province\":\"\\u5e7f\\u4e1c\\u7701\",\"user_id\":\"2088022356459468\",\"user_status\":\"T\",\"user_type\":\"2\"},\"sign\":\"h\\/\\/z+idUKPiRh3yZjnLzMNdYdLcN3+c\\/Q+Za1AjFMcEq\\/l9LTzHzusO+I1ColEkh03HWREU2TkpZMsQ1\\/FvOB2PAVT27hEQ6e4hbeyxUaafJPlS22jPcXVp5fKn0tvatfrgzVFyo4e2gt8w2wXTai9SVB+Eq7RQvn+6wBZS7C7ICtIEEf2lwSpmh\\/\\/k8YMq\\/DyM4mQe7Ec7fY5OmWyzsiTEv\\/9flybAwKynOfS9xCcRh4ZkD1KFroWpmSO29wicW0qs69Eom4be26mHTXhaPaYXO1ZP+J11gGclTFwF492rjF\\/5sxOYhUqaAvOsmG2fNN5FxYeO6zaiiWBa9UCyyMQ==\"}"
    println(getAge(str))
  }*/
}
