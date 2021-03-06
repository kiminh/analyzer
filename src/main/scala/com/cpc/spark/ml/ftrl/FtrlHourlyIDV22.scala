package com.cpc.spark.ml.ftrl

/**
  * author: huazhenhao
  * date: 9/18/18
  */

import com.cpc.spark.common.{Murmur3Hash, Utils}
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.cpc.spark.ml.train.Ftrl
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object FtrlHourlyIDV22 {

  val LOCAL_DIR = "/home/cpc/ftrl/"
  val HDFS_MODEL_DIR = "hdfs:///user/cpc/qtt-ftrl-model/"
  val HDFS_MODEL_HISTORY_DIR = "hdfs:///user/cpc/qtt-ftrl-model-history/"
  val DEST_DIR = "/home/work/mlcpp/model/"

  val DOWN_SAMPLE_RATE = 0.2

  // return (searchid, label, xgfeature, error)
  def mapFunc(line: String): (String, Double, String, Int) = {
    val array = line.split("\t")
    if (array.length < 3) {
      return ("", 0, "", 1)
    }
    val label = array(0).toDouble
    return (array(2).trim, label, array(1), 0)
  }

  def main(args: Array[String]): Unit = {

    val dt = args(0)
    val hour = args(1)
    val upload = args(2).toBoolean
    val startFresh = args(3).toBoolean
    val typename = args(4)
    val gbdtVersion = args(5).toInt
    val ftrlVersion = args(6).toInt
    val l1 = args(7).toDouble
    val profileDt = args(8).toString
    val typearray = typename.split("-")
    val adslot = typearray(0)
    val ctrcvr = typearray(1)

    println(s"dt=$dt")
    println(s"hour=$hour")
    println(s"typename=$typename (list-ctr, content-ctr, interact-ctr, all-cvr)")
    println(s"gbdtVersion=$gbdtVersion")
    println(s"ftrlVersion=$ftrlVersion")
    println(s"upload=$upload")
    println(s"forceNew=$startFresh")
    println(s"adslot=$adslot")
    println(s"ctrcvr=$ctrcvr")
    println(s"l1=$l1")
    println(s"profileDt=$profileDt")


    val inputName = s"/user/cpc/qtt-portrait-ftrl/sample_for_ftrl_with_id/ftrl-with-id-${dt}-${hour}-${typename}-${gbdtVersion}.svm"
    println(s"inputname = $inputName")

    val spark: SparkSession = Utils.buildSparkSession(name = "full_id_ftrl")

    val currentHDFS = s"${HDFS_MODEL_DIR}ftrl-$typename-$ftrlVersion.mlm"
    val ftrl = Ftrl.getModelFromProtoOnHDFS(startFresh, currentHDFS, spark)
    ftrl.L1 = l1
    println("before training model info:")
    printModelInfo(ftrl)

    import spark.implicits._

    // id, label, features
    val sample = spark.sparkContext
      .textFile(inputName, 50)
      .map(mapFunc)
      .toDF("searchid", "label", "xgBoostFeatures", "hasError")

    println(s"xgBoost total data size = ${sample.count()}")
    println(s"xgBoost filtered data size = ${sample.filter(x => x.getAs[Int]("hasError") > 0).count()}")
    println(s"xgBoost correct data size = ${sample.filter(x => x.getAs[Int]("hasError") == 0).count()}")

    val userProfile = spark.sql(
      s"""
         | select *
         | from dl_cpc.cpc_user_features_from_algo
         | where
       """.stripMargin
    )

    val log = spark.sql(
      s"""
        |select * from
        |(select *,
        | ext['adclass'].int_value as ad_class_int,
        | ext_int['exp_style'] as exp_style_int
        | from dl_cpc.cpc_union_log
        |where `date` = '$dt' and hour = '$hour'
        |and media_appsid  in ('80000001', '80000002') and isshow = 1 and ext['antispam'].int_value = 0
        |and ideaid > 0 and adsrc = 1 and adslot_type in (1) AND userid > 0) a
        |left join (
        | select
        |   member_id,
        |   features['u_dy_6_readcate'].stringarrayvalue as rec_user_cate,
        |   features['u_dy_6_favocate5'].stringarrayvalue as rec_user_fav
        | from dl_cpc.cpc_user_features_from_algo
        | where load_date='$profileDt'
        |) b on (a.ext_string['qtt_member_id'] = b.member_id)
      """.stripMargin)


    var merged = sample
      .filter(x => x.getAs[Int]("hasError") == 0)
      .join(log, Seq("searchid"), "inner")
    println(s"join with log size = ${merged.select("searchid").distinct().count()}")

    // join user app
    val userApps = spark.table("dl_cpc.cpc_user_installed_apps").filter(s"load_date='$dt'")
    merged = merged.join(userApps, Seq("uid"), joinType = "left")

    val allData = createFeatures(merged)

    val nameSpaceCount = allData.map(x => x._3).flatMap(x => x).map(x => (x, 1d)).reduceByKey(_ + _).collect()
    println("feature name space count:")
    nameSpaceCount.foreach(x => println(s"${x._1}:${x._2}"))

    val dataWithID = allData.map(x => (x._1, x._2))

    println("start model training")
    ftrl.trainWithDict(spark, dataWithID.collect())

    println("after training model info:")
    printModelInfo(ftrl)

    // save model file locally
    val name = s"$ctrcvr-protrait$ftrlVersion-ftrl-id-qtt-$adslot"
    val filename = s"$LOCAL_DIR$name.mlm"
    Ftrl.saveLrPbPackWithDict(ftrl, filename, "ctr-ftrl-v1", name)
    println(s"Save model locally to $filename")

    if (upload) {
      Ftrl.saveToProtoToHDFS(currentHDFS, spark, ftrl)
      val historyHDFS = s"${HDFS_MODEL_HISTORY_DIR}ftrl-$typename-$ftrlVersion-$dt-$hour.mlm"
      Ftrl.saveToProtoToHDFS(historyHDFS, spark, ftrl)
      println(MUtils.updateMlcppOnlineData(filename, s"$DEST_DIR$name.mlm", ConfigFactory.load()))
    }
  }

  def printModelInfo(ftrl: Ftrl): Unit = {
    println(s"Model dict size: ${ftrl.wDict.size}")
  }

  def createFeatures(df: DataFrame): RDD[(Array[Int], Double,  Seq[String])] = {
     df.rdd.map(x => {
      // prepare xgboost features
      val array = x.getAs[String]("xgBoostFeatures").split("\\s+")
      val xgBoostFeatures = array.map(x => {
        val vals = x.split(":")
        vals(0).toInt
      })
      // get label
      val label = x.getAs[Double]("label")
      // generate original string id
      val (allId, allNameSpace) = getAllIDFeatures(x)
      // get hashed id
      val hashedID = allId.map(a => getRawHashedID(a))
      // combine xgboost feature and hashed id
      ((xgBoostFeatures.toSet ++ hashedID.toSet).toArray, label, allNameSpace)
    })
  }

  // return: string formed id
  def getAllIDFeatures(row: Row): (Seq[String], Seq[String]) = {
    val idFeatures = new ListBuffer[String]()
    val namespace = new ListBuffer[String]()
    // advertiser id
    val advertiserID = row.getAs[Int]("userid")
    idFeatures.append("adv" + advertiserID.toString)
    if (advertiserID != 0) {
      namespace.append("adv")
    }
    // plan id
    val planID = row.getAs[Int]("planid")
    idFeatures.append("pl" + planID.toString)
    if (planID != 0) {
      namespace.append("pl")
    }
    // unit id
    val unitID = row.getAs[Int]("unitid")
    idFeatures.append("unt" + unitID.toString)
    if (unitID != 0) {
      namespace.append("unt")
    }
    // idea id
    val ideaID = row.getAs[Int]("ideaid")
    idFeatures.append("id" + ideaID.toString)
    if (ideaID != 0) {
      namespace.append("id")
    }
    // adclass
    val adclassID = row.getAs[Int]("ad_class_int")
    idFeatures.append("adc" + adclassID.toString)
    if (adclassID != 0) {
      namespace.append("adc")
    }
    // cityid
    val cityID = row.getAs[Int]("city")
    idFeatures.append("ct" + cityID.toString)
    if (cityID != 0) {
      namespace.append("ct")
    }
    // user interest
    val interestString = row.getAs[String]("interests")
    interestString.split(",").foreach(x => {
      val interestID = x.split("=")(0)
      idFeatures.append("i" + interestID.toString)
      idFeatures.append("i" + interestID.toString + "adv" + advertiserID.toString)
      idFeatures.append("i" + interestID.toString + "unt" + unitID.toString)
    })
    if (interestString.length > 0) {
      namespace.append("i")
      namespace.append("i_adv_")
    }
    // style
    val styleID = row.getAs[Long]("exp_style_int")

    if (styleID == 510127) {
      idFeatures.append("is_jinbi")
      namespace.append("is_jinbi")
    }
    // installed apps
    if (row.getAs[Object]("pkgs") != null) {
      val apps = row.getAs[mutable.WrappedArray[String]]("pkgs")
      apps.foreach(x => {
        idFeatures.append("ap" + x)
        // app cross advertiser id
        idFeatures.append("ap" + x + "adv" + advertiserID.toString )
      })
      if (apps.nonEmpty) {
        namespace.append("ap")
        namespace.append("ap_adv_")
      }
    }
    // rec_user_cate
    if (row.getAs[Object]("rec_user_cate") != null) {
      val categories = row.getAs[mutable.WrappedArray[String]]("rec_user_cate")
      categories.foreach(x => {
        idFeatures.append("ruc" + x)
        idFeatures.append("ruc" + x + "adv" + advertiserID.toString)
      })
      if (categories.nonEmpty) {
        namespace.append("ruc")
        namespace.append("ruc_adv_")
      }
    }
    // rec_user_source
    if (row.getAs[Object]("rec_user_fav") != null) {
      val categories = row.getAs[mutable.WrappedArray[String]]("rec_user_fav")
      categories.foreach(x => {
        idFeatures.append("ruf" + x)
        idFeatures.append("ruf" + x + "adv" + advertiserID.toString)
      })
      if (categories.nonEmpty) {
        namespace.append("ruf")
        namespace.append("ruf_adv_")
      }
    }
    (idFeatures, namespace)
  }

  def getRawHashedID(name: String): Int = {
    Murmur3Hash.stringHash64(name, 0).toInt
  }
}
