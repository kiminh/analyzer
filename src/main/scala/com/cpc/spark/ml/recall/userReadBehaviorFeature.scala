package com.cpc.spark.ml.recall

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

object userReadBehaviorFeature {
  def main(args: Array[String]): Unit = {
    val days = 1
    val model_target = args(0).toInt
    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .appName("prepare article data_b".format())
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -days)
    val sdate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, days)
    val edate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    var stmt =
      """
        |select device_id as did, features["u_dy_5_readkeyword"].stringarrayvalue as keyWords,
        |features['u_dy_6_readcate'].stringarrayvalue as rec_user_cate,
        |features['u_dy_6_favocate5'].stringarrayvalue as rec_user_fav
        |from dl_cpc.cpc_user_features_from_algo
        |where load_date="%s"
        |
      """.stripMargin.format(sdate)
  }

}
