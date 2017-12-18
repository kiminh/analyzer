package com.cpc.spark.ml.ctrmodel.v1

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by roydong on 15/12/2017.
  */
object SaveFeatureIDs {

  def main(args: Array[String]): Unit = {
    val date = args(0)

    val spark = SparkSession.builder()
      .appName("save feature ids" + date)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val ulog = spark.sql(
      s"""
         |select * from dl_cpc.cpc_union_log where `date` = "%s" and isshow = 1
        """.stripMargin.format(date))
      .as[UnionLog].rdd.cache()

    saveids(spark, ulog.map(_.media_appsid.toInt), "mediaid", date)
    saveids(spark, ulog.map(_.planid), "planid", date)
    saveids(spark, ulog.map(_.unitid), "unitid", date)
    saveids(spark, ulog.map(_.ideaid), "ideaid", date)
    saveids(spark, ulog.map(_.adslotid.toInt), "slotid", date)
    saveids(spark, ulog.map(_.city), "cityid", date)
    saveids(spark, ulog.map(_.ext.getOrElse("adclass", ExtValue()).int_value), "adclass", date)

    ulog.unpersist()
  }

  def saveids(spark: SparkSession, ids: RDD[Int], name: String, date: String): Unit = {
    import spark.implicits._
    val path = "/user/cpc/feature_ids/%s/%s".format(name, date)
    ids.distinct(100).filter(_ > 0)
      .sortBy(x => x)
      .toDF()
      .write
      .mode(SaveMode.Overwrite)
      .parquet(path)
    println(path + " done")
  }
}
