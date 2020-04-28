package com.cpc.spark.qukan.userprofile

/**
  * @author xuxiang -> fym 2019-04-28 15:10
  */

import sys.process._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import com.cpc.spark.qukan.utils.Udfs

object CreateNewTagTable {
  def main(args: Array[String]): Unit = {
    val dateValue: String = args(0)
    val dateRange: Int = args(1).toInt
    val spark: SparkSession = SparkSession.builder().appName("[cpc-data] member-id").enableHiveSupport().getOrCreate()
    for (i <- 0 to dateRange) {
      val zfbTable: Dataset[Row] = spark.sql(s"SELECT ROW_NUMBER() over( partition by member_id order by update_time DESC) as rows1, member_id, info from gobblin.qukan_member_zfb_log").filter("rows1 = 1")
      val mainTable: DataFrame = spark.sql(s"SELECT device as uid, member_id, max(day) as `date` from bdm.qukan_daily_active_p WHERE `day` = date_add('%s', %s) GROUP BY device, member_id".format(dateValue, i))
      val unionTable1: DataFrame = spark.sql(s"select uid, max(sex) as sex, max(`timestamp`) as `timestamp` from dl_cpc.cpc_basedata_adx_event where `day` = date_add('%s', %s) and uid <> '' group by uid".format(dateValue, i))
      val unionTable2: DataFrame = spark.table("dl_cpc.cpc_bd_adx_blob").filter(s"day = date_add('%s', %s) and uid <> ''".format(dateValue, i)).select("uid",  "timestamp", "tags")
      val UnionTable: DataFrame = unionTable2.join(unionTable1, Seq("uid", "timestamp"), "inner").select("uid", "sex", "tags")
        .groupBy("uid").agg("sex" -> "max", "tags" -> "max").withColumnRenamed("max(tags)", "tags").withColumnRenamed("max(sex)", "sex")
        .withColumn("isstudent", Udfs.checkStudent()(col("tags"))).withColumn("isstudent2", Udfs.checkStudent2()(col("tags")))
        .select("uid", "sex", "isstudent", "isstudent2")

      mainTable.join(UnionTable, Seq("uid"), "left_outer")
        .join(zfbTable, Seq("member_id"), "left_outer")
        .withColumn("iszfb", Udfs.checkZfb()(col("info")))
        .withColumn("birthday", Udfs.birthdayZfb()(col("info")))
        .select("uid", "member_id", "sex", "isstudent", "iszfb", "birthday","isstudent2", "date")
        .write.mode("overwrite").insertInto("dl_cpc.cpc_uid_memberid_tag_daily")

      s"hadoop fs -touchz hdfs://emr-cluster/home/successflag/cpcdept/%s/dl_cpc.cpc_uid_memberid_tag_daily-00-00.ok".format(dateValue) !
    }
  }
}
