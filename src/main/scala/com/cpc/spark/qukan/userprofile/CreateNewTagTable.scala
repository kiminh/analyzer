package com.cpc.spark.qukan.userprofile

/**
  * @author fym
  * @version created: 2019-04-28 15:10
  * @desc inherited from an ancient version (anonymous author).
  */

import java.util.Calendar

import sys.process._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.qukan.utils.Udfs

import scala.collection.mutable

object CreateNewTagTable {

  def main(args: Array[String]): Unit = {
    
    val dateValue = args(0)
    val dateRange = args(1).toInt

    val spark = SparkSession.builder()
      .appName("[root-cpc] create new tag table")
      .enableHiveSupport()
      .getOrCreate()

    for (i <- 0 to dateRange) {
      val sql1 = s"SELECT  ROW_NUMBER() over(partition by member_id  order by update_time DESC) as rows1,member_id ,info  from gobblin.qukan_member_zfb_log"
      var zfbTable = spark.sql(sql1).filter("rows1=1")

      val sql3 = s"SELECT  device as uid,member_id,max(day) as `date` from bdm.qukan_daily_active_p WHERE `day` = date_add('$dateValue', $i) GROUP BY device,member_id"
      val mainTable = spark.sql(sql3)

      val t1 = s"test.cpc_uid_max_timestamp"
      val sql2 = s"select uid ,max(`timestamp`) as `timestamp` from dl_cpc.cpc_basedata_union_events where `day` = date_add('$dateValue', $i) and uid<>'' group by uid"
      spark.sql(s"drop table if exists $t1")

      spark.sql(sql2).write.mode("overwrite").saveAsTable(t1)
      val UnionTable1 = spark.table(t1)
      println(s"$t1 done")

      val UnionTable2 = spark.table("dl_cpc.cpc_basedata_union_events")
        .filter(s"day = date_add('$dateValue', $i) and uid IS NOT NULL")
        .select("uid", "sex", "timestamp", "interests")

      val t2 = s"test.cpc_uid_max_timestamp_info"
      spark.sql(s"drop table if exists $t2")
      UnionTable2.join(UnionTable1, Seq("uid", "timestamp"), "inner")
        .select("uid", "sex", "interests").write.mode("overwrite").saveAsTable(t2)
      var UnionTable = spark.table(t2)
      println(s"$t2 done")

      UnionTable = UnionTable.groupBy("uid")
        .agg("sex" -> "max", "interests" -> "max")
        .withColumnRenamed("max(interests)", "interests")
        .withColumnRenamed("max(sex)", "sex")

      UnionTable = UnionTable.withColumn("isstudent", Udfs.checkStudent()(col("interests")))
        .withColumn("isstudent2", Udfs.checkStudent2()(col("interests")))
        .select("uid", "sex", "isstudent","isstudent2")



      // var zfbMap = mutable.LinkedHashMap[String, String]()

      // for (row <- zfbTable.collect()) {
      //   zfbMap += (row(0).toString.trim -> row(1).toString.trim)
      // }


      val mainJoinUnionTable = mainTable.join(UnionTable, Seq("uid"), "left_outer")
        .join(zfbTable, Seq("member_id"), "left_outer")
        .withColumn("iszfb", Udfs.checkZfb()(col("info")))
        .withColumn("birthday", Udfs.birthdayZfb()(col("info")))
        .select("uid", "member_id", "sex", "isstudent", "iszfb", "birthday","isstudent2", "date")
      //.select("uid", "member_id",  "isstudent", "iszfb", "birthday", "date")

      mainJoinUnionTable.show()
      val tableName = "dl_cpc.cpc_uid_memberid_tag_daily"

      println("begin to insert")
      mainJoinUnionTable.write.mode("overwrite").insertInto(tableName)

      println(dateValue,i,"done")

      spark.sql(s"drop table if exists $t1")
      spark.sql(s"drop table if exists $t2")

      val cal = Calendar.getInstance()

      cal.set(Calendar.YEAR, dateValue.split("-")(0).toInt)
      cal.set(Calendar.MONTH, dateValue.split("-")(1).toInt - 1)
      cal.set(Calendar.DAY_OF_MONTH, dateValue.split("-")(2).toInt)
      cal.set(Calendar.HOUR_OF_DAY, 0)
      cal.set(Calendar.MINUTE, 0)
      cal.set(Calendar.SECOND, 0)

      val okFileName = "hdfs://emr-cluster/home/successflag/cpcdept/%s/%s.%s-00-00.ok"
        .format(
          dateValue,
          "dl_cpc",
          "cpc_uid_memberid_tag_daily"
        )

      val createOkFile = s"hadoop fs -touchz %s"
        .format(okFileName) !
    }
  }
}