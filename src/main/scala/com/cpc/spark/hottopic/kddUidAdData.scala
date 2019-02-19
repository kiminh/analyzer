package com.cpc.spark.hottopic

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/2/16 17:02
  */
object kddUidAdData {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val hour = args(1)

        val spark = SparkSession.builder()
          .appName(s"kddUidAdData date = $date , hour = $hour")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql1 =
            s"""
               |select title,title_split
               |from dl_cpc.kdd_ad_title_split
             """.stripMargin


        val title = spark.sql(sql1).cache()

        title.show(10)

        val sql2 =
            s"""
               |select *
               |from dl_cpc.kdd_ad_log
               |where `date` = '$date'
               |and hour = '$hour'
             """.stripMargin

        val union = spark.sql(sql2)
          .withColumnRenamed("cpc_req_title","title")
          .join(title,Seq("title"),"left_outer")
          .drop("title")
          .withColumnRenamed("title_split","cpc_req_title")

          .withColumnRenamed("csj_req_title","title")
          .join(title,Seq("title"),"left_outer")
          .drop("title")
          .withColumnRenamed("title_split","csj_req_title")

          .withColumnRenamed("gdt_req_title","title")
          .join(title,Seq("title"),"left_outer")
          .drop("title")
          .withColumnRenamed("title_split","gdt_req_title")

          .withColumnRenamed("cpc_show_title","title")
          .join(title,Seq("title"),"left_outer")
          .drop("title")
          .withColumnRenamed("title_split","cpc_show_title")

          .withColumnRenamed("csj_show_title","title")
          .join(title,Seq("title"),"left_outer")
          .drop("title")
          .withColumnRenamed("title_split","csj_show_title")

          .withColumnRenamed("gdt_show_title","title")
          .join(title,Seq("title"),"left_outer")
          .drop("title")
          .withColumnRenamed("title_split","gdt_show_title")

          .withColumnRenamed("cpc_click_title","title")
          .join(title,Seq("title"),"left_outer")
          .drop("title")
          .withColumnRenamed("title_split","cpc_click_title")

          .withColumnRenamed("csj_click_title","title")
          .join(title,Seq("title"),"left_outer")
          .drop("title")
          .withColumnRenamed("title_split","csj_click_title")

          .withColumnRenamed("gdt_click_title","title")
          .join(title,Seq("title"),"left_outer")
          .drop("title")
          .withColumnRenamed("title_split","gdt_click_title")

        title.unpersist()

        union.show(10)

        union.createOrReplaceTempView("union")

        val sql3 =
            s"""
               |select uid,
               |  concat_ws("\001",
               |    concat_ws("\002","cpc_req_type","str",concat_ws("\003", collect_set(cpc_req_type))) as cpc_req_type,
               |    concat_ws("\002","csj_req_type","str",concat_ws("\003", collect_set(csj_req_type))) as csj_req_type,
               |    concat_ws("\002","gdt_req_type","str",concat_ws("\003", collect_set(gdt_req_type))) as gdt_req_type,
               |    concat_ws("\002","cpc_req_position","str",concat_ws("\003", collect_set(cpc_req_position))) as cpc_req_position,
               |    concat_ws("\002","csj_req_position","str",concat_ws("\003", collect_set(csj_req_position))) as csj_req_position,
               |    concat_ws("\002","gdt_req_position","str",concat_ws("\003", collect_set(gdt_req_position))) as gdt_req_position,
               |    concat_ws("\002","cpc_req_title","str",concat_ws("\003", collect_set(cpc_req_title))) as cpc_req_title,
               |    concat_ws("\002","csj_req_title","str",concat_ws("\003", collect_set(csj_req_title))) as csj_req_title,
               |    concat_ws("\002","gdt_req_title","str",concat_ws("\003", collect_set(gdt_req_title))) as gdt_req_title,
               |    concat_ws("\002","cpc_show_type","str",concat_ws("\003", collect_set(cpc_show_type))) as cpc_show_type,
               |    concat_ws("\002","csj_show_type","str",concat_ws("\003", collect_set(csj_show_type))) as csj_show_type,
               |    concat_ws("\002","gdt_show_type","str",concat_ws("\003", collect_set(gdt_show_type))) as gdt_show_type,
               |    concat_ws("\002","cpc_show_position","str",concat_ws("\003", collect_set(cpc_show_position))) as cpc_show_position,
               |    concat_ws("\002","csj_show_position","str",concat_ws("\003", collect_set(csj_show_position))) as csj_show_position,
               |    concat_ws("\002","gdt_show_position","str",concat_ws("\003", collect_set(gdt_show_position))) as gdt_show_position,
               |    concat_ws("\002","cpc_show_title","str",concat_ws("\003", collect_set(cpc_show_title))) as cpc_show_title,
               |    concat_ws("\002","csj_show_title","str",concat_ws("\003", collect_set(csj_show_title))) as csj_show_title,
               |    concat_ws("\002","gdt_show_title","str",concat_ws("\003", collect_set(gdt_show_title))) as gdt_show_title,
               |    concat_ws("\002","cpc_click_type","str",concat_ws("\003", collect_set(cpc_click_type))) as cpc_click_type,
               |    concat_ws("\002","csj_click_type","str",concat_ws("\003", collect_set(csj_click_type))) as csj_click_type,
               |    concat_ws("\002","gdt_click_type","str",concat_ws("\003", collect_set(gdt_click_type))) as gdt_click_type,
               |    concat_ws("\002","cpc_click_position","str",concat_ws("\003", collect_set(cpc_click_position))) as cpc_click_position,
               |    concat_ws("\002","csj_click_position","str",concat_ws("\003", collect_set(csj_click_position))) as csj_click_position,
               |    concat_ws("\002","gdt_click_position","str",concat_ws("\003", collect_set(gdt_click_position))) as gdt_click_position,
               |    concat_ws("\002","cpc_click_title","str",concat_ws("\003", collect_set(cpc_click_title))) as cpc_click_title,
               |    concat_ws("\002","csj_click_title","str",concat_ws("\003", collect_set(csj_click_title))) as csj_click_title,
               |    concat_ws("\002","gdt_click_title","str",concat_ws("\003", collect_set(gdt_click_title))) as gdt_click_title
               |) as origin,
               |  '$date' as `date`,
               |  '$hour' as hour
               |from union
               |group by uid
             """.stripMargin

        val result = spark.sql(sql3)

        result.repartition(10)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.kdd_uid_ad_log")
    }
}

/*
create table if not exists dl_cpc.kdd_uid_ad_log
(
    uid string,
    origin string
)
PARTITIONED by (`date` string, hour string)
STORED as PARQUET;
 */
