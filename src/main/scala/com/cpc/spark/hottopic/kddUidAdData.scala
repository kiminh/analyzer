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
               |from from dl_cpc.kdd_ad_log
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

        title.show(10)


    }
}
