package com.cpc.spark.hottopic

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/1/24 15:17
  */
object GetIdeaTitle {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"GetIdeaTitle date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

//        val sql =
//            s"""
//               |select click_title,valid_req_title,show_title
//               |from dl_cpc.kdd_uid_data
//               |where `date`='$date'
//               |and click_title is not null
//               |and length(click_title) > 0
//             """.stripMargin
//
//        val data = spark.sql(sql).rdd.map(x => x.getAs[String]("uid") + "|" + x.getAs[String]("click_title"))

        val reqSql =
            s"""
               |select valid_req_title
               |from dl_cpc.kdd_uid_data
               |where `date`='$date'
               |and valid_req_title is not null
               |and length(valid_req_title) > 0
             """.stripMargin

        val reqData = spark.sql(reqSql).rdd.map(x => x.getAs[String]("valid_req_title"))

        val clickSql =
            s"""
               |select click_title
               |from dl_cpc.kdd_uid_data
               |where `date`='$date'
               |and click_title is not null
               |and length(click_title) > 0
             """.stripMargin

        val clicData = spark.sql(clickSql).rdd.map(x => x.getAs[String]("click_title"))

        val showSql =
            s"""
               |select show_title
               |from dl_cpc.kdd_uid_data
               |where `date`='$date'
               |and show_title is not null
               |and length(show_title) > 0
             """.stripMargin

        val showData = spark.sql(clickSql).rdd.map(x => x.getAs[String]("show_title"))

        val data = reqData.union(clicData).union(showData).distinct()

        data.repartition(1).saveAsTextFile(s"hdfs://emr-cluster/warehouse/kdd/$date")


    }
}
