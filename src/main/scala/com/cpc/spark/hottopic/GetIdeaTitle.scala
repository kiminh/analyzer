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

        val sql =
            s"""
               |select uid,click_title
               |from dl_cpc.kdd_uid_data
               |where `date`='$date'
               |and click_title is not null
               |and length(click_title) > 0
             """.stripMargin

        val data = spark.sql(sql).rdd.map(x => x.getAs[String]("uid") + "|" + x.getAs[String]("click_title"))

        data.repartition(1).saveAsTextFile(s"hdfs://emr-cluster/warehouse/kdd/$date")


    }
}
