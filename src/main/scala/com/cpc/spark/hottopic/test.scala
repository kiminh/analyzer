package com.cpc.spark.hottopic

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/1/25 17:01
  */
object test {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"test date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val reqSql =
            s"""
               |select substr(valid_req_title,0, length(valid_req_title) -1)
               |from dl_cpc.kdd_uid_data
               |where `date`='$date'
               |and valid_req_title is not null
               |and length(valid_req_title) > 0
             """.stripMargin

        val d = spark.sparkContext.textFile("hdfs://emr-cluster/warehouse/kdd_result/result3.txt")
          .map(x => {
              val y = x.split("|")
              tmp(title = y(0), titlesplit=y(1))
          })
          .toDF()

        val req = spark.sql(reqSql)
        val res = req.join(d,  req("valid_req_title")===d("title"))

        res.show(20)
    }
    case class tmp(var title:String = "", var titlesplit:String = "")
}
