package com.cpc.spark.qukan.interest


import com.cpc.spark.udfs.myUdfs._
import com.cpc.spark.streaming.tools.SparkApp
import org.apache.spark.sql.functions.col
import com.cpc.spark.qukan.userprofile.SetUserProfileTag._
object GenDownloadTag {
  def main(args: Array[String]): Unit = {
    val dateBaseValue = args(0)
    for(dateAddValue<- 0 to 2){
      val sql1 = s"SELECT searchid,uid,isclick,isshow  from dl_cpc.cpc_union_log WHERE `date`=date_add('$dateBaseValue', $dateAddValue) and interaction=2 and isshow=1 and uid is not null"
      val sql2 = s"SELECT searchid,label as iscvr from  dl_cpc.ml_cvr_feature_v1 WHERE `date`=date_add('$dateBaseValue', $dateAddValue)"
      val Table1 = SparkApp.spark.sql(sql1)
      val Table2 = SparkApp.spark.sql(sql2)
      var unionTable = Table1.join(Table2,Seq("searchid"),"left_outer")
      var tableNameTemp =  s"test.cpc_downloadtag_"+dateAddValue.toString
      unionTable.write.mode("overwrite").saveAsTable(tableNameTemp)
    }

    var tableName = s"test.cpc_downloadtag_"+"0"
    var downloadTagTable = SparkApp.spark.table(tableName)
    for(dateAddValue<- 1 to 2) {
      tableName = s"test.cpc_downloadtag_"+dateAddValue.toString
      downloadTagTable = downloadTagTable.union(SparkApp.spark.table(tableName))
    }
    val tableNameTemp = "test.cpc_downloadtag"
    downloadTagTable.write.mode("overwrite").saveAsTable(tableNameTemp)
    println(downloadTagTable.count())
    downloadTagTable.show()

    val isshowNum = downloadTagTable.count().toDouble
    val clickNumAll = SparkApp.spark.table(tableNameTemp).filter("isclick=1").count().toDouble
    val iscvrNumAll = SparkApp.spark.table(tableNameTemp).filter("iscvr=1").count().toDouble
    val ctrThres = clickNumAll/isshowNum
    val cvrThres = iscvrNumAll/clickNumAll
    println(ctrThres+"   "+cvrThres)

    val sql3 = s"SELECT uid,sum(isclick) as clicknum ,sum(iscvr) as iscvrnum , sum(isshow) as showNum from test.cpc_downloadtag group by uid"


    downloadTagTable=SparkApp.spark.sql(sql3)
    println(downloadTagTable.filter("clicknum is null").count())
    println(downloadTagTable.filter("iscvrnum is null").count())
    println(downloadTagTable.filter("showNum is null").count())
    downloadTagTable.show()
    downloadTagTable = downloadTagTable.withColumn("downloadtag",downloadTag(ctrThres,cvrThres)(col("clicknum"),col("iscvrnum"),col("showNum")))
    downloadTagTable.show()
    var resultRdd=downloadTagTable.select("uid","downloadtag")
      .withColumn("operation",operationTag()()).rdd
        .map(x=>(x.getAs[String](0),x.getAs[Int](1),x.getAs[Boolean](2)))
    SetUserProfileTagInHiveHourly(resultRdd)

    }

}
