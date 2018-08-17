package com.cpc.spark.qukan.interest
import com.cpc.spark.udfs.myUdfs._
import com.cpc.spark.streaming.tools.SparkApp
import org.apache.spark.sql.functions.col
import com.cpc.spark.qukan.userprofile.SetUserProfileTag._
import org.apache.spark.sql.SparkSession
object GenDownloadTag {

  def main(args: Array[String]): Unit = {
    val dateBaseValue = args(0)
    for(dateAddValue<- 0 to 1){


      val sql1 = s"SELECT searchid,uid,isclick,isshow  from dl_cpc.cpc_union_log WHERE `date`=date_add('$dateBaseValue', $dateAddValue) and interaction=2 and isshow=1 and uid is not null"

      println(sql1)
      val sql2 = s"SELECT searchid , trace_op1 as iscvr  from  dl_cpc.cpc_union_trace_log  WHERE `date`=date_add('$dateBaseValue', $dateAddValue)"
      println(sql2)

      val ctx=SparkSession.builder()
        .appName("master")
        .enableHiveSupport()
        .getOrCreate()

      println(111)
      val Table1 = ctx.sql(sql1)
      println(222)
      val Table2 = ctx.sql(sql2)
      println(333)
      var unionTable = Table1.join(Table2,Seq("searchid"),"left_outer")
      unionTable.show()
      var tableNameTemp =  s"dl_cpc.cpc_downloadtag_"+dateAddValue.toString
      ctx.sql(s"drop table if exists $tableNameTemp")
      println(444)
      unionTable.write.mode("overwrite").saveAsTable(tableNameTemp)
    }
    println("union begin")
    val ctx=SparkSession.builder()
      .appName("master")
      .enableHiveSupport()
      .getOrCreate()
    var tableName = s"dl_cpc.cpc_downloadtag_"+"0"
    var downloadTagTable = ctx.table(tableName)
    for(dateAddValue<- 1 to 1) {
      tableName = s"test.cpc_downloadtag_"+dateAddValue.toString
      downloadTagTable = downloadTagTable.union(ctx.table(tableName))
    }
    println("add column begin")
    downloadTagTable = downloadTagTable.withColumn("iscvrint",ChangeCvrStringToInt()(col("iscvr")))

    println("add column done")
    val tableNameTemp = "test.cpc_downloadtag"
    ctx.sql(s"drop table if exists $tableNameTemp")
    downloadTagTable.write.mode("overwrite").saveAsTable(tableNameTemp)
    println(downloadTagTable.count())
    downloadTagTable.show()
    println("union done")
    val isshowNum = downloadTagTable.count().toDouble
    val clickNumAll = ctx.table(tableNameTemp).filter("isclick=1").count().toDouble
    val iscvrNumAll = ctx.table(tableNameTemp).filter("iscvrint='1'").count().toDouble

    println("isshowNum  :"+isshowNum)
    println("clickNumAll  :"+clickNumAll)
    println("iscvrNumAll  :"+iscvrNumAll)
    val ctrThres = clickNumAll/isshowNum
    val cvrThres = iscvrNumAll/clickNumAll
    println(ctrThres+"   "+cvrThres)

    val sql3 = s"SELECT uid,sum(isclick) as clicknum ,sum(if (iscvrint='1',1,0)) as iscvrnum , sum(isshow) as showNum from test.cpc_downloadtag group by uid"


    downloadTagTable=ctx.sql(sql3)
    println(downloadTagTable.filter("clicknum is null").count())
    println(downloadTagTable.filter("iscvrnum is null").count())
    println(downloadTagTable.filter("showNum is null").count())
    downloadTagTable.show()
    downloadTagTable = downloadTagTable.withColumn("downloadtag",downloadTag(ctrThres,cvrThres)(col("clicknum"),col("iscvrnum"),col("showNum")))
    downloadTagTable.show()
    var resultRdd=downloadTagTable.select("uid","downloadtag")
      .withColumn("operation",operationTag()()).rdd
        .map(x=>(x.getAs[String](0),x.getAs[Int](1),x.getAs[Boolean](2)))
    println("table done")
    SetUserProfileTagInHiveHourly(resultRdd)

    }

}
