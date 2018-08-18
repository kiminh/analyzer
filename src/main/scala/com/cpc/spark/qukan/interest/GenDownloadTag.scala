package com.cpc.spark.qukan.interest
import com.cpc.spark.udfs.myUdfs._
import com.cpc.spark.streaming.tools.SparkApp
import org.apache.spark.sql.functions.col
import com.cpc.spark.qukan.userprofile.SetUserProfileTag._
import org.apache.spark.sql.SparkSession
object GenDownloadTag {

  def main(args: Array[String]): Unit = {
    val dateBaseValue = args(0)
    for(dateAddValue<- 0 to 0){


      val sql1 = s"SELECT searchid,uid,isclick,isshow  from dl_cpc.cpc_union_log WHERE `date`=date_add('$dateBaseValue', $dateAddValue) and interaction=2 and adsrc = 1 and isshow=1 and uid is not null"


      val sql2 = s"SELECT searchid , trace_op1 as iscvr  from  dl_cpc.cpc_union_trace_log  WHERE `date`=date_add('$dateBaseValue', $dateAddValue)"


      val ctx=SparkSession.builder()
        .appName("master")
        .enableHiveSupport()
        .getOrCreate()


      val Table1 = ctx.sql(sql1)

      val Table2 = ctx.sql(sql2)

      var unionTable = Table1.join(Table2,Seq("searchid"),"left_outer")

      var tableNameTemp =  s"dl_cpc.cpc_downloadtag_"+dateAddValue.toString
      ctx.sql(s"drop table if exists $tableNameTemp")

      unionTable.write.mode("overwrite").saveAsTable(tableNameTemp)
    }

    val ctx=SparkSession.builder()
      .appName("master")
      .enableHiveSupport()
      .getOrCreate()
    var tableName = s"dl_cpc.cpc_downloadtag_"+"0"
    var downloadTagTable = ctx.table(tableName)
   /* for(dateAddValue<- 1 to 2) {
      tableName = s"test.cpc_downloadtag_"+dateAddValue.toString
      downloadTagTable = downloadTagTable.union(ctx.table(tableName))
    }*/


    downloadTagTable = downloadTagTable.withColumn("iscvrint",ChangeCvrStringToInt()(col("iscvr")))



    val tableNameTemp = "test.cpc_downloadtag"
    ctx.sql(s"drop table if exists $tableNameTemp")
    downloadTagTable.write.mode("overwrite").saveAsTable(tableNameTemp)

    println(ctx.sql("select count(*) from test.cpc_downloadtag where uid is not null"))
    println(ctx.sql("select count(*) from test.cpc_downloadtag where isclick=1"))
    val isshowNum = downloadTagTable.count().toDouble
    val clickNumAll = downloadTagTable.filter("isclick=1").count().toDouble
    val iscvrNumAll = downloadTagTable.filter("iscvrint='1'").count().toDouble



    val ctrThres = clickNumAll/isshowNum
    val cvrThres = iscvrNumAll/clickNumAll


    println(ctrThres)
    println(cvrThres)
    val sql3 = s"SELECT uid,sum(isclick) as clicknum ,sum(if (iscvrint='1',1,0)) as iscvrnum , sum(isshow) as showNum from test.cpc_downloadtag group by uid"


    downloadTagTable=ctx.sql(sql3)
    println(downloadTagTable.withColumn("ctr",calculatectr()(col("clicknum"),col("iscvrnum")))
        .filter(s"ctr>$ctrThres").count())

    downloadTagTable = downloadTagTable.withColumn("downloadtag",downloadTag(ctrThres,cvrThres)(col("clicknum"),col("iscvrnum"),col("showNum")))

    println(downloadTagTable.filter("downloadtag='243'").count())
    println(downloadTagTable.filter("downloadtag='244'").count())
    /*var resultRdd=downloadTagTable.select("uid","downloadtag")
      .withColumn("operation",operationTag()()).rdd
        .map(x=>(x.getAs[String](0),x.getAs[Int](1),x.getAs[Boolean](2)))

    val result = SetUserProfileTagInHiveDaily(resultRdd)*/


    }

}
