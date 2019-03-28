package com.cpc.spark.hottopic

import com.cpc.spark.hottopic.HotTopicCtrAuc.DetailAuc
import org.apache.spark.sql.SparkSession
import com.cpc.spark.tools.CalcMetrics
import com.cpc.spark.tools.OperateMySQL
/**
  * @author Liuyulin
  * @date 2019/3/25 15:10
  */
object HotTopicCtrCvrAucGauc {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val hour = args(1)
    val spark = SparkSession.builder()
      .appName(s"HotTopicCtrAuc date = $date")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val media = "hot_topic"
    val sql =
      s"""
         |select
         |a.score1,
         |a.score2,
         |a.label1,
         |a.ctr_model_name,
         |a.cvr_model_name,
         |a.uid,
         |case when b.searchid is not null then 1 else 0 end as label2
         |from
         |(select
         | searchid,
         |  exp_ctr as score1,
         |  exp_cvr as score2,
         |  isclick as label1,
         |  ctr_model_name,
         |  cvr_model_name,
         |  cast(uid as string) as uid
         |from dl_cpc.cpc_hot_topic_basedata_union_events
         |where day = '$date'
         |and `hour`='$hour'
         |and media_appsid in ('80002819')
         |and adsrc = 1
         |and isshow = 1
         |and ideaid > 0
         |and userid > 0
         |and (charge_type IS NULL OR charge_type = 1)  )a
         |left join
         |    (
         |        select tmp.searchid
         |        from
         |        (
         |            select
         |                final.searchid as searchid,
         |                final.ideaid as ideaid,
         |                case
         |                    when final.src="elds" and final.label_type=6 then 1
         |                    when final.src="feedapp" and final.label_type in (4, 5) then 1
         |                    when final.src="yysc" and final.label_type=12 then 1
         |                    when final.src="wzcp" and final.label_type in (1, 2, 3) then 1
         |                    when final.src="others" and final.label_type=6 then 1
         |                    else 0
         |                end as isreport
         |            from
         |            (
         |                select
         |                    searchid, media_appsid, uid,
         |                    planid, unitid, ideaid, adclass,
         |                    case
         |                        when (adclass like '134%' or adclass like '107%') then "elds"
         |                        when (adslot_type<>7 and adclass like '100%') then "feedapp"
         |                        when (adslot_type=7 and adclass like '100%') then "yysc"
         |                        when adclass in (110110100, 125100100) then "wzcp"
         |                        else "others"
         |                    end as src,
         |                    label_type
         |                from
         |                    dl_cpc.ml_cvr_feature_v1
         |                where
         |                    `date`='$date'
         |                    and 'hour'='$hour'
         |                    and label2=1
         |                    and media_appsid in ('80002819')
         |                ) final
         |            ) tmp
         |        where tmp.isreport=1
         |    ) b
         |    on a.searchid = b.searchid
             """.stripMargin

    val union = spark.sql(sql).cache()
    val CtrAucGaucListBuffer = scala.collection.mutable.ListBuffer[DetailAucGauc]()
    val CvrAucGaucListBuffer = scala.collection.mutable.ListBuffer[DetailAucGauc]()

    //分模型-ctr
    val ctrModelNames = union.filter("length(ctr_model_name)>0").select("ctr_model_name")
      .distinct()
      .collect()
      .map(x => x.getAs[String]("ctr_model_name"))

    println("ctrModelNames 's num is " + ctrModelNames.length)
    for (ctrModelName <- ctrModelNames) {
      val ctrModelUnion = union.filter(s"ctr_model_name = '$ctrModelName'").withColumnRenamed("score1","score").withColumnRenamed("label1","label")
      val ctrModelAuc = CalcMetrics.getAuc(spark,ctrModelUnion)
      val ctrModeGaucLists = CalcMetrics.getGauc(spark, ctrModelUnion,"uid")
        .filter(x => x.getAs[Double]("auc") != -1)
      .rdd
      .map(x => {
        val uid = x.getAs[String]("name")
        val auc = x.getAs[Double]("auc")
        val sum = x.getAs[Double]("sum")
        (uid,auc,sum)
      })
      .collect()
    var top =0.0
    var bottom =0.0
    for(ctrModeGaucList <- ctrModeGaucLists) {
      top += ctrModeGaucList._2 * ctrModeGaucList._3
      bottom += ctrModeGaucList._3
    }

    var gauc = top/bottom
      CtrAucGaucListBuffer += DetailAucGauc(
        auc = ctrModelAuc,
        gauc = gauc,
        model = ctrModelName,
        date = date,
        hour = hour)

      val ctrModeGaucLists1 = CalcMetrics.getGauc(spark, ctrModelUnion, "uid").collect()
      val gauc1 = ctrModeGaucLists1.filter(x => x.getAs[Double]("auc") != -1)
        .map(x => (x.getAs[Double]("auc") * x.getAs[Double]("sum"), x.getAs[Double]("sum")))
        .reduce((x, y) => (x._1 + y._1, x._2 + y._2))
      val gaucROC1 = if (gauc1._2 != 0) gauc1._1 * 1.0 / gauc1._2 else 0

    }

    val CtrAucGauc = CtrAucGaucListBuffer.toList.toDF()
    CtrAucGauc.repartition(1)
                    .write
                    .mode("overwrite")
                    //.saveAsTable("test.cpc_hot_topic_ctr_auc_gauc_hourly")
                    .insertInto("dl_cpc.cpc_hot_topic_ctr_auc_gauc_hourly")
    println("test.cpc_hot_topic_ctr_auc_gauc_hourly success!")

    val sql_cvr =
      s"""
         |select cvr_model_name,exp_cvr as score,uid,if(b.searchid is not null,1,0) as label
         |from
         |(
         |    select searchid,cvr_model_name,exp_cvr,uid
         |    from dl_cpc.cpc_hot_topic_basedata_union_events
         |    where day = '$date' and hour = '$hour'
         |    and media_appsid in ('80002819')
         |    and adsrc = 1
         |    and isclick = 1
         |    and (charge_type is null or charge_type=1)
         |    and uid not like "%.%"
         |    and uid not like "%000000%"
         |    and length(uid) in (14, 15, 36)
         |    and ideaid > 0
         |    and userid > 0
         |) a
         |left outer join
         |(
         |    select tmp.searchid
         |    from
         |    (
         |        select
         |            final.searchid as searchid,
         |            final.ideaid as ideaid,
         |            case when final.src="elds" and final.label_type=6 then 1
         |                when final.src="feedapp" and final.label_type in (4, 5) then 1
         |                when final.src="yysc" and final.label_type=12 then 1
         |                when final.src="wzcp" and final.label_type in (1, 2, 3) then 1
         |                when final.src="others" and final.label_type=6 then 1
         |                else 0
         |            end as isreport
         |        from
         |        (
         |            select searchid, media_appsid, uid,
         |                planid, unitid, ideaid, adclass,
         |                case when (adclass like '134%' or adclass like '107%') then "elds"
         |                    when (adslot_type<>7 and adclass like '100%') then "feedapp"
         |                    when (adslot_type=7 and adclass like '100%') then "yysc"
         |                    when adclass in (110110100, 125100100) then "wzcp"
         |                    else "others"
         |                end as src,
         |                label_type
         |            from
         |                dl_cpc.ml_cvr_feature_v1
         |            where
         |                `date`='$date' and hour = '$hour'
         |                and label2=1
         |                and media_appsid in ("80002819")
         |            ) final
         |        ) tmp
         |    where tmp.isreport=1
         |) b
         |on a.searchid = b.searchid
         |where cvr_model_name is not null
         |and length(cvr_model_name) > 0
         |and cvr_model_name not like '%noctr%'
             """.stripMargin


    val union_cvr = spark.sql(sql_cvr).cache()

//    分模型-cvr
    val cvrModelNames = union_cvr.select("cvr_model_name")
      .distinct()
      .collect()
      .map(_.getAs[String]("cvr_model_name"))

    println("cvrModelNames 's num is " + cvrModelNames.length)

    for (cvrModelName <- cvrModelNames) {
      val cvrModelUnion = union_cvr.filter(s"cvr_model_name = '$cvrModelName' ")
      val cvrModelAuc = CalcMetrics.getAuc(spark, cvrModelUnion)
      println("auc" + cvrModelAuc)
      var gauc1 = CalcMetrics.getGauc(spark, cvrModelUnion, "uid").filter("auc != -1").collect()
      if (gauc1.length > 0) {
        println("YES!")
        val gauc = gauc1
        .map(x => (x.getAs[Double]("auc") * x.getAs[Double]("sum"), x.getAs[Double]("sum")))
          .reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        val gaucROC = if (gauc._2 != 0) gauc._1 * 1.0 / gauc._2 else 0

        CvrAucGaucListBuffer += DetailAucGauc(
          auc = cvrModelAuc,
          gauc = gaucROC,
          model = cvrModelName,
          date = date,
          hour = hour)
      }
    }
    val CvrAucGauc = CvrAucGaucListBuffer.toList.toDF()
    CvrAucGauc.repartition(1)
      .write
      .mode("overwrite")
      //.saveAsTable("test.cpc_hot_topic_cvr_auc_gauc_hourly")
     .insertInto("dl_cpc.cpc_hot_topic_cvr_auc_gauc_hourly")
    println("test.cpc_hot_topic_cvr_auc_gauc_hourly success!")

    val tableName1 = "report2.cpc_hot_topic_ctr_auc_gauc_hourly"
    val tableName2 = "report2.cpc_hot_topic_cvr_auc_gauc_hourly"
    val deleteSql1 = s"delete from $tableName1 where 'date' = '$date' and hour = '$hour'"
    val deleteSql2 = s"delete from $tableName2 where 'date' = '$date' and hour = '$hour'"
    OperateMySQL.del(deleteSql1) //先删除历史数据
    CtrAucGauc.write.mode(SavaMode.Append)
      .jdbc(mariadb_write_url,"report2.cpc_hot_topic_ctr_auc_gauc_hourly",mariadb_write_prop)
    println("insert into report2.cpc_hot_topic_ctr_auc_gauc_hourly success!")
    CtrAucGauc.unpersist()

    OperateMySQL.del(deleteSql2) //先删除历史数据
    CvrAucGauc.write.mode(SavaMode.Append)
      .jdbc(mariadb_write_url,"report2.cpc_hot_topic_cvr_auc_gauc_hourly",mariadb_write_prop)
    println("insert into report2.cpc_hot_topic_cvr_auc_gauc_hourly success!")
    CvrAucGauc.unpersist()

//    OperateMySQL.insert(CtrAucGauc,tableName1)
//    OperateMySQL.update(deleteSql2) //先删除历史数据
//    OperateMySQL.insert(CvrAucGauc,tableName2)

  }
  case class DetailAucGauc(
                           var auc: Double = 0,
                           var gauc: Double = 0,
                           var model: String = "",
                           var date: String = "",
                           var hour: String = "")
}
