package com.cpc.spark.OcpcProtoType.suggest_cpa_qtt

import org.apache.spark.sql._

object OcpcCheckFileOcpc {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession
      .builder()
      .appName(s"ocpc unitid auc: $date, $hour")
      .enableHiveSupport().getOrCreate()

    val sqlRequest1 =
      s"""
         |SELECT
         |    t.`date`,
         |    sum(case when t.isclick=1 then t.price else 0 end) * 0.01 as cost,
         |    round(sum(case WHEN t.isclick=1 then t.price else 0 end)*0.1/sum(t.isshow),3) as cpm,
         |    sum(case when t.isclick=1 then t.price else 0 end) * 0.01 / sum(t.iscvr) as cpa,
         |    sum(t.isclick) * 1.0 / sum(t.isshow) as ctr,
         |    sum(t.iscvr) * 1.0 / sum(t.isclick) as cvr,
         |    sum(t.isshow) as show,
         |    sum(t.isclick) as click,
         |    sum(t.iscvr) as cv
         |FROM
         |    (select
         |        a.`date`,
         |        a.unitid,
         |        a.userid,
         |        a.isclick,
         |        a.isshow,
         |        a.price,
         |        b.iscvr,
         |        a.is_ocpc
         |    from
         |        (select
         |            searchid,
         |            unitid,
         |            userid,
         |            isclick,
         |            isshow,
         |            price,
         |            `date`,
         |            is_ocpc
         |        from
         |            dl_cpc.ocpc_filter_unionlog
         |        where
         |            `date` between '2019-03-01' and '2019-03-10'
         |        and
         |            (cast(adclass as string) like "134%" or cast(adclass as string) like "107%")
         |        and
         |            media_appsid  in ("80000001", "80000002")
         |        and
         |            isshow = 1
         |        and
         |            antispam = 0
         |        and
         |            adsrc = 1
         |        and
         |            is_ocpc=1
         |        AND charge_type IN (0,1,28)
         |        ) as a
         |    left join
         |        (
         |            select cv_table.searchid, 1 as iscvr
         |            from
         |            (
         |                -- 赤兔
         |                select
         |                    searchid
         |                from dl_cpc.ml_cvr_feature_v1
         |                lateral view explode(cvr_list) b as a
         |                where `date` >= '2019-03-01'
         |                and access_channel="site"
         |                and a in ('ctsite_form')
         |                and (adclass like '134%' or adclass like '107%')
         |
         |                union all
         |                -- 表单
         |                SELECT
         |                    searchid
         |                FROM
         |                    dl_cpc.site_form_unionlog
         |                WHERE
         |                    `date` >= '2019-03-01'
         |                AND
         |                    ideaid>0
         |                AND
         |                    searchid is not null
         |
         |
         |                union all
         |                -- api
         |                SELECT
         |                    searchid
         |                FROM
         |                    dl_cpc.ml_cvr_feature_v2
         |                WHERE
         |                    `date` >= '2019-03-01'
         |                AND
         |                    label=1
         |                AND
         |                    (adclass like '134%' or adclass like '107%')
         |            ) cv_table
         |            group by searchid
         |        ) b
         |    on
         |        a.searchid = b.searchid) as t
         |GROUP BY t.`date`
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |SELECT
         |    t.`date`,
         |    sum(case when t.isclick=1 then t.price else 0 end) * 0.01 as cost,
         |    round(sum(case WHEN t.isclick=1 then t.price else 0 end)*0.1/sum(t.isshow),3) as cpm,
         |    sum(case when t.isclick=1 then t.price else 0 end) * 0.01 / sum(t.iscvr) as cpa,
         |    sum(t.isclick) * 1.0 / sum(t.isshow) as ctr,
         |    sum(t.iscvr) * 1.0 / sum(t.isclick) as cvr,
         |    sum(t.isshow) as show,
         |    sum(t.isclick) as click,
         |    sum(t.iscvr) as cv
         |FROM
         |    (select
         |        a.`date`,
         |        a.unitid,
         |        a.userid,
         |        a.isclick,
         |        a.isshow,
         |        a.price,
         |        b.iscvr,
         |        a.is_ocpc
         |    from
         |        (select
         |            searchid,
         |            unitid,
         |            userid,
         |            isclick,
         |            isshow,
         |            price,
         |            `date`,
         |            is_ocpc
         |        from
         |            dl_cpc.ocpc_filter_unionlog
         |        where
         |            `date` between '2019-03-11' and '2019-03-20'
         |        and
         |            (cast(adclass as string) like "134%" or cast(adclass as string) like "107%")
         |        and
         |            media_appsid  in ("80000001", "80000002")
         |        and
         |            isshow = 1
         |        and
         |            antispam = 0
         |        and
         |            adsrc = 1
         |        and
         |            is_ocpc=1
         |        AND charge_type IN (0,1,28)
         |        ) as a
         |    left join
         |        (
         |            select cv_table.searchid, 1 as iscvr
         |            from
         |            (
         |                -- 赤兔
         |                select
         |                    searchid
         |                from dl_cpc.ml_cvr_feature_v1
         |                lateral view explode(cvr_list) b as a
         |                where `date` >= '2019-03-01'
         |                and access_channel="site"
         |                and a in ('ctsite_form')
         |                and (adclass like '134%' or adclass like '107%')
         |
         |                union all
         |                -- 表单
         |                SELECT
         |                    searchid
         |                FROM
         |                    dl_cpc.site_form_unionlog
         |                WHERE
         |                    `date` >= '2019-03-01'
         |                AND
         |                    ideaid>0
         |                AND
         |                    searchid is not null
         |
         |
         |                union all
         |                -- api
         |                SELECT
         |                    searchid
         |                FROM
         |                    dl_cpc.ml_cvr_feature_v2
         |                WHERE
         |                    `date` >= '2019-03-01'
         |                AND
         |                    label=1
         |                AND
         |                    (adclass like '134%' or adclass like '107%')
         |            ) cv_table
         |            group by searchid
         |        ) b
         |    on
         |        a.searchid = b.searchid) as t
         |GROUP BY t.`date`
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark.sql(sqlRequest2)


    val sqlRequest3 =
      s"""
         |SELECT
         |    t.`date`,
         |    sum(case when t.isclick=1 then t.price else 0 end) * 0.01 as cost,
         |    round(sum(case WHEN t.isclick=1 then t.price else 0 end)*0.1/sum(t.isshow),3) as cpm,
         |    sum(case when t.isclick=1 then t.price else 0 end) * 0.01 / sum(t.iscvr) as cpa,
         |    sum(t.isclick) * 1.0 / sum(t.isshow) as ctr,
         |    sum(t.iscvr) * 1.0 / sum(t.isclick) as cvr,
         |    sum(t.isshow) as show,
         |    sum(t.isclick) as click,
         |    sum(t.iscvr) as cv
         |FROM
         |    (select
         |        a.`date`,
         |        a.unitid,
         |        a.userid,
         |        a.isclick,
         |        a.isshow,
         |        a.price,
         |        b.iscvr,
         |        a.is_ocpc
         |    from
         |        (select
         |            searchid,
         |            unitid,
         |            userid,
         |            isclick,
         |            isshow,
         |            price,
         |            `date`,
         |            is_ocpc
         |        from
         |            dl_cpc.ocpc_filter_unionlog
         |        where
         |            `date` between '2019-03-21' and '2019-03-26'
         |        and
         |            (cast(adclass as string) like "134%" or cast(adclass as string) like "107%")
         |        and
         |            media_appsid  in ("80000001", "80000002")
         |        and
         |            isshow = 1
         |        and
         |            antispam = 0
         |        and
         |            adsrc = 1
         |        and
         |            is_ocpc=1
         |        AND charge_type IN (0,1,28)
         |        ) as a
         |    left join
         |        (
         |            select cv_table.searchid, 1 as iscvr
         |            from
         |            (
         |                -- 赤兔
         |                select
         |                    searchid
         |                from dl_cpc.ml_cvr_feature_v1
         |                lateral view explode(cvr_list) b as a
         |                where `date` >= '2019-03-01'
         |                and access_channel="site"
         |                and a in ('ctsite_form')
         |                and (adclass like '134%' or adclass like '107%')
         |
         |                union all
         |                -- 表单
         |                SELECT
         |                    searchid
         |                FROM
         |                    dl_cpc.site_form_unionlog
         |                WHERE
         |                    `date` >= '2019-03-01'
         |                AND
         |                    ideaid>0
         |                AND
         |                    searchid is not null
         |
         |
         |                union all
         |                -- api
         |                SELECT
         |                    searchid
         |                FROM
         |                    dl_cpc.ml_cvr_feature_v2
         |                WHERE
         |                    `date` >= '2019-03-01'
         |                AND
         |                    label=1
         |                AND
         |                    (adclass like '134%' or adclass like '107%')
         |            ) cv_table
         |            group by searchid
         |        ) b
         |    on
         |        a.searchid = b.searchid) as t
         |GROUP BY t.`date`
       """.stripMargin
    println(sqlRequest3)
    val data3 = spark.sql(sqlRequest3)

    val result = data1.union(data2).union(data3)

    result.write.mode("overwrite").saveAsTable("test.check_data_elds20190327ocpc1")

  }
}
