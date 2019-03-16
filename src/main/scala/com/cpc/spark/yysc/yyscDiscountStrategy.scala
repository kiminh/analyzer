package com.cpc.spark.yysc

import java.io.FileOutputStream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import yyscdiscount.yyscdiscount.discount
import yyscdiscount.yyscdiscount.yyscDiscount
/**
  * @author Jinbao
  * @date 2019/3/16 14:43
  */
object yyscDiscountStrategy {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val t = args(1).toInt //过期时间
        val spark = SparkSession.builder()
          .appName(s"yyscDiscountStrategy date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql =
            s"""
               |select uid,
               |    click_num,
               |    case when click_num > 15 and click_num <= 21 then 0.9
               |         when click_num > 21 and click_num <= 30 then 0.6
               |         when click_num > 31 and click_num <= 43 then 0.1
               |         else 0
               |    end as price_discount,
               |    case when click_num > 15 and click_num <= 21 then 0.9
               |         when click_num > 21 and click_num <= 30 then 0.6
               |         when click_num > 31 and click_num <= 43 then 0.1
               |         else 0
               |    end as coin_discount,
               |    '$date' as create_date
               |from
               |(
               |    select uid,sum(isclick) as click_num
               |    from dl_cpc.cpc_basedata_union_events
               |    where day = '$date'
               |    and media_appsid in ("80000001", "80000002")
               |    and adsrc = 1
               |    and adslot_type = 7
               |    and isclick = 1
               |    and (charge_type is null or charge_type=1)
               |    group by uid
               |) a
               |where click_num > 15
             """.stripMargin

        val uidData = spark.sql(sql)

        uidData.createOrReplaceTempView("tmp_data")

        val unionSql =
            s"""
               |select if (a.uid is null,b.uid,a.uid) as uid,
               |    case when a.uid is not null and b.uid is null then a.click_num
               |         when a.uid is null and b.uid is not null then b.click_num
               |         else if (a.click_num > b.click_num, a.click_num, b.click_num)
               |    end as click_num,
               |    case when a.uid is not null and b.uid is null then a.price_discount
               |         when a.uid is null and b.uid is not null then b.price_discount
               |         else if (a.click_num > b.click_num, a.price_discount, b.price_discount)
               |    end as price_discount,
               |    case when a.uid is not null and b.uid is null then a.coin_discount
               |         when a.uid is null and b.uid is not null then b.coin_discount
               |         else if (a.click_num > b.click_num, a.coin_discount, b.coin_discount)
               |    end as coin_discount,
               |    case when a.uid is not null and b.uid is null then a.create_date
               |         when a.uid is null and b.uid is not null then b.create_date
               |         else if (a.click_num > b.click_num, a.create_date, b.create_date)
               |    end as create_date,
               |    '$date' as `date`
               |(
               |    select uid, click_num, price_discount, coin_discount, create_date
               |    from dl_cpc.cpc_yysc_discount_record
               |    where `date`=date_sub('$date',1)
               |    and datediff('$date',create_date)<= $t
               |) a
               |full outer join
               |(
               |    select uid, click_num, price_discount, coin_discount, creat_date
               |    from tmp_data
               |) b
               |on a.uid = b.uid
             """.stripMargin

        val union = spark.sql(unionSql).cache()

        union.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.cpc_yysc_discount_record")

        val dis = union.select("uid","price_discount","coin_discount")
          .rdd
          .map(x => {
              val uid = x.getAs[String]("uid")
              val price = x.getAs[Double]("price_discount")
              val coin = x.getAs[Double]("coin_discount")
              discount(uid = uid, price = price, coin = coin)
          })
          .collect()

        val disPb = yyscDiscount(dis = dis)

        disPb.writeTo(new FileOutputStream("yyscDiscount.pb"))

        println("write to yyscDiscount.pb success")

    }
}
/*
create table if not exists dl_cpc.cpc_yysc_discount_record
(
    uid string,
    click_num int,
    price_discount double,
    coin_discount double,
    create_date string
)
PARTITIONED BY (`date` string)
STORED as PARQUET;
 */
