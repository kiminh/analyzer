package com.cpc.spark.material

import java.io.File

import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis
import org.apache.spark.sql._


/**
  * 备份redis中的物料数据到hive
  */
object MaterialFromRedisToHive {

  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val filePath = s"${args(1)}/$dt.json"
    val dbName = args(2)
    val tableName = args(3)

    val host = "r-2ze226246c9ccae4.redis.rds.aliyuncs.com"
    val port = 6379
    val auth = "GkzF0odYHNHpBcov"
    println(s"""${">" * 5}dt: $dt""")
    println(s"""${">" * 5}本地存储路径: $filePath""")
    println(s"""${">" * 5}dbName: $dbName""")
    println(s"""${">" * 5}tableName: $tableName""")


    val jedis = new Jedis(host, port)
    jedis.auth(auth)
    // 查询所有material, 存入本地文件
    val file = new File(filePath)
    file.createNewFile()
    val beginTimeMillis = System.currentTimeMillis()
    println(s"""${">" * 5} 开始scan数据: $beginTimeMillis""")
    JedisHelper.scan(jedis, file)
    println(s"""${">" * 5} 结束scan数据，耗时: ${(System.currentTimeMillis() - beginTimeMillis) / 1000} S""")

    val spark = SparkSession
      .builder()
      .appName("material_from_redis_to_hive")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val hiveLocation = s"""hdfs://emr-cluster2/warehouse/$dbName.db/$tableName/dt=$dt"""
    spark.read.json(s"file://$filePath")
      .select(
        $"redis_key",
        $"ideaid".as("ideaid"),
        $"type".as("type"),
        $"title".as("title"),
        $"description".as("description"),
        $"image".as("image").cast("string"),
        $"app".cast("string"),
        $"longDescription".as("long_description"),
        $"clickUrl".as("click_url"),
        $"impressionUrl".as("impression_url"),
        $"clickMonitorUrl".as("click_monitor_url"),
        $"subCate".as("sub_cate"),
        $"subCateValue".as("sub_cate_value"),
        $"qqqrUrl".as("qqqr_url"),
        $"btnText".as("btn_text"),
        $"characters".as("characters").cast("string"),
        $"video".as("video").cast("string"),
        $"StyleId".as("style_id"),
        $"motivateStyleId".as("motivate_style_id"),
        $"motivateStyleDescription".as("motivate_style_description"),
        $"deeplinkUrl".as("deeplink_url"),
        $"h5Url".as("h5_url"),
        $"secondTitle".as("second_title"),
        $"sourceMark".as("source_mark"),
        $"landingPageUrl".as("landing_page_url"),
        $"cardImages".as("card_images").cast("string"),
        $"wxNameSite".as("wx_name_site"),
        $"inspireVideoType1".as("inspire_video_type1"),
        $"baseTemplate".as("base_template"),
        $"siteId".as("site_id"),
        $"splitTitles".as("split_titles"),
        $"wildcard".as("wildcard")
      )
      .write.mode(SaveMode.Overwrite)
      .parquet(hiveLocation)

    spark.sql(
      s"""
         |alter table $dbName.$tableName
         |add if not exists partition(dt="$dt")
         |location "$hiveLocation"
      """.stripMargin)

    spark.close()
    println("done")
  }
}