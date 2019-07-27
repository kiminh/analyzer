package com.cpc.spark.log.anal

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable

object TopCTRIdeaV3 {
	def main(args: Array[String]): Unit = {
		if (args.length < 2) {
			System.err.println("Usage: GetUserProfile <day_before> <int> <table:string>")
			System.exit(-1)
		}
		import spark.implicits._
		Logger.getRootLogger.setLevel(Level.WARN)
		val dayBefore = args(0).toInt	// 10
		val dbTable = args(1)			// top_ctr_idea (adv_report库下面的)

		val dateBefore = Calendar.getInstance().add(Calendar.DATE, -dayBefore)

		val spark = SparkSession.builder()
    		.appName("top ctr idea")
    		.enableHiveSupport()
    		.getOrCreate()

		// 从hive表读取近10天的广告信息
		var adCtr: DataFrame = null
		for (i <- 0 until dayBefore) {
			val date = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().add(Calendar.DATE, -i))
			val stmt =
				s"""
				   |select adslot_type, ideaid, media_appsid, sum(isclick) as sum_click, sum(isshow) as sum_show
				   |from dl_cpc.cpc_basedata_union_events where `day` = "%s" and isshow = 1
				   |and adslot_id > 0 and ideaid > 0 and exp_style <> 510127
				   |group by adslot_type, ideaid, media_appsid
				 """.stripMargin.format(date)
			println(stmt)
			val ulog = spark.sql(stmt)
			if (adCtr == null) {
				adCtr = ulog
			} else {
				adCtr =adCtr.union(ulog)
			}
		}

		val adInfo = adCtr
			.groupBy("adslot_type", "ideaid", "media_appsid")
    		.agg(
				expr("sum(sum_click)").cast("int").alias("click"),
				expr("sum(sum_show)").cast("int").alias("show")
			)
    		.where("click>0 and show>1000")
    		.rdd
    		.map {
				r =>
					val ideaId = r.getAs[Int]("ideaid")
					val adSlotType = r.getAs[Int]("adslot_type")
					val mediaAppSid = r.getAs[String]("media_appsid")
					val click = r.getAs[Int]("click")
					val show = r.getAs[Int]("show")
					val mediaId = mediaAppSid match {
						case "80000001" | "80000002" => 1   // 趣头条
						case "80001098" | "80001292" | "80001539" | "80002480" | "80001011" | "80004786" | "80004787" => 4    // 米读
						case "80002819" => 5  // 段子
						case "80003865" => 6  // 趣键盘
						case _ => 2 // 联盟头部
					}

					val k = (adSlotType, ideaId, mediaAppSid)
					val v = AdInfo( adslot_type = adSlotType, idea_id = ideaId, media_id = mediaId, show = show, click = click )
					(k ,v)
			}
			.reduceByKey((x, y) => AdInfo(
				adslot_type = x.adslot_type,
				idea_id = x.idea_id,
				media_id = x.media_id,
				show = x.show + y.show,
				click = x.click + y.click
			))
			.toDF()

		println("总共取得的条数:" + adInfo.count())

		val conf = ConfigFactory.load()
		val dsn = conf.getString("mariadb.adv.url")
		val props = new Properties()
		props.put("user", conf.getString("mariadb.adv.user"))
		props.put("password", conf.getString("mariadb.adv.password"))
		props.put("driver", conf.getString("mariadb.adv.driver"))

		val ideaTable = spark.read.jdbc(dsn, "idea", props)
    		.select("id", "title", "image", "type", "video_id", "user_id", "category")
    		.filter("type in (1,2,3,4,6,7,8,9)")
    		.toDF()

		val userTable = spark.read.jdbc(dsn, "user", props)
    		.select("id", "belong")
    		.toDF()

		val resourceTable = spark.read.jdbc(dsn, "resource", props)
    		.select("id", "type", "remote_url")
    		.toDF()

		val s = ideaTable("id")
		val adIdeaDF = adInfo.join(ideaTable, ideaTable("id") === adInfo("idea_id"))
			.select("id", "adslot_type", "media_id", "show", "click", "title", "image", "type", "video_id", "user_id", "category")

		val adIdeaUserDF = adIdeaDF.join(userTable, userTable("id") === adIdeaDF("user_id"), "left")
			.select("belong")

		println("与idea表join后的条数:" + adIdeaDF.count())


	}






	private case class AdInfo(
		agent_id: Int = 0,
		user_id: Int = 0,
		idea_id: Int = 0,
		adclass: Int = 0,
		adslot_type: Int = 0,
		media_id: Int = 0,
		click: Int = 0,
		show: Int = 0
	) {}
}
