package com.cpc.spark.log.anal

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{explode, split}

import scala.collection.mutable

object TopCTRIdeaV3 {
	def main(args: Array[String]): Unit = {
		if (args.length < 2) {
			System.err.println("Usage: GetUserProfile <day_before> <int> <table:string>")
			System.exit(-1)
		}

		Logger.getRootLogger.setLevel(Level.WARN)
		val dayBefore = args(0).toInt	// 10
		val dbTable = args(1)			// top_ctr_idea (adv_report库下面的)

		val spark = SparkSession.builder()
    		.appName("top ctr idea")
    		.enableHiveSupport()
    		.getOrCreate()


		// 从hive表读取近10天的广告信息
		var adCtr: DataFrame = null
		for (i <- 0 until dayBefore) {
			val calendar = Calendar.getInstance()
			calendar.add(Calendar.DATE, -i)
			val date = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime)
			val stmt =
				s"""
				   |select adslot_type, ideaid, media_appsid, sum(isclick) as sum_click, sum(isshow) as sum_show
				   |from dl_cpc.cpc_basedata_union_events
				   |where day = '$date'
				   |and isshow = 1
				   |and adslot_id > 0
				   |and ideaid > 0
				   |and exp_style <> 510127
				   |group by adslot_type, ideaid, media_appsid
				 """.stripMargin
			println(stmt)
			val ulog = spark.sql(stmt)
			if (adCtr == null) {
				adCtr = ulog
			} else {
				adCtr = adCtr.union(ulog)
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
			.map(x => (
				x._2.idea_id,
				x._2.media_id,
				x._2.adslot_type,
				x._2.click,
				x._2.show
			))

		println("总共取得的条数:" + adInfo.count())

		val unionDF = spark.createDataFrame(adInfo)
			.toDF("idea_id", "media_id", "adslot_type", "click", "show")
		unionDF.show()

		val conf = ConfigFactory.load()
		val dsn = conf.getString("mariadb.adv.url")
		val props = new Properties()
		props.put("user", conf.getString("mariadb.adv.user"))
		props.put("password", conf.getString("mariadb.adv.password"))
		props.put("driver", conf.getString("mariadb.adv.driver"))

		val ideaDF = spark.read.jdbc(dsn, "idea", props)
    		.select("id", "title", "image", "type", "video_id", "user_id", "category")
    		.filter("type in (1,2,3,4,6,7,8,9)")
    		.toDF()

		/*
		val calendar = Calendar.getInstance()
		calendar.add(Calendar.DATE, -dayBefore)
		val date = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime)
		val costDF = spark.read.jdbc(dsn, "cost", props)
    		.select("idea_id", "create_time")
    		.filter(s"""(cash_cost + coupon_cost) > 0 and create_time > '$date'""")
    		.toDF()
		*/

		val userDF = spark.read.jdbc(dsn, "user", props)
    		.select("id", "belong")
    		.toDF()

		val resourceDF = spark.read.jdbc(dsn, "resource", props)
    		.select("id", "remote_url")
    		.toDF()

		val unionXidea = unionDF.join(ideaDF, ideaDF("id") === unionDF("idea_id"))
			.select("adslot_type", "idea_id", "media_id", "show", "click", "title", "image", "type", "video_id", "user_id", "category")
			.toDF("adslot_type", "idea_id", "media_id", "show", "click", "title", "image", "type", "video_id", "user_id", "category")

		println("union与idea表join后的条数:" + unionXidea.count())

		val unionXideaXuser = unionXidea.join(userDF, unionXidea("user_id") === userDF("id"))
			.select("user_id", "adslot_type", "idea_id", "media_id", "belong", "show", "click", "title", "image", "type", "video_id", "category")
    		.toDF()

		unionXideaXuser.show()

		println("union与idea和user表join后的条数:" + unionXideaXuser.count())

		val explodeDF = unionXideaXuser.withColumn("image", explode(split(unionXideaXuser("image"), "[,]")))

		explodeDF.show()

		val explodeXresource = explodeDF.join(resourceDF, resourceDF("id") === explodeDF("image"), "left")
    		.select("user_id", "adslot_type", "idea_id", "media_id", "belong", "show", "click", "title", "remote_url", "type", "video_id", "category")
    		.toDF("user_id", "adslot_type", "idea_id", "media_id", "belong", "show", "click", "title", "image_url", "type", "video_id", "category")

		explodeXresource.show()


		val unionXideaXuserXresource = explodeXresource.join(resourceDF, resourceDF("id") === explodeXresource("video_id"), "left")
    		.select("user_id", "adslot_type", "idea_id", "media_id", "belong", "show", "click", "title", "image_url", "type", "remote_url", "category")
			.toDF("user_id", "adslot_type", "idea_id", "media_id", "belong", "show", "click", "title", "image_url", "mtype", "video_url", "category")

		unionXideaXuserXresource.show()

		val topIdeaRDD = unionXideaXuserXresource.rdd
    		.map {
				r =>
					var remote_url = r.getAs[String]("image_url")
					if (r.getAs[Int]("mtype") == 4) {
						remote_url = r.getAs[String]("video_url")
					}
					val user_id = r.getAs[Long]("user_id").intValue()
					val adslot_type = r.getAs[Int]("adslot_type")
					val idea_id = r.getAs[Int]("idea_id")
					val media_id = r.getAs[Int]("media_id")
					val agent_id = r.getAs[Int]("belong")
					val title = r.getAs[String]("title")
					val mtype = r.getAs[Int]("mtype")
					val click = r.getAs[Int]("click")
					val show = r.getAs[Int]("show")
					val adclass_1 = r.getAs[Int]("category")
					val adclass = adclass_1 / 1000000 * 1000000 + 100100

					val k = (adslot_type, idea_id, media_id, mtype)

					val v = TopIdea(
						user_id = user_id,
						idea_id = idea_id,
						agent_id = agent_id,
						media_id = media_id,
						adslot_type = adslot_type,
						title = title,
						mtype = mtype,
						images = remote_url,
						click = click,
						show = show,
						adclass = adclass,
						adclass_1 = adclass_1
					)

					(k ,v)
			}
    		.reduceByKey((x, y) => TopIdea(
				user_id = x.user_id,
				agent_id = x.agent_id,
				idea_id = x.idea_id,
				media_id = x.media_id,
				adslot_type = x.adslot_type,
				mtype = x.mtype,
				title = x.title,
				adclass = x.adclass,
				adclass_1 = x.adclass_1,
				//images = if (x.images==y.images) x.images else x.images + " " + y.images,
				images = if (x.images.contains(y.images)) x.images else x.images + " " + y.images,
				show = x.show + y.show,
				click = x.click + y.click
			))
    		.map( x => x._2)
			.filter(x => !(x.adclass == 118100100 && x.mtype == 4) &&
        				 !Seq(
							 1537507, 1540081, 1544700, 1544832, 1545016,
							 1545020, 1545025, 1545197, 1545200, 1545202,
							 1547033, 1549325, 1549327, 1549328, 1549329,
							 1549342, 1552330, 1552333, 1552336, 1552339,
							 1552340, 1552545, 1552551, 1552553, 1552562,
							 1552565, 1001028, 1501897
						 ).contains(x.user_id) &&
        				 !(
          					(110110100 == x.adclass && isStringContainsSomeChars(x.title, Seq("千", "万", "十万", "百万", "1000", "10000", "买车", "买房", "暴富", "马云"))) ||
            				(125100100 == x.adclass && isStringContainsSomeChars(x.title, Seq("千", "万", "十万", "百万", "1000", "10000", "买车", "买房", "暴富", "马云"))) ||
            				(100101109 == x.adclass && isStringContainsSomeChars(x.title, Seq("提现", "现金", "充值", "真金")))
          				 )
      		) //推荐素材去掉test用户 1001028 1501897； 过滤违规关键词


		val topIdeaDF = spark.createDataFrame(topIdeaRDD)
		topIdeaDF.show()

		val topCTRIdeaRDD = topIdeaRDD.map {
			x =>
				TopIdea(
					agent_id = x.agent_id,
					user_id = x.user_id,
					idea_id = x.idea_id,
					adslot_type = x.adslot_type,
					media_id = x.media_id,
					adclass = x.adclass,
					adclass_1 = x.adclass_1,
					title = x.title,
					mtype = x.mtype,
					images = x.images,
					ctr_score = (x.click.toDouble / x.show.toDouble * 1000000).toInt,
					from = "cpc_adv"
				)
		}.toLocalIterator.toSeq

		println("获取的总元素个数: " + topCTRIdeaRDD.length)

		val adSlotType: Array[Int] = Array(1,2,3,4,5,6,7)
		val rateMap: mutable.Map[Int, Double] = mutable.HashMap()
		val maxCtrMap: mutable.Map[Int, Int] = mutable.HashMap()
		val adSlotTypeMap: mutable.Map[Int, Long] = mutable.HashMap()

		for (i <- adSlotType) {
			val t = topCTRIdeaRDD.filter(_.adslot_type == i)
			println("adslot_type=" + i + "有 " + t.length + " 条")

			adSlotTypeMap.put(i, t.length)

			if (t.length > 0) {
				rateMap.put(i, t.length.toDouble / topCTRIdeaRDD.length.toDouble)
				maxCtrMap.put(i, t.map(_.ctr_score).max)
			}
		}

		println("占比: " + rateMap)
		for ((x, y) <- maxCtrMap) {
			println("adslot_type: " + x + "; max_ctr: " + y)
		}
		for (i <- adSlotType) {
		}

		var topIdeaRDD2: Seq[TopIdea] = Seq()
		var topCtrIdeaData = mutable.Seq[TopIdea]()

		for (i <- adSlotType) {
			val needSize = (80000 * rateMap.getOrElse[Double](i, 0.0)).toInt
			val actualSize = adSlotTypeMap.getOrElse[Long](i, 0).toInt
			println("adslot_type=" + i + " 从union表实际取得 " + actualSize + " 条")

			if (needSize > actualSize || actualSize < 500) {
				topIdeaRDD2 = topCTRIdeaRDD
						.filter(_.adslot_type == i)
						.sortWith(_.ctr_score > _.ctr_score)
						.take(actualSize)
			} else {
				topIdeaRDD2 = topCTRIdeaRDD
    					.filter(_.adslot_type == i)
    					.sortWith(_.ctr_score > _.ctr_score)
    					.take(needSize)
			}
			println("adslot_type=" + i + " 放到adv_report有 " + topIdeaRDD2.length + " 条")
			topCtrIdeaData = topCtrIdeaData ++ topIdeaRDD2
		}

		val advReportConf = ConfigFactory.load()
		val advReportDsn = advReportConf.getString("mariadb.adv_report.url")
		val advReportProps = new Properties()
		advReportProps.put("user", advReportConf.getString("mariadb.adv_report.user"))
		advReportProps.put("password", advReportConf.getString("mariadb.adv_report.password"))
		advReportProps.put("driver", advReportConf.getString("mariadb.adv_report.driver"))

		// truncate table
		try {
			Class.forName(props.getProperty("driver"))
			val conn = DriverManager.getConnection(
				advReportDsn,
				advReportProps.getProperty("user"),
				advReportProps.getProperty("password")
			)
			val stmt = conn.createStatement()
			val sql =
				s"""
				   |TRUNCATE TABLE adv_report.%s
				 """.stripMargin.format(dbTable)
			stmt.executeUpdate(sql)
			stmt.close()
			conn.close()
		} catch {
			case e: Exception => println("truncate table failed:" + e)
		}

		spark.createDataFrame(topCtrIdeaData)
    		.drop("adslot_type", "show", "click")
    		.show()

		spark.createDataFrame(topCtrIdeaData)
			.drop("adslot_type", "show", "click")
    		.write
    		.mode(SaveMode.Append)
    		.jdbc(advReportDsn, "adv_report." + dbTable, advReportProps)

		// 手动插入素材
		try {
			Class.forName(advReportProps.getProperty("driver"))
			val conn = DriverManager.getConnection(
				advReportDsn,
				advReportProps.getProperty("user"),
				advReportProps.getProperty("password"))
			val stmt = conn.createStatement()
			val sql_insert =
				s"""
				   |INSERT INTO `adv_report`.%s (`agent_id`, `user_id`, `idea_id`, `adclass`, `adclass_1`, `title`, `mtype`, `images`, `ctr_score`, `from`) VALUES (1000003, 1000002, '1953639', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (1).gif', 21848, 'cpc_adv'), (1000003, 1000002, '1607163', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (1).jpg', 71297, 'cpc_adv'), (1000003, 1000002, '1933010', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (10).gif', 112170, 'cpc_adv'), (1000003, 1000002, '1569289', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (11).gif', 147245, 'cpc_adv'), (1000003, 1000002, '1933013', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (12).gif', 106979, 'cpc_adv'), (1000003, 1000002, '1953639', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (13).gif', 148176, 'cpc_adv'), (1000003, 1000002, '1607162', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (14).gif', 58631, 'cpc_adv'), (1000003, 1000002, '1648200', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (15).gif', 55726, 'cpc_adv'), (1000003, 1000002, '1594572', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (16).gif', 3962, 'cpc_adv'), (1000003, 1000002, '1594614', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (17).gif', 131729, 'cpc_adv'), (1000003, 1000002, '1714728', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (18).gif', 80458, 'cpc_adv'), (1000003, 1000002, '1648200', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (19).gif', 6200, 'cpc_adv'), (1000003, 1000002, '1594614', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (2).gif', 49995, 'cpc_adv'), (1000003, 1000002, '1594572', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (2).jpg', 58845, 'cpc_adv'), (1000003, 1000002, '1646182', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (20).gif', 133804, 'cpc_adv'), (1000003, 1000002, '1555287', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (21).gif', 42934, 'cpc_adv'), (1000003, 1000002, '1813161', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (22).gif', 131831, 'cpc_adv'), (1000003, 1000002, '1607160', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (23).gif', 54403, 'cpc_adv'), (1000003, 1000002, '1607162', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (24).gif', 44261, 'cpc_adv'), (1000003, 1000002, '1607162', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (25).gif', 13918, 'cpc_adv'), (1000003, 1000002, '1813169', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (26).gif', 14341, 'cpc_adv'), (1000003, 1000002, '1714728', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (27).gif', 139885, 'cpc_adv'), (1000003, 1000002, '1569114', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (28).gif', 110873, 'cpc_adv'), (1000003, 1000002, '1813169', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (29).gif', 65812, 'cpc_adv'), (1000003, 1000002, '2326306', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (3).gif', 122163, 'cpc_adv'), (1000003, 1000002, '1823313', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (3).jpg', 99336, 'cpc_adv'), (1000003, 1000002, '1594572', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (30).gif', 89187, 'cpc_adv'), (1000003, 1000002, '1594572', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (31).gif', 138798, 'cpc_adv'), (1000003, 1000002, '1646182', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (32).gif', 126034, 'cpc_adv'), (1000003, 1000002, '1569292', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (33).gif', 133063, 'cpc_adv'), (1000003, 1000002, '1594614', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (34).gif', 92821, 'cpc_adv'), (1000003, 1000002, '1714728', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (35).gif', 33353, 'cpc_adv'), (1000003, 1000002, '1648200', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (36).gif', 133961, 'cpc_adv'), (1000003, 1000002, '1813169', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (37).gif', 100940, 'cpc_adv'), (1000003, 1000002, '2144124', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (38).gif', 146980, 'cpc_adv'), (1000003, 1000002, '1594740', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (39).gif', 89101, 'cpc_adv'), (1000003, 1000002, '1615483', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (4).gif', 54233, 'cpc_adv'), (1000003, 1000002, '1993322', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (40).gif', 22142, 'cpc_adv'), (1000003, 1000002, '1823313', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (41).gif', 145392, 'cpc_adv'), (1000003, 1000002, '1813161', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (42).gif', 143942, 'cpc_adv'), (1000003, 1000002, '1555287', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (43).gif', 120704, 'cpc_adv'), (1000003, 1000002, '1569289', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (44).gif', 15070, 'cpc_adv'), (1000003, 1000002, '1594614', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (45).gif', 142998, 'cpc_adv'), (1000003, 1000002, '1569191', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (46).gif', 111307, 'cpc_adv'), (1000003, 1000002, '1607161', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (47).gif', 44012, 'cpc_adv'), (1000003, 1000002, '1615483', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (48).gif', 57153, 'cpc_adv'), (1000003, 1000002, '1953639', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (49).gif', 70206, 'cpc_adv'), (1000003, 1000002, '1607163', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (5).gif', 59875, 'cpc_adv'), (1000003, 1000002, '1594614', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (50).gif', 95314, 'cpc_adv'), (1000003, 1000002, '1813169', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (51).gif', 119052, 'cpc_adv'), (1000003, 1000002, '1813169', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (52).gif', 56878, 'cpc_adv'), (1000003, 1000002, '1944679', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (53).gif', 115883, 'cpc_adv'), (1000003, 1000002, '1677143', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (54).gif', 6554, 'cpc_adv'), (1000003, 1000002, '1813189', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (55).gif', 87389, 'cpc_adv'), (1000003, 1000002, '1944791', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (56).gif', 118144, 'cpc_adv'), (1000003, 1000002, '1648200', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (57).gif', 62397, 'cpc_adv'), (1000003, 1000002, '1944791', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (58).gif', 49610, 'cpc_adv'), (1000003, 1000002, '1531704', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (59).gif', 73881, 'cpc_adv'), (1000003, 1000002, '1813169', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (6).gif', 22529, 'cpc_adv'), (1000003, 1000002, '1531704', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (60).gif', 38291, 'cpc_adv'), (1000003, 1000002, '2326306', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (61).gif', 75621, 'cpc_adv'), (1000003, 1000002, '1933010', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (62).gif', 126823, 'cpc_adv'), (1000003, 1000002, '1944791', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (63).gif', 119973, 'cpc_adv'), (1000003, 1000002, '1933010', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (64).gif', 47111, 'cpc_adv'), (1000003, 1000002, '1944679', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (65).gif', 6459, 'cpc_adv'), (1000003, 1000002, '2326306', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (66).gif', 4899, 'cpc_adv'), (1000003, 1000002, '1531704', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (67).gif', 61837, 'cpc_adv'), (1000003, 1000002, '1607160', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (68).gif', 109574, 'cpc_adv'), (1000003, 1000002, '1953639', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (69).gif', 55667, 'cpc_adv'), (1000003, 1000002, '1607163', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (7).gif', 127761, 'cpc_adv'), (1000003, 1000002, '1714728', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (70).gif', 100649, 'cpc_adv'), (1000003, 1000002, '1993322', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (71).gif', 30624, 'cpc_adv'), (1000003, 1000002, '1607160', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (72).gif', 144934, 'cpc_adv'), (1000003, 1000002, '1594785', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (73).gif', 113500, 'cpc_adv'), (1000003, 1000002, '1594785', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (74).gif', 128220, 'cpc_adv'), (1000003, 1000002, '1594614', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (75).gif', 36377, 'cpc_adv'), (1000003, 1000002, '1569289', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (76).gif', 128772, 'cpc_adv'), (1000003, 1000002, '2326306', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (77).gif', 102770, 'cpc_adv'), (1000003, 1000002, '1953639', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (78).gif', 78947, 'cpc_adv'), (1000003, 1000002, '1607162', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (79).gif', 8472, 'cpc_adv'), (1000003, 1000002, '1646182', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (8).gif', 86996, 'cpc_adv'), (1000003, 1000002, '1933013', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (80).gif', 129537, 'cpc_adv'), (1000003, 1000002, '1594785', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (81).gif', 56053, 'cpc_adv'), (1000003, 1000002, '1607160', 132101100, 132101100, '', 7, 'http://static.aiclk.com/uploads/online/201806057 (9).gif', 55698, 'cpc_adv')
        """.stripMargin.format(dbTable)
			stmt.executeUpdate(sql_insert)
			stmt.close()
			conn.close()
		} catch {
			case e: Exception => println("insert table failed : " + e);
		}
	} // main end


	def isStringContainsSomeChars(str: String, seq: Seq[String]): Boolean = {
		for (s <- seq) {
			if (str.contains(s)) {
				return true
			}
		}
		false
	}

	private case class TopIdea(
								  agent_id: Int = 0,
								  user_id: Int = 0,
								  idea_id: Int = 0,
								  adclass: Int = 0,
								  adclass_1: Int = 0, //一级行业
								  adslot_type: Int = 0,
								  media_id: Int = 0,
								  title: String = "",
								  mtype: Int = 0,
								  images: String = "",
								  ctr_score: Int = 0,
								  from: String = "",
								  click: Int = 0,
								  show: Int = 0
							  ) {}



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
