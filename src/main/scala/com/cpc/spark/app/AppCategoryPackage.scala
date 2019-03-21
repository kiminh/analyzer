package com.cpc.spark.app

import com.cpc.spark.util.GetAppCateFromBaiduUil
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.Source

object AppCategoryPackage {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val outdb = args(1)

    val appList = readAppList()
    val appCategory = appList.flatMap(name => GetAppCateFromBaiduUil.getAppCate(name, 50))
      .map(row => AppCategory(row._1, row._2))

    val spark = SparkSession.builder()
      .appName(s"AppCategoryPackage CoinUnionLog date = $date")
      .enableHiveSupport()
      .getOrCreate()

    val appCategoryRdd = spark.sparkContext.parallelize(appCategory)
    spark.createDataFrame(appCategoryRdd).repartition(1).createOrReplaceTempView("t")
    spark.sql(
      s"""
         | insert overwrite table $outdb.app_category_package partition(dt='$date')
         | select name,pak from t
       """.stripMargin)


  }

  def readAppList() = {
    val listapp = List("休闲游戏", "宝石消除", "动作射击", "儿童益智", "体育格斗", "经营策略", "跑酷竞速", "网络游戏", "扑克棋牌",
      "塔防守卫", "角色扮演", "波动游戏", "捕鱼游戏", "得力游戏", "模拟人生", "通信聊天", "美化手机", "生活服务", "常用工具",
      "新闻资讯", "金融理财", "育儿母婴", "影音图像", "网上购物", "阅读学习", "旅游出行", "性能优化", "社交网络", "办公软件",
      "资讯qtt", "娱乐消遣", "彩票APP", "短视频", "网贷")
    listapp
  }

  case class AppCategory(name: String, pak: String)

}
