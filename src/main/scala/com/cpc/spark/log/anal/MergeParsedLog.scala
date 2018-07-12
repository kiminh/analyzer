package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.Date


/**
  * /warehouse/dl_cpc.db/logparsed_cpc_search_minute
  * /warehouse/dl_cpc.db/logparsed_cpc_click_minute
  * /warehouse/dl_cpc.db/logparsed_cpc_show_minute
  * (searchid,ideaid)
  */
object MergeParsedLog {

  //数据源根目录
  var srcRoot = "/warehouse/dl_cpc.db"
  var prefix = ""
  var suffix = ""

  //时间格式
  val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd/HH")

  //当前日期
  var g_date = new Date()



}
