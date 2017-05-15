package com.cpc.spark.log.parser

/**
  * Created by Roy on 2017/5/8.
  */
case class TraceReportLog(
                           user_id: Int = 0,
                           plan_id: Int = 0,
                           unit_id: Int = 0,
                           idea_id: Int = 0,
                           date: String = "",
                           hour: String = "",
                           trace_type: String = "",
                           duration: Int = 0
                   ) {
}

