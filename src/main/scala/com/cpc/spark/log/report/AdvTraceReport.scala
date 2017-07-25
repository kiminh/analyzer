package com.cpc.spark.log.report

/**
  * Created by Roy on 2017/4/25.
  */

case class AdvTraceReport(
                           user_id: Int = 0,
                           plan_id: Int = 0,
                           unit_id: Int = 0,
                           idea_id: Int = 0,
                           date: String = "",
                           hour: String = "",
                           trace_type: String = "",
                           duration: Int = 0,
                           auto: Int = 0,
                           total_num:Int =0
                         )
