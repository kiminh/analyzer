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
                           hour: Int = 0,
                           trace_type: String = "",
                           duration: Int = 0,
                           count: Int = 0
                         ) {

  val key = "%d-%d-%d-%d-%d".format(user_id, plan_id, unit_id, idea_id, date, hour, trace_type, duration)
}
