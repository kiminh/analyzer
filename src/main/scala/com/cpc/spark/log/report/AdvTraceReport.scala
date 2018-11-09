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
                           trace_op1: String = "",
                           duration: Int = 0,
                           auto: Int = 0,
                           total_num: Int = 0,
                           impression: Int = 0,
                           click: Int = 0
                         ) {

  val key = (this.user_id, this.plan_id, this.unit_id, this.idea_id, this.date, this.hour, this.trace_type, this.trace_op1, this.duration, this.auto)

  def sum(o: AdvTraceReport): AdvTraceReport = {
    this.copy(
      total_num = this.total_num + o.total_num,
      impression = this.impression + o.impression,
      click = this.click + o.click
    )
  }

}
