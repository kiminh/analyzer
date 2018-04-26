package com.cpc.spark.log.report

/**
  * Created by zhaogang on 2017/5/24.
  */
case class TagReport(
                      tag: Int = 0,
                      date: String = "",
                      device_num: Int = 0,
                      uv: Int = 0,
                      pv: Int = 0,
                      click: Int = 0
                    )
