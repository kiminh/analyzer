package com.cpc.spark.log.report

/**
  * Created by Roy on 2017/4/25.
  */
case class MediaUvReport(
                      media_id: Int = 0,
                      adslot_id: Int = 0,
                      adslot_type: Int = 0,
                      uniq_user: Int = 0,
                      date: String = ""
                    ) {

  def sum(r: MediaUvReport): MediaUvReport = {
    copy(
      uniq_user = uniq_user + r.uniq_user
    )
  }

}
