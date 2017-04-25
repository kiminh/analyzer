package com.cpc.spark.log.anal

/**
  * Created by Roy on 2017/4/25.
  */
case class MediaUvReport (
                           media_id: Int = 0,
                           adslot_id: Int= 0,
                           uniq_user: Int = 0,
                           date: String = "",
                           hour: Int = 0
                         ) {

  val key = "%s-%d".format(media_id, adslot_id)

  def sum(r: MediaUvReport): MediaUvReport = {
    copy(
      uniq_user = r.uniq_user + uniq_user
    )
  }
}

