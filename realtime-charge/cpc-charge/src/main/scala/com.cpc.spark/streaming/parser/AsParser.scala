package com.cpc.spark.streaming.parser

import com.cpc.spark.common.Ui

object AsParser {
  val None = (false, "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,0)
  //(true,sid,date,hour,ui.typed, idea_id, unit_id, plan_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click)
// 
    def parseAs(as : Ui) : 
    (Boolean,String,String,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int) 
    = {
      
      None
      
    }
}