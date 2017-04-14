package com.cpc.spark.streaming.test

import com.cpc.spark.common.Event
import com.cpc.spark.common.Ui
import com.cpc.spark.streaming.tools.Utils
import data.Data

object TestMain {

  def main(args: Array[String]) {
    //     val line = "10.10.251.91 - - [28/Mar/2017:12:48:42 +0800] \"GET /show?CAAQMQ.zNPTdOLqUEmJUErleFmSeOn-e-rlUOPvcO5RwOc8hkhleFg-UENJhCPvU8hhIQD9XrBrnJYXnrLsPKHNPrNxNzHaeENKUOPJwONuwruzuKVLNyNuFNudOrruPNPhNVruwN3NNnrNBrmgNyNNLrmgNygNf8c-LijQhkyHDv5-ckRnwnU-mnUqUENlm8m-U8ePeEy1hqjQWCSHf9L6hCR6cqyAbOUKUaKXUOuszYyRDCeRD9y0LgHuOrruPrrguuNPFNmrNOrNHNhN HTTP/1.1\" 200 43 \"-\" \"Python-urllib/2.7\" \"-\" \"-\" 0.000"
    //     val event = Event.parse_show_log(line)
    //     println(event.event)

    //     val ui_log = "NOTICE:  2017-03-26 14:52:09 * CgAQmcrdxgUaEggAEgAaACICCgAqBAoAEgA6ACIdCgc1MDAwMTg4EAEaBgjwARDAAiAAKAAwADoAQAAqngEIARIICAQQBBgEIAAaJggBEiBmNjYxNmRjMTY5ZDA1MjU4ZDRiNTVjNDExMzg1NWJmNxgAIgZYaWFvbWkqCkhNLU5PVEUtMVMySERhbHZpay8xLjYuMCAoTGludXg7IFU7IEFuZHJvaWQgNC40LjQ7IEhNIE5PVEUgMVMgTUlVSS9WOC4wLjEuMC5LSEtDTkRHKToAQABKAFgAYABqADInCAIQARgCIAEoAjABOAFA0A9IAFAAWNEPigEAkgEAoAEAsgEAugEAQABKCggDEgYIABAAGABSEQgBEg0xMTQuOTIuNjYuMTQ3WjZzcF9tYWRob3VzZWlkOkMzQkM2MTA0QjQzMjNENTR8Y29tLm5oencuYmluZ2R1fDMwMHwxMDBaCXRlbmNlbnRrcGIJCJwBEAEYACADagwxMC4xMC4yNTEuNzdwAbgB5tjDAQ=="
    //     val as = Ui.parseData(ui_log)
    //     println(as.ui)

         val click_log = "NOTICE:  2017-04-10 11:09:41 * CgYIABAIGAESqwIIABIoMDE4MjYyYjBlMGYyOTA1MDE4MzgxN2Y1MzNiYmQ3MjAzMDQyMDk0Nxjz7qvHBSD+/ZezCyj17qvHBTD+/ZezCzobCgg4MDAwMDAwMRIAGgAqBzEwMjUxNTYwAToAQggInAEQERjwAUoQCBcQOBhLKFc4A0ABSABaAFIECAEQLWIYChJodHRwOi8vcGVhay1mcy5jbi8QAEoAcgQIARIAegCCAQCKAUcIARIICAUQABgCIAAaFQgBEg84NjA5MDEwMzEwOTk4NzAYATgASiAxY2YyNmRiZDZiYmZhZTA4MGU5Y2E4ZWIzYTM1YzFkYpIBDAgDEggIABAAGAAgAJoBAwiQTqIBCGNwYy1iajAyqgEIY3BjLWJqMDKgBgCoBrbQqd7W7r31WA=="
         val click = Event.parse_click_log(click_log)
         println(click.event)

    //    println(Utils.get_date_hour_from_time(1490613139000L))
//    val data_builder = Data.Log.newBuilder()
//    val field_builder = Data.Log.Field.newBuilder()
//    val map_builder = Data.Log.Field.Map.newBuilder()
//    val value_type = Data.ValueType.newBuilder()
    
    
  }
}