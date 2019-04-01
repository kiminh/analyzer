package com.cpc.spark.OcpcProtoType.aa_ab_report

import java.io.BufferedWriter
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}

object WriteCsv {
  def main(args: Array[String]): Unit = {

  }

  def write(dataDF: DataFrame, headName: List[String], filePath: String): Unit ={
    var out: FileOutputStream = null
    var osw: OutputStreamWriter = null
    var bw: BufferedWriter = null
    val rowList = dataDF.collect()
    try{
      new File(filePath)
      out = new FileOutputStream(filePath)
      osw = new OutputStreamWriter(out)
      bw = new BufferedWriter(osw)
      // 首先写title
      for(name <- headName){
        bw.write(name)
        bw.write(",")
      }
      bw.flush()
      bw.newLine()
      // 然后写数据
      for(row <- rowList){
        for(i <- 0 until row.length){
          bw.write(row.get(i).toString)
          bw.write(",")
        }
        bw.flush()
        bw.newLine()
      }
    }catch {
      case e: Exception => println(e)
    }finally {
      if(bw != null) bw.close()
      if(osw != null) osw.close()
      if(out != null) out.close()
    }
  }
}
