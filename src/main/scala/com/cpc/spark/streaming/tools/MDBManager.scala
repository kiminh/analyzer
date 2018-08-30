package com.cpc.spark.streaming.tools

import com.mchange.v2.c3p0.ComboPooledDataSource
import java.util.Properties
import java.io.InputStream
import java.io.FileInputStream
import java.io.File
import org.apache.spark.SparkFiles
import java.sql.Connection

class MDBManager(dbType: Int) extends Serializable {
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true);
  private val prop = new Properties()
  private var in: InputStream = _

  if(dbType == 0){
    in = getClass().getResourceAsStream("/c3p0.properties")
  }else if(dbType == 1){
    in = new FileInputStream(new File(SparkFiles.get("c3p0.properties")))
  }else if(dbType == 2){
    in = new FileInputStream(new File(SparkFiles.get("c3p0_db2.properties")))
  }else if(dbType == 3){
    in = new FileInputStream(new File(SparkFiles.get("c3p0_dsp.properties")))
  }else{
    in = getClass().getResourceAsStream("/c3p0.properties")
  }
  try {
    prop.load(in);
    cpds.setJdbcUrl(prop.getProperty("jdbcUrl").toString());
    cpds.setDriverClass(prop.getProperty("driverClass").toString());
    cpds.setUser(prop.getProperty("user").toString());
    cpds.setPassword(prop.getProperty("password").toString());
    cpds.setMaxPoolSize(Integer.valueOf(prop.getProperty("maxPoolSize").toString()));
    cpds.setMinPoolSize(Integer.valueOf(prop.getProperty("minPoolSize").toString()));
    cpds.setAcquireIncrement(Integer.valueOf(prop.getProperty("acquireIncrement").toString()));
    cpds.setInitialPoolSize(Integer.valueOf(prop.getProperty("initialPoolSize").toString()));
    cpds.setMaxIdleTime(Integer.valueOf(prop.getProperty("maxIdleTime").toString()));
  } catch {
    case ex: Exception => ex.printStackTrace()
  }
  def getConnection: Connection = {
    try {
      return cpds.getConnection();
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        null
    }
  }
}

object MDBManager {
  var mdbManager: MDBManager = _
  var mdbManager2: MDBManager = _
  var dspManager: MDBManager = _
  def getMDBManager(isLocal: Boolean): MDBManager = {
    synchronized {
      if (mdbManager == null) {
        mdbManager = new MDBManager(1) //older
      }
    }
    mdbManager
  }
  def getMDB2Manager(isLocal: Boolean): MDBManager = {
    synchronized {
      if (mdbManager2 == null) {
        mdbManager2 = new MDBManager(2)//new
      }
    }
    mdbManager2
  }
  def getDspMDBManager(isLocal: Boolean): MDBManager = {
    synchronized {
      if (dspManager == null) {
        dspManager = new MDBManager(3)//dsp
      }
    }
    dspManager
  }
}
