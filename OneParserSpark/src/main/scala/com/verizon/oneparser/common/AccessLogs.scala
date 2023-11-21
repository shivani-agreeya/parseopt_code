package com.verizon.oneparser.common

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.ProcessLogCodes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.util.Properties

object AccessLogs extends ProcessLogCodes with LazyLogging{

  /*def main(args: Array[String]){

    val conf = new SparkConf().setAppName("My demo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val driver = "org.postgresql.Driver"
    //val url = "jdbc:postgresql://aws-us-east-1-portal.16.dblayer.com:10394/tennisdb?user=***&password=***"
    val url = "jdbc:postgresql://10.20.40.116:5470/dmat_qa?currentSchema=dmat_qa&user=postgres&password=postgres123"
    println("create")
    try {
      Class.forName(driver)
      val jdbcDF = sqlContext.read.format("jdbc").options(Map("url" -> url, "driver" -> driver, "dbtable" -> "(select id, creation_time, file_name, file_location from "+Constants.PG_OPLOGS_TBL_NAME+" where creation_time > now() - interval '1 day' and lower(state)='moved to hdfs') as "+Constants.PG_OPLOGS_TBL_NAME+"")).load()
      jdbcDF.show()
      println("success")
    } catch {
      case e : Throwable => {
        println(e.toString())
        println("Exception");
      }
    }
    sc.stop()
  }*/
  implicit val  rowEncoder1 = org.apache.spark.sql.Encoders.kryo[Row]
  def main(args : Array[String]) ={
    val spark = SparkSession.builder().getOrCreate()

    val conf = spark.sparkContext.getConf
    conf.setAppName("OneParserProcess")

    val idArr = Array(601,608,759,39,41)
    //val inbldInfo = getInBldInfo(spark,idArr.mkString(","))
  }

  /*def getInBldInfo(spark:SparkSession, ids:String) = {
    val prop = new Properties()
    prop.setProperty("user", Config.POSTGRES_DB_USER)
    prop.setProperty("password", Config.POSTGRES_DB_PWD)
    prop.setProperty("driver", Config.POSTGRES_DRIVER)
    val inbldTblMap = mutable.Map.empty[Int, String]
    var conn: Connection = null
    var inBldDs:Dataset[Row] = null
    try {
      Class.forName("org.postgresql.Driver").newInstance()
      conn = DriverManager.getConnection(Config.POSTGRES_DB_URL, prop)
      val inbldPrepStmt = conn.prepareStatement("select id, image_height from inbuilding where id in ("+ids+") and image_height is not null")
      val inbldRows = inbldPrepStmt.executeQuery()
      while (inbldRows.next()) {
        val imageHeight = inbldRows.getString("image_height")
        val inbldId = inbldRows.getInt("id")
        inbldTblMap(inbldId) = imageHeight
      }
      import spark.implicits._
      inBldDs = inbldTblMap.toSeq.toDF("inbldImgId","inbldImgHeight")
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      if(conn!=null)
      conn.close()
    }
    inBldDs
  }*/

  def fetchLogs(spark:SparkSession):Dataset[Row] = {

    val prop = new Properties()
    prop.setProperty("user", "postgres")
    prop.setProperty("password", "postgres123")
    prop.setProperty("driver", "org.postgresql.Driver")
    val logsTblMap = mutable.Map.empty[String, String]
    var conn: Connection = null
    var logTblDs:Dataset[Row] = null
    try {
      Class.forName("org.postgresql.Driver").newInstance()
      conn = DriverManager.getConnection("jdbc:postgresql://10.20.40.116:5470/dmat_qa?currentSchema=dmat_qa", prop)
      val dateFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val CURRENT_DATE: String = dateFormatter.format(new Date())
      val sql:String = "select file_name, file_location from "+Constants.PG_OPLOGS_TBL_NAME+" where cast(creation_time as date)='"+CURRENT_DATE+"' and (lower(state)='sequence done' or lower(state)='hive inprogress')"
      println("Current Date : "+CURRENT_DATE)
      println("Query: "+sql)
      val logsQuery = conn.prepareStatement(sql)

      val statesRows = logsQuery.executeQuery()
      while (statesRows.next()) {
        val fileName = statesRows.getString("file_name")
        val fileLocation = statesRows.getString("file_location")
        logsTblMap(fileName) = fileLocation
      }
      import spark.implicits._
      logTblDs = logsTblMap.toSeq.toDF("file_name", "file_location")
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      if(conn!=null)
      conn.close()
    }
    //logTblDs.show()
    logTblDs
  }
  /*def fetchLogs2(spark:SparkSession) ={
    var logTblDs:Dataset[Row] = null
    try {
      Class.forName(Config.POSTGRES_DRIVER)
      logTblDs = spark.sqlContext.read.format("jdbc").options(Map("url" -> Config.POSTGRES_CONN_URL, "driver" -> Config.POSTGRES_DRIVER, "dbtable" -> Constants.TEST_SEL_QUERY)).load()
      logTblDs.show()
    } catch {
      case e : Throwable => {
        println(e.toString())
        println("Exception");
      }
    }
    logTblDs
  }*/

}
