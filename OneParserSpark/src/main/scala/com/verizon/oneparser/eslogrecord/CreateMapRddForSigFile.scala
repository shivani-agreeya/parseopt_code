package com.verizon.oneparser.eslogrecord

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.common.Constants
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.ProcessDMATRecords.{get0x2501KPIs, getFreqRsrp0x2157KPIs, getLocValues, getNrServingCellKPIs, isValidNumeric, round}

import scala.collection.immutable.Nil
import scala.collection.mutable.Map
import scala.util.control.Breaks.{break, breakable}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import com.esri.webmercator._
import com.verizon.oneparser.parselogs.LogRecordParser5G.parseFloat
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object CreateMapRddForSigFile extends Serializable with LazyLogging {
  implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
  implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
  val getTimestamp: (String => Option[Timestamp]) = s => s match {
    case "" => None
    case _ => {
      val format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")
      format.setTimeZone(TimeZone.getTimeZone("UTC"))
      Try(new Timestamp(format.parse(s).getTime)) match {
        case Success(t) => Some(t)
        case Failure(_) => None
      }
    }
  }
  val getTimestampPG: (String => Option[Timestamp]) = s => s match {
    case "" => None
    case _ => {
      val format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")
      Try(new Timestamp(format.parse(s).getTime)) match {
        case Success(t) => Some(t)
        case Failure(_) => None
      }
    }
  }
  def getCurrentdateTimeStamp: Timestamp = {
    val today = Calendar.getInstance()
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now: String = timeFormat.format(today.getTime)
    val currentTime = java.sql.Timestamp.valueOf(now)
    currentTime
  }
  def getSigFileMapRdd(joinedAllTables: DataFrame, repartitionNo: Int,reportFlag: Boolean, commonConfigParams:CommonConfigParameters): Dataset[Map[String, Any]] = {
    val mapRDD = joinedAllTables.repartition(repartitionNo.toInt).mapPartitions(iter => {
      iter.map(row=> {
        val fileName = row.get(row.fieldIndex("fileName")).toString
        val fileNameWithExt = Constants.getDlfFromSeq(fileName)
        var fileType:String = ""
        var x: String = ""
        var y: String = ""
        x = if (!row.isNullAt(row.fieldIndex("Longitude"))) {
          row.getString(row.fieldIndex("Longitude"))
        } else if (!row.isNullAt(row.fieldIndex("Longitude2"))) {
          row.getString(row.fieldIndex("Longitude2"))
        } else ""
        y = if (!row.isNullAt(row.fieldIndex("Latitude"))) {
          row.getString(row.fieldIndex("Latitude"))
        } else if (!row.isNullAt(row.fieldIndex("Latitude2"))) {
          row.getString(row.fieldIndex("Latitude2"))
        }  else ""
        //TODO dont execute for reports
        if(!reportFlag){
          if (x.isEmpty) {
            x = if (!row.isNullAt(row.fieldIndex("previousLongitude"))) {
              row.getString(row.fieldIndex("previousLongitude"))
            } else if (!row.isNullAt(row.fieldIndex("previousLongitude2"))) {
              row.getString(row.fieldIndex("previousLongitude2"))
            } else ""
          }
          if (y.isEmpty) {
            y = if (!row.isNullAt(row.fieldIndex("previousLatitude"))) {
              row.getString(row.fieldIndex("previousLatitude"))
            } else if (!row.isNullAt(row.fieldIndex("previousLatitude2"))) {
              row.getString(row.fieldIndex("previousLatitude2"))
            }  else ""
          }
        }
        var mx: java.lang.Double = 0.0
        if (x != null && !x.isEmpty) {
          mx = if (x.toDouble > -180 && x.toDouble < 180) x.toDouble toMercatorX else mx
        }
        var my: java.lang.Double = 0.0
        if (y != null && !y.isEmpty) {
          my = if(y.toDouble > -90 && y.toDouble <90) y.toDouble toMercatorY else my
        }
        fileType = fileNameWithExt.split('.')(1).toUpperCase
        val isSigFile = fileType.toLowerCase.equals(Constants.CONST_SIG)
        val vz_regions: String = if (!row.isNullAt(row.fieldIndex("vz_regions"))) row.getString(row.fieldIndex("vz_regions")) else null
        val CountryCode: String = if (!row.isNullAt(row.fieldIndex("CountryCode"))) row.getString(row.fieldIndex("CountryCode")) else null
        val stusps: String = if (!row.isNullAt(row.fieldIndex("stusps"))) row.getString(row.fieldIndex("stusps")) else null
        val countyns: String = if (!row.isNullAt(row.fieldIndex("countyns"))) row.getString(row.fieldIndex("countyns")) else null
        //        val firstRecordFlag: Integer = if (!row.isNullAt(row.fieldIndex("rnFR"))) row.getInt(row.fieldIndex("rnFR")) else null
        //        val lastRecordFlag: Integer = if (!row.isNullAt(row.fieldIndex("rnLR"))) row.getInt(row.fieldIndex("rnLR")) else null
        var traceScanner0xFFF4Map: scala.collection.mutable.Map[String,Any] = null
        var autoTestMap: scala.collection.mutable.Map[String, Any] = null

        // Irrelevant for SIG files
        if(!isSigFile) {
          traceScanner0xFFF4Map = getTraceScannerKPIs(row)
        }
        //        logger.info(s" traceScanner0xFFF4Map=$traceScanner0xFFF4Map")
        if(!isSigFile) {
          autoTestMap = getAutoTestTestKpis(row)
        }
        val  mapWithoutMetadata = Map("ObjectId" -> scala.util.Random.nextInt(),
          "TestId" -> row.getInt(row.fieldIndex("testId")),
          "FileName" -> Constants.getDlfFromSeq(fileName)) ++
          ((if (!reportFlag && row.getString(row.fieldIndex("dateWithoutMillis")) != null) Map("TimeStamp" -> getTimestamp(row.getString(row.fieldIndex("dateWithoutMillis"))).getOrElse(),
            "internal_index" -> getIndexName(row.getString(row.fieldIndex("dateWithoutMillis")),
              if (!isSigFile) Constants.CONST_VZKPIS else fileNameWithExt.split('.')(1).toUpperCase,
              Constants.ES_NON_VMAS_ROUTINE,
              if (!row.isNullAt(row.fieldIndex("deviceOS"))) row.getString(row.fieldIndex("deviceOS")) else StringUtils.EMPTY)) else Nil) ++
            (if (fileNameWithExt != null) Map("Type" -> fileType, "internal_type" -> getTypeName(fileType)) else Nil) ++
            /*(if(fileType != "SIG") Map("TestId" -> row.getInt(row.fieldIndex("testId"))) else Nil) ++*/
            (if (fileType != "SIG") Map("ProcessedTime" -> getCurrentdateTimeStamp) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("deviceOS"))) Map("DeviceOS" -> row.getString(row.fieldIndex("deviceOS"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("emailID"))) Map("EmailId" -> row.getString(row.fieldIndex("emailID"))) else Map("EmailId" -> "concessions@vzwdt.com")) ++
            (if (!row.isNullAt(row.fieldIndex("first_Name"))) Map("FirstName" -> row.getString(row.fieldIndex("first_Name"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("last_Name"))) Map("LastName" -> row.getString(row.fieldIndex("last_Name"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("DmUser"))) Map("DmUser" -> row.getInt(row.fieldIndex("DmUser"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("mdn_logs"))) Map("MDN" -> row.getString(row.fieldIndex("mdn_logs"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("imei_logs"))) Map("Imei" -> row.getString(row.fieldIndex("imei_logs"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("technology_logs"))) Map("Technology" -> row.getString(row.fieldIndex("technology_logs"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("carrierName"))) Map("CarrierName" -> row.getString(row.fieldIndex("carrierName"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("modelName_logs"))) Map("ModelName" -> row.getString(row.fieldIndex("modelName_logs"))) else Nil)) ++
          (if (mx != 0 && my != 0) getLocValues(mx, my, x, y) else Nil) ++
          (if (mx != 0 && my != 0) Map("Outlier" -> 0) else Nil) ++
          // Irrelevant for VZ KPIs
          (if(isSigFile) {
            (if (mx != 0 && my != 0) { Map("DriveScanner" -> 1) } else if(mx==0.0 && my==0.0) { Map("DriveScanner" -> 0) } else Nil)
          } else Nil) ++
          // (if (mx != 0 && my != 0) { Map("DriveScanner" -> 1) } else if(mx==0.0 && my==0.0) { Map("DriveScanner" -> 0) } else Nil) ++
          (if(traceScanner0xFFF4Map != null && traceScanner0xFFF4Map.size > 0) traceScanner0xFFF4Map else Nil) ++
          (if (autoTestMap != null && autoTestMap.nonEmpty) autoTestMap else Nil) ++
          //          (if (firstRecordFlag != null && firstRecordFlag == 1 && fileType != "SIG") Map("firstRecord" -> true) else Nil) ++
          //          (if (lastRecordFlag != null && lastRecordFlag == 1 && fileType != "SIG") Map("lastRecord" -> true) else Nil) ++
          (if (vz_regions != null) Map("VzwRegions" -> vz_regions) else Nil) ++
          (if (CountryCode != null) {
            (if(CountryCode == "244") {
              Map("Country" -> "USA")
            } else if (vz_regions != null) {
              Map("Country" -> vz_regions)
            } else Nil)
          } else Nil) ++
          (if (stusps != null) Map("StateCode" -> stusps) else Nil) ++
          (if (countyns != null) Map("County" -> countyns) else Nil)
        val MapWithoutMetaData = mapWithoutMetadata  ++ hbflexKpisFromSig(row:Row)
        val mapWithMetadata =  mapWithoutMetadata ++ Map("document_metadata" -> mapWithoutMetadata.keys.toList)
        import org.json4s.jackson.Serialization
        implicit val formats = org.json4s.DefaultFormats
        val jsonStringWithoutMetaData = Serialization.write(mapWithoutMetadata -- Set("internal_type", "internal_index"))
        mapWithMetadata ++ Map("jsonString_WriteToHdfs" -> jsonStringWithoutMetaData)
      })
    })
    mapRDD
  }

  def hbflexKpisFromSig(row:Row) : scala.collection.mutable.Map[String, Any] ={

    var refPower_sig: Array[(String, String)] = null
    var refQuality_sig: Array[(String, String)] = null
    var cinr_sig: Array[(String, String)] = null
    var cellIdSig: List[String] = null
    var refPowerSig: List[String]  = null
    var refQualitySig: List[String]  = null
    var cinrSig: List[String]  = null
    var frequencySig: Integer =  null
    var channelNumberSig: Integer = null
    var bandwidthSig: String = null
    var hexlogcodeSig: String = null


    var cinr0x2406Sig: String = null
    var cpType0x2406Sig: String = null
    var totalRsPower0x2406Sig: String = null
    var cellId0x2406Sig: String = null
    var secondarySyncQuality0x2406: String = null
    var secondarySyncPower0x2406: String = null
    var primarySyncQuality0x2406: String = null
    var primarySyncPower0x2406: String = null

   /* var cinr0x2406_sig: Array[(String, String)] = null
    var cpType0x2406_sig: Array[(String, String)] = null
    var totalRsPower0x2406_sig: Array[(String, String)] = null
    var secondarySyncQuality0x2406_sig: Array[(String, String)] = null
    var secondarySyncPower0x2406_sig: Array[(String, String)] = null
    var primarySyncQuality0x2406_sig: Array[(String, String)] = null
    var primarySyncPower0x2406_sig: Array[(String, String)] = null*/



    var frequency0x2406_sig: Integer = null
    var channelNumber0x2406_sig:String = null
    var cinr0x2406_sig: List[String] = null
    var cellID0x2406_sig: List[String] = null
    var cpType0x2406_sig: List[String] = null
    var totalRsPower0x2406_sig: List[String] = null
    var secondarySyncQuality0x2406_sig: List[String] = null
    var secondarySyncPower0x2406_sig: List[String] = null
    var primarySyncQuality0x2406_sig: List[String] = null
    var primarySyncPower0x2406_sig: List[String] = null


    var cinr0x2408Sig: String = null
    var cellId0x2408Sig: String = null
    var totalRsPower0x2408_collectedSig: String = null
    var overallPower0x2408_collectedSig: String = null
    var overallQuality0x2408_collectedSig: String = null
    var cpType0x2408_collectedSig: String = null
    var antQuality0x2408_collectedSig: String = null
    var antPower0x2408_collectedSig: String = null
    var numOfAntennas0x2408_Sig: String =null
    var frequency0x2408_Sig: String = null
    var bandwidth0x2408_sig: String = null
    var numOfSignals0x2408_Sig: String = null
    var channelPower0x2408_sig: String = null
    var numberOfTxAntennaPorts0x2408_Sig: String = null
    var cinr0x2408_sig: Array[(String, String)] = null
    var cellId0x2408_sig: Array[(String, String)] = null
    var totalRsPower0x2408_sig: Array[(String, String)] = null
    var overallPower0x2408_sig: Array[(String, String)] = null
    var overallQuality0x2408_sig: Array[(String, String)] = null
    var cpType0x2408_sig: Array[(String, String)] = null
    var antQuality0x2408_sig: Array[(String, String)] = null
    var antPower0x2408_sig: Array[(String, String)] = null
    var numOfAntennas0x2408_sig: Array[(String, String)] = null


    var frequency0x240D_Sig: String =null
    var channelNumber0x240D_Sig: String = null
    var bandwidth0x240D_Sig: String = null
    var carrierRSSI0x240D_sig: String = null
    var numOfSignals0x240D_sig: String = null
    var numOfBlocks0x240D_sig: String = null
    var CINR0x240D_collectedSig: String = null
    var cellId0x240DSig: String = null
    var rsrp0x240D_collectedSig: String = null
    var rsrq0x240D_collectedSig: String = null
    var numOfAntennaPorts0x240D_collectedSig: String = null
    var CINR0x240D_sig: Array[(String, String)] = null
    var cellId0x240D_sig: Array[(String, String)] = null
    var rsrp0x240D_sig:Array[(String,String)] = null
    var rsrq0x240D_sig:Array[(String,String)] = null
    var numOfAntennaPorts0x240D_sig: Array[(String, String)] = null


    var pci0x2501_collectedSig: List[String] = null
    var frequency0x2501_collectedSig: List[String]  = null
    var subCarrierSpacing0x2501_collectedSig: List[String]  = null
    var numbeams0x2501_collectedSig: List[String]  = null
    var beamIndex0x2501_collectedSig: List[String]  = null
    var pssBrsrp0x2501_collectedSig: List[String]  = null
    var pssBrsrq0x2501_collectedSig: List[String]  = null
    var pssCinr0x2501_collectedSig: List[String]  = null
    var sssBrsrp0x2501_collectedSig: List[String]  = null
    var sssBrsrq0x2501_collectedSig: List[String]  = null
    var sssCinr0x2501_collectedSig: List[String]  = null
    var sssDelay0x2501_collectedSig: List[String]  = null
    var ssbBrsrp0x2501_collectedSig: List[String]  = null
    var ssbBrsrq0x2501_collectedSig: List[String]  = null
    var ssbCinr0x2501_collectedSig: List[String]  = null
    var numSSBurstDataBlocks0x2501_sig: String = null
    var channelNumber0x2501_sig: String = null
    var ssbRSSI0x2501_sig: String = null
    var pci0x2501_Sig: Array[(String, String)] = null
    var frequency0x2501_Sig: Array[(String, String)] = null
    var subCarrierSpacing0x2501_Sig: Array[(String, String)] = null
    var numbeams0x2501_Sig: Array[(String, String)] = null
    var beamIndex0x2501_Sig: Array[(String, String)] = null
    var pssBrsrp0x2501_Sig: Array[(String, String)] = null
    var pssBrsrq0x2501_Sig: Array[(String, String)] = null
    var pssCinr0x2501_Sig: Array[(String, String)] = null
    var sssBrsrp0x2501_Sig: Array[(String, String)] = null
    var sssBrsrq0x2501_Sig: Array[(String, String)] = null
    var sssCinr0x2501_Sig: Array[(String, String)] = null
    var sssDelay0x2501_Sig: Array[(String, String)] = null
    var ssbBrsrp0x2501_Sig: Array[(String, String)] = null
    var ssbBrsrq0x2501_Sig: Array[(String, String)] = null
    var ssbCinr0x2501_Sig: Array[(String, String)] = null

    var freqRsrp0x2157: List[(Int, Double)] = null

    var freq0x2157_Sig: String = null
    var centerFreq0x240C:Integer = null
    var numSignals0x240C:Integer = null
    var numAntennas0x240C:List[String] = null
    var pci0x240C:List[String] = null
    var rsrp0x240C:List[String] = null
    var rsrq0x240C:List[String] = null
    var rscinr0x240C:List[String] = null
    var antennaRsrp0x240C:List[String] = null
    var antennaRsrq0x240C:List[String] = null
    var antennaCinr0x240C:List[String] = null

    val fileName = row.get(row.fieldIndex("fileName")).toString
    val fileNameWithExt = Constants.getDlfFromSeq(fileName)
    val fileType = fileNameWithExt.split('.')(1).toUpperCase

    var sigCellIdKpis = scala.collection.mutable.Map[String, Any]()

  if(fileType=="SIG")  {

    frequencySig = if (!row.isNullAt(row.fieldIndex("freq0x240b"))) row.getInt(row.fieldIndex("freq0x240b")) else null
    cellIdSig = if (!row.isNullAt(row.fieldIndex("cellId0x240b"))) row.getSeq(row.fieldIndex("cellId0x240b")).toList else null
    refPowerSig = if (!row.isNullAt(row.fieldIndex("primaryRsrp0x240b"))) row.getSeq(row.fieldIndex("primaryRsrp0x240b")).toList  else null
    refQualitySig = if (!row.isNullAt(row.fieldIndex("primaryRsrq0x240b"))) row.getSeq(row.fieldIndex("primaryRsrq0x240b")).toList  else null
    cinrSig = if (!row.isNullAt(row.fieldIndex("cinr0x240b"))) row.getSeq(row.fieldIndex("cinr0x240b")).toList  else null
    channelNumberSig = if (!row.isNullAt(row.fieldIndex("channelNumber0x240b"))) row.getInt(row.fieldIndex("channelNumber0x240b")) else null
    bandwidthSig = if (!row.isNullAt(row.fieldIndex("bandwidth0x240b"))) row.getString(row.fieldIndex("bandwidth0x240b")) else null
    hexlogcodeSig = if (!row.isNullAt(row.fieldIndex("hexLogCode_sig"))) row.getString(row.fieldIndex("hexLogCode_sig")) else null
    centerFreq0x240C = if (!row.isNullAt(row.fieldIndex("centerFreq0x240c"))) row.getInt(row.fieldIndex("centerFreq0x240c")) else null


    numOfSignals0x2408_Sig = if (!row.isNullAt(row.fieldIndex("numSignals0x2408_sig"))) row.getString(row.fieldIndex("numSignals0x2408_sig")) else null
    channelPower0x2408_sig = if (!row.isNullAt(row.fieldIndex("channelPower0x2408_sig"))) row.getString(row.fieldIndex("channelPower0x2408_sig")) else null
    frequency0x2408_Sig = if (!row.isNullAt(row.fieldIndex("frequency0x2408_sig"))) row.getString(row.fieldIndex("frequency0x2408_sig")) else null
    bandwidth0x2408_sig = if (!row.isNullAt(row.fieldIndex("bandwidth0x2408_sig"))) row.getString(row.fieldIndex("bandwidth0x2408_sig")) else null
    numberOfTxAntennaPorts0x2408_Sig = if (!row.isNullAt(row.fieldIndex("numberOfTxAntennaPorts0x2408_collectedSig"))) row.getString(row.fieldIndex("numberOfTxAntennaPorts0x2408_collectedSig")) else null
    cinr0x2408Sig = if (!row.isNullAt(row.fieldIndex("CINR0x2408_collectedSig"))) row.getString(row.fieldIndex("CINR0x2408_collectedSig")) else null
    cellId0x2408Sig = if (!row.isNullAt(row.fieldIndex("cellId0x2408_collectedSig"))) row.getString(row.fieldIndex("cellId0x2408_collectedSig")) else null
    totalRsPower0x2408_collectedSig = if (!row.isNullAt(row.fieldIndex("totalRsPower0x2408_collectedSig"))) row.getString(row.fieldIndex("totalRsPower0x2408_collectedSig")) else null
    overallPower0x2408_collectedSig = if (!row.isNullAt(row.fieldIndex("overallPower0x2408_collectedSig"))) row.getString(row.fieldIndex("overallPower0x2408_collectedSig")) else null
    overallQuality0x2408_collectedSig = if (!row.isNullAt(row.fieldIndex("overallQuality0x2408_collectedSig"))) row.getString(row.fieldIndex("overallQuality0x2408_collectedSig")) else null
    cpType0x2408_collectedSig = if (!row.isNullAt(row.fieldIndex("cpType0x2408_collectedSig"))) row.getString(row.fieldIndex("cpType0x2408_collectedSig")) else null
    antQuality0x2408_collectedSig = if (!row.isNullAt(row.fieldIndex("antQuality0x2408_collectedSig"))) row.getString(row.fieldIndex("antQuality0x2408_collectedSig")) else null
    antPower0x2408_collectedSig = if (!row.isNullAt(row.fieldIndex("antPower0x2408_collectedSig"))) row.getString(row.fieldIndex("antPower0x2408_collectedSig")) else null


    frequency0x240D_Sig = if (!row.isNullAt(row.fieldIndex("frequency0x240D_sig"))) row.getString(row.fieldIndex("frequency0x240D_sig")) else null
    channelNumber0x240D_Sig = if (!row.isNullAt(row.fieldIndex("channelNumber0x240D_sig"))) row.getString(row.fieldIndex("channelNumber0x240D_sig")) else null
    bandwidth0x240D_Sig = if (!row.isNullAt(row.fieldIndex("bandwidth0x240D_sig"))) row.getString(row.fieldIndex("bandwidth0x240D_sig")) else null
    carrierRSSI0x240D_sig = if (!row.isNullAt(row.fieldIndex("carrierRSSI0x240D_sig"))) row.getString(row.fieldIndex("carrierRSSI0x240D_sig")) else null
    numOfSignals0x240D_sig = if (!row.isNullAt(row.fieldIndex("numSignals0x240D_sig"))) row.getString(row.fieldIndex("numSignals0x240D_sig")) else null
    CINR0x240D_collectedSig = if (!row.isNullAt(row.fieldIndex("CINR0x240D_collectedSig"))) row.getString(row.fieldIndex("CINR0x240D_collectedSig")) else null
    cellId0x240DSig = if (!row.isNullAt(row.fieldIndex("cellId0x240D_collectedSig"))) row.getString(row.fieldIndex("cellId0x240D_collectedSig")) else null
    rsrp0x240D_collectedSig = if (!row.isNullAt(row.fieldIndex("rsrp0x240D_collectedSig"))) row.getString(row.fieldIndex("rsrp0x240D_collectedSig")) else null
    rsrq0x240D_collectedSig = if (!row.isNullAt(row.fieldIndex("rsrq0x240D_collectedSig"))) row.getString(row.fieldIndex("rsrq0x240D_collectedSig")) else null
    numOfAntennaPorts0x240D_collectedSig = if (!row.isNullAt(row.fieldIndex("numberOfTxAntennaPorts0x240D_collectedSig"))) row.getString(row.fieldIndex("numberOfTxAntennaPorts0x240D_collectedSig")) else null


    frequency0x2406_sig = if (!row.isNullAt(row.fieldIndex("freq0x2406"))) row.getInt(row.fieldIndex("freq0x2406")) else null
    channelNumber0x2406_sig = if (!row.isNullAt(row.fieldIndex("channelNumber0x2406"))) row.getString(row.fieldIndex("channelNumber0x2406")) else null
    cinr0x2406_sig = if (!row.isNullAt(row.fieldIndex("cinr0x2406"))) row.getSeq(row.fieldIndex("cinr0x2406")).toList else null
    cellID0x2406_sig = if (!row.isNullAt(row.fieldIndex("cellId0x2406"))) row.getSeq(row.fieldIndex("cellId0x2406")).toList else null
    cpType0x2406_sig = if (!row.isNullAt(row.fieldIndex("cptType0x2406"))) row.getSeq(row.fieldIndex("cptType0x2406")).toList  else null
    secondarySyncQuality0x2406_sig = if (!row.isNullAt(row.fieldIndex("secondaryRsrq0x2406"))) row.getSeq(row.fieldIndex("secondaryRsrq0x2406")).toList else null
    secondarySyncPower0x2406_sig = if (!row.isNullAt(row.fieldIndex("secondaryRsrp0x2406"))) row.getSeq(row.fieldIndex("secondaryRsrp0x2406")).toList else null
    primarySyncQuality0x2406_sig = if (!row.isNullAt(row.fieldIndex("primaryRsrq0x2406"))) row.getSeq(row.fieldIndex("primaryRsrq0x2406")).toList  else null
    primarySyncPower0x2406_sig = if (!row.isNullAt(row.fieldIndex("primaryRsrp0x2406"))) row.getSeq(row.fieldIndex("primaryRsrp0x2406")).toList  else null




    numSSBurstDataBlocks0x2501_sig = if (!row.isNullAt(row.fieldIndex("numSSBurstDataBlocks0x2501_sig"))) row.getString(row.fieldIndex("numSSBurstDataBlocks0x2501_sig")) else null
    ssbRSSI0x2501_sig = if (!row.isNullAt(row.fieldIndex("ssbRSSI0x2501_sig"))) row.getString(row.fieldIndex("ssbRSSI0x2501_sig")) else null
    channelNumber0x2501_sig = if (!row.isNullAt(row.fieldIndex("channelNumber0x2501_sig"))) row.getString(row.fieldIndex("channelNumber0x2501_sig")) else null
    pci0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("pci0x2501_sig"))) row.getSeq(row.fieldIndex("pci0x2501_sig")).toList else null
    frequency0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("frequency0x2501_sig"))) row.getSeq(row.fieldIndex("frequency0x2501_sig")).toList else null
    subCarrierSpacing0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("subcarrierspacing0x2501_sig"))) row.getSeq(row.fieldIndex("subcarrierspacing0x2501_sig")).toList else null
    numbeams0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("numbeams0x2501_sig"))) row.getSeq(row.fieldIndex("numbeams0x2501_sig")).toList else null
    beamIndex0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("beamIndex0x2501_sig"))) row.getSeq(row.fieldIndex("beamIndex0x2501_sig")).toList else null
    pssBrsrp0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("pssBrsrp0x2501_sig"))) row.getSeq(row.fieldIndex("pssBrsrp0x2501_sig")).toList else null
    pssBrsrq0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("pssBrsrq0x2501_sig"))) row.getSeq(row.fieldIndex("pssBrsrq0x2501_sig")).toList else null
    pssCinr0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("pssCinr0x2501_sig"))) row.getSeq(row.fieldIndex("pssCinr0x2501_sig")).toList else null
    sssBrsrp0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("sssBrsrp0x2501_sig"))) row.getSeq(row.fieldIndex("sssBrsrp0x2501_sig")).toList else null
    sssBrsrq0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("sssBrsrq0x2501_sig"))) row.getSeq(row.fieldIndex("sssBrsrq0x2501_sig")).toList else null
    sssCinr0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("sssCinr0x2501_sig"))) row.getSeq(row.fieldIndex("sssCinr0x2501_sig")).toList else null
    sssDelay0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("sssDelay0x2501_sig"))) row.getSeq(row.fieldIndex("sssDelay0x2501_sig")).toList else null
    ssbBrsrp0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("ssbBrsrp0x2501_sig"))) row.getSeq(row.fieldIndex("ssbBrsrp0x2501_sig")).toList else null
    ssbBrsrq0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("ssbBrsrq0x2501_sig"))) row.getSeq(row.fieldIndex("ssbBrsrq0x2501_sig")).toList else null
    ssbCinr0x2501_collectedSig = if (!row.isNullAt(row.fieldIndex("ssbCinr0x2501_sig"))) row.getSeq(row.fieldIndex("ssbCinr0x2501_sig")).toList else null

    freq0x2157_Sig = if (!row.isNullAt(row.fieldIndex("freq0x2157")) && row.getInt(row.fieldIndex("freq0x2157")) != 0) row.getInt(row.fieldIndex("freq0x2157")).toString else null
    freqRsrp0x2157 = if (!row.isNullAt(row.fieldIndex("freqRsrp0x2157"))) getFreqRsrp0x2157KPIs(row.getString(row.fieldIndex("freqRsrp0x2157"))) else null


    if (cellId0x2408Sig != null && cinr0x2408Sig != null) cinr0x2408_sig = getNrServingCellKPIs(cellId0x2408Sig, cinr0x2408Sig)
    if (cellId0x2408Sig != null && totalRsPower0x2408_collectedSig != null) totalRsPower0x2408_sig = getNrServingCellKPIs(cellId0x2408Sig, totalRsPower0x2408_collectedSig)
    if (cellId0x2408Sig != null && overallPower0x2408_collectedSig != null) overallPower0x2408_sig = getNrServingCellKPIs(cellId0x2408Sig, overallPower0x2408_collectedSig)
    if (cellId0x2408Sig != null && overallQuality0x2408_collectedSig != null) overallQuality0x2408_sig = getNrServingCellKPIs(cellId0x2408Sig, overallQuality0x2408_collectedSig)
    if (cellId0x2408Sig != null && cpType0x2408_collectedSig != null) cpType0x2408_sig = getNrServingCellKPIs(cellId0x2408Sig, cpType0x2408_collectedSig)
    if (cellId0x2408Sig != null && antQuality0x2408_collectedSig != null) antQuality0x2408_sig = getNrServingCellKPIs(cellId0x2408Sig, antQuality0x2408_collectedSig)
    if (cellId0x2408Sig != null && antPower0x2408_collectedSig != null) antPower0x2408_sig = getNrServingCellKPIs(cellId0x2408Sig, antPower0x2408_collectedSig)
    if (cellId0x2408Sig != null && numberOfTxAntennaPorts0x2408_Sig != null) numOfAntennas0x2408_sig = getNrServingCellKPIs(cellId0x2408Sig, numberOfTxAntennaPorts0x2408_Sig)



    if (cellId0x240DSig != null && CINR0x240D_collectedSig != null) CINR0x240D_sig = getNrServingCellKPIs(cellId0x240DSig, CINR0x240D_collectedSig)
    if (cellId0x240DSig != null && rsrp0x240D_collectedSig != null) rsrp0x240D_sig = getNrServingCellKPIs(cellId0x240DSig, rsrp0x240D_collectedSig)
    if (cellId0x240DSig != null && rsrq0x240D_collectedSig != null) rsrq0x240D_sig = getNrServingCellKPIs(cellId0x240DSig, rsrq0x240D_collectedSig)
    if (cellId0x240DSig != null && numOfAntennaPorts0x240D_collectedSig != null) numOfAntennaPorts0x240D_sig = getNrServingCellKPIs(cellId0x240DSig, numOfAntennaPorts0x240D_collectedSig)


    if (pci0x2501_collectedSig != null && frequency0x2501_collectedSig != null) frequency0x2501_Sig = get0x2501KPIs(pci0x2501_collectedSig, frequency0x2501_collectedSig)
    if (pci0x2501_collectedSig != null && subCarrierSpacing0x2501_collectedSig != null) subCarrierSpacing0x2501_Sig = get0x2501KPIs(pci0x2501_collectedSig, subCarrierSpacing0x2501_collectedSig)
    if (pci0x2501_collectedSig != null && numbeams0x2501_collectedSig != null) numbeams0x2501_Sig = get0x2501KPIs(pci0x2501_collectedSig, numbeams0x2501_collectedSig)
    if (pci0x2501_collectedSig != null && beamIndex0x2501_collectedSig != null) beamIndex0x2501_Sig = get0x2501KPIs(pci0x2501_collectedSig, beamIndex0x2501_collectedSig)
    if (pci0x2501_collectedSig != null && pssBrsrp0x2501_collectedSig != null) pssBrsrp0x2501_Sig = get0x2501KPIs(pci0x2501_collectedSig, pssBrsrp0x2501_collectedSig)
    if (pci0x2501_collectedSig != null && pssBrsrq0x2501_collectedSig != null) pssBrsrq0x2501_Sig = get0x2501KPIs(pci0x2501_collectedSig, pssBrsrq0x2501_collectedSig)
    if (pci0x2501_collectedSig != null && pssCinr0x2501_collectedSig != null) pssCinr0x2501_Sig = get0x2501KPIs(pci0x2501_collectedSig, pssCinr0x2501_collectedSig)
    if (pci0x2501_collectedSig != null && sssBrsrp0x2501_collectedSig != null) sssBrsrp0x2501_Sig = get0x2501KPIs(pci0x2501_collectedSig, sssBrsrp0x2501_collectedSig)
    if (pci0x2501_collectedSig != null && sssBrsrq0x2501_collectedSig != null) sssBrsrq0x2501_Sig = get0x2501KPIs(pci0x2501_collectedSig, sssBrsrq0x2501_collectedSig)
    if (pci0x2501_collectedSig != null && sssCinr0x2501_collectedSig != null) sssCinr0x2501_Sig = get0x2501KPIs(pci0x2501_collectedSig, sssCinr0x2501_collectedSig)
    if (pci0x2501_collectedSig != null && sssDelay0x2501_collectedSig != null) sssDelay0x2501_Sig = get0x2501KPIs(pci0x2501_collectedSig, sssDelay0x2501_collectedSig)
    if (pci0x2501_collectedSig != null && ssbBrsrp0x2501_collectedSig != null) ssbBrsrp0x2501_Sig = get0x2501KPIs(pci0x2501_collectedSig, ssbBrsrp0x2501_collectedSig)
    if (pci0x2501_collectedSig != null && ssbBrsrq0x2501_collectedSig != null) ssbBrsrq0x2501_Sig = get0x2501KPIs(pci0x2501_collectedSig, ssbBrsrq0x2501_collectedSig)
    if (pci0x2501_collectedSig != null && ssbCinr0x2501_collectedSig != null) ssbCinr0x2501_Sig = get0x2501KPIs(pci0x2501_collectedSig, ssbCinr0x2501_collectedSig)
    numSignals0x240C = if (!row.isNullAt(row.fieldIndex("numSignals0x240c"))) row.getInt(row.fieldIndex("numSignals0x240c")) else null
    pci0x240C = if (!row.isNullAt(row.fieldIndex("pci0x240c"))) row.getSeq(row.fieldIndex("pci0x240c")).toList else null
    rsrp0x240C = if (!row.isNullAt(row.fieldIndex("rsrp0x240c"))) row.getSeq(row.fieldIndex("rsrp0x240c")).toList else null
    rsrq0x240C = if (!row.isNullAt(row.fieldIndex("rsrq0x240c"))) row.getSeq(row.fieldIndex("rsrq0x240c")).toList else null
    rscinr0x240C = if (!row.isNullAt(row.fieldIndex("rscinr0x240c"))) row.getSeq(row.fieldIndex("rscinr0x240c")).toList else null
    numAntennas0x240C = if (!row.isNullAt(row.fieldIndex("numberOfAntennas0x240c"))) row.getSeq(row.fieldIndex("numberOfAntennas0x240c")).toList else null
    antennaRsrp0x240C = if (!row.isNullAt(row.fieldIndex("antennaRsrp0x240c"))) row.getSeq(row.fieldIndex("antennaRsrp0x240c")).toList else null
    antennaRsrq0x240C = if (!row.isNullAt(row.fieldIndex("antennaRsrq0x240c"))) row.getSeq(row.fieldIndex("antennaRsrq0x240c")).toList else null
    antennaCinr0x240C = if (!row.isNullAt(row.fieldIndex("antennaCinr0x240c"))) row.getSeq(row.fieldIndex("antennaCinr0x240c")).toList else null

    if(fileType != null && fileType == "SIG"  && freq0x2157_Sig != null && freq0x2157_Sig.nonEmpty && freqRsrp0x2157 != null && freqRsrp0x2157.size > 0) {
      sigCellIdKpis += (if (freq0x2157_Sig != null && freq0x2157_Sig.nonEmpty) "GenericScannerPNRef2157Frequency" -> freq0x2157_Sig.toInt else "GenericScannerPNRef2157Frequency" -> "")
      breakable {
        for (i <- 1 to freqRsrp0x2157.size) {
          if (i > 8) {
            break
          }
          sigCellIdKpis += (if (freqRsrp0x2157 != null && freqRsrp0x2157(i-1)._1 > 0) "PNRef2157Signal" + (i-1) + "Frequency" -> freqRsrp0x2157(i-1)._1 else "PNRef2157Signal" + (i-1) + "Frequency"  -> "",
            if (freqRsrp0x2157 != null && freqRsrp0x2157(i-1)._2 != null) "PNRef2157Signal" + (i-1) + "Power" -> freqRsrp0x2157(i-1)._2 else "PNRef2157Signal" + (i-1) + "Power"  -> "")
        }
      }
    }


    if (frequencySig != null && frequencySig.toInt>0) {

      sigCellIdKpis += (if (frequencySig != null) "PNRef240BFrequency" -> frequencySig.toInt else "PNRef240BFrequency" -> "",
        if (channelNumberSig != null) "PNRef240BChannelNumber" -> channelNumberSig.toInt else "PNRef240BChannelNumber" -> "",
        if (bandwidthSig != null) "PNRef240BBandwidth" -> bandwidthSig.split("MHz")(0).toFloat else "PNRef240BBandwidth" -> "")

      if (cellIdSig != null) {
        breakable {
          for (i <- 1 to cellIdSig.size) {
            if (i > 8) {
              break
            }
            sigCellIdKpis += (if (cellIdSig != null && isValidNumeric(cellIdSig(i - 1), Constants.CONST_NUMBER)) "PNRef240BSignal" + (i - 1) + "CellID" -> cellIdSig(i - 1).toInt else "PNRef240BSignal" + i + "CellID" -> "",
              if (refPowerSig != null && isValidNumeric(refPowerSig(i - 1), Constants.CONST_FLOAT)) "PNRef240BSignal" + (i - 1) + "RefPower" -> refPowerSig(i - 1).toFloat else "PNRef240BSignal" + i + "RefPower" -> "",
              if (refQualitySig != null && isValidNumeric(refQualitySig(i - 1), Constants.CONST_FLOAT)) "PNRef240BSignal" + (i - 1) + "RefQuality" -> refQualitySig(i - 1).toFloat else "PNRef240BSignal" + i + "RefQuality" -> "",
              if (cinrSig != null && isValidNumeric(cinrSig(i - 1), Constants.CONST_FLOAT)) "PNRef240BSignal" + (i - 1) + "CINR" -> cinrSig(i - 1).toFloat else "PNRef240BSignal" + i + "CINR" -> "")
          }
        }
      }
    }



    if(frequency0x2406_sig != null && frequency0x2406_sig.toInt>0) {

      sigCellIdKpis += (if (frequency0x2406_sig != null) "PNSync2406Frequency"  -> frequency0x2406_sig.toInt  else "PNSync2406Frequency"  -> "",
        if (channelNumber0x2406_sig != null && isValidNumeric(channelNumber0x2406_sig, Constants.CONST_NUMBER)) "PNSync2406ChannelNumber"  -> channelNumber0x2406_sig.toInt  else "PNSync2406ChannelNumber"  -> "")

       if(cellID0x2406_sig != null) {
         breakable {
           for (i <- 1 to cellID0x2406_sig.size) {
             if (i > 10) {
               break
             }
             sigCellIdKpis += (if (cellID0x2406_sig != null && isValidNumeric(cellID0x2406_sig(i - 1), Constants.CONST_NUMBER)) "PNSync2406Signal" + (i - 1) + "CellID" -> cellID0x2406_sig(i - 1).toInt else "PNSync2406Signal" + i + "CellID" -> "",
               if (primarySyncQuality0x2406_sig != null && isValidNumeric(primarySyncQuality0x2406_sig(i - 1), Constants.CONST_FLOAT)) "PNSync2406Signal" + (i - 1) + "PrimarySyncQuality" -> primarySyncQuality0x2406_sig(i - 1).toFloat else "PNSync2406Signal" + i + "PrimarySyncQuality" -> "",
               if (primarySyncPower0x2406_sig != null && isValidNumeric(primarySyncPower0x2406_sig(i - 1), Constants.CONST_FLOAT)) "PNSync2406Signal" + (i - 1) + "PrimarySyncPower" -> primarySyncPower0x2406_sig(i - 1).toFloat else "PNSync2406Signal" + i + "PrimarySyncPower" -> "",
               if (secondarySyncQuality0x2406_sig != null && isValidNumeric(secondarySyncQuality0x2406_sig(i - 1), Constants.CONST_FLOAT)) "PNSync2406Signal" + (i - 1) + "SecondarySyncQuality" -> secondarySyncQuality0x2406_sig(i - 1).toFloat else "PNSync2406Signal" + i + "SecondarySyncQuality" -> "",
               if (secondarySyncPower0x2406_sig != null && isValidNumeric(secondarySyncPower0x2406_sig(i - 1), Constants.CONST_FLOAT)) "PNSync2406Signal" + (i - 1) + "SecondarySyncPower" -> secondarySyncPower0x2406_sig(i - 1).toFloat else "PNSync2406Signal" + i + "SecondarySyncPower" -> "",
               if (cinr0x2406_sig != null && isValidNumeric(cinr0x2406_sig(i - 1), Constants.CONST_FLOAT)) "PNSync2406Signal" + (i - 1) + "CINR" -> cinr0x2406_sig(i - 1).toFloat else "PNSync2406Signal" + i + "CINR" -> "")

           }
         }
       }
    }


    if(cellId0x2408Sig != null && cellId0x2408Sig.nonEmpty) {

      sigCellIdKpis += (

        if (frequency0x2408_Sig != null && frequency0x2408_Sig.nonEmpty) "AgilentLTEScannerPNRef2408Frequency"  -> frequency0x2408_Sig.toInt  else "AgilentLTEScannerPNRef2408Frequency"  -> "",
        if (bandwidth0x2408_sig != null && bandwidth0x2408_sig.nonEmpty) "AgilentLTEScannerPNRef2408Bandwidth"  -> bandwidth0x2408_sig.toInt  else "AgilentLTEScannerPNRef2408Bandwidth"  -> "",
        if (channelPower0x2408_sig != null && channelPower0x2408_sig.nonEmpty) "AgilentLTEScannerPNRef2408Power"  -> channelPower0x2408_sig.toFloat  else "AgilentLTEScannerPNRef2408Power"  -> "",
        if (numOfSignals0x2408_Sig != null && numOfSignals0x2408_Sig.nonEmpty) "AgilentLTEScannerPNRef2408NumberofSignals"  -> numOfSignals0x2408_Sig.toInt  else "AgilentLTEScannerPNRef2408NumberofSignals"  -> "")


      breakable {
        for (i <- 1 to cinr0x2408_sig.size) {
          if(i>5) {
            break
          }
          sigCellIdKpis += (if (cinr0x2408_sig != null && cinr0x2408_sig(i-1)._1.nonEmpty) "AgilentLTEScannerPNRef2408Signal" + (i-1) + "CellID" -> cinr0x2408_sig(i-1)._1.toInt else "AgilentLTEScannerPNRef2408Signal" + (i-1) + "CellID"  -> "",
            if (totalRsPower0x2408_sig != null  && totalRsPower0x2408_sig(i-1)._2.nonEmpty) "AgilentLTEScannerPNRef2408Signal" + (i-1) + "TotalRsPower"  -> totalRsPower0x2408_sig(i-1)._2.toFloat else "AgilentLTEScannerPNRef2408Signal" + (i-1) + "TotalRsPower"  -> "",
            if (overallPower0x2408_sig != null && overallPower0x2408_sig(i-1)._2.nonEmpty) "AgilentLTEScannerPNRef2408Signal" + (i-1) + "OverallPower"  -> overallPower0x2408_sig(i-1)._2.toFloat else "AgilentLTEScannerPNRef2408Signal" + (i-1) + "OverallPower"  -> "",
            if (overallQuality0x2408_sig != null  && overallQuality0x2408_sig(i-1)._2.nonEmpty) "AgilentLTEScannerPNRef2408Signal" + (i-1) + "OverallQuality"  -> overallQuality0x2408_sig(i-1)._2.toFloat else "AgilentLTEScannerPNRef2408Signal" + (i-1) + "OverallQuality"  -> "",
            if (cpType0x2408_sig != null && cpType0x2408_sig(i-1)._2.nonEmpty) "AgilentLTEScannerPNRef2408Signal" + (i-1) + "CPType"  -> cpType0x2408_sig(i-1)._2.toFloat else "AgilentLTEScannerPNRef2408Signal" + (i-1) + "CPType"  -> "",
            if (antQuality0x2408_sig != null && antQuality0x2408_sig(i-1)._2.nonEmpty) "AgilentLTEScannerPNRef2408Signal" + (i-1) + "CINR"  -> antQuality0x2408_sig(i-1)._2.toFloat  else "AgilentLTEScannerPNRef2408Signal" + (i-1) + "CINR"  -> "")

          breakable {
            if (numOfAntennas0x2408_sig != null && numOfAntennas0x2408_sig(i-1)._2.nonEmpty) {
              for (j <-1 to numOfAntennas0x2408_sig(i-1)._2.toInt) {
                if(i>3 || j>2) {
                  break
                }
                sigCellIdKpis += (if (antQuality0x2408_sig != null && antQuality0x2408_sig(i-1)._1.nonEmpty) "AgilentLTEScannerPNRef2408Signal" + (i-1) + "Ant" + (j-1) + "Quality" -> antQuality0x2408_sig(i-1)._1.toFloat else "AgilentLTEScannerPNRef2408Signal" + (i-1) + "Ant" + (j-1) + "Quality"  -> "",
                  if (antPower0x2408_sig != null && antPower0x2408_sig(i-1)._2.nonEmpty) "AgilentLTEScannerPNRef2408Signal" + (i-1) + "Ant" + (j-1)  + "Power"  -> antPower0x2408_sig(i-1)._2.toFloat  else "AgilentLTEScannerPNRef2408Signal" + (i-1) + "Ant" + (j-1)  + "Power"  -> ""
                )

              }
            }

          }

        }
      }

    }

    if((cellId0x240DSig != null && cellId0x240DSig.nonEmpty) && (CINR0x240D_sig != null && CINR0x240D_sig.nonEmpty)) {

      sigCellIdKpis += (if (frequency0x240D_Sig != null && frequency0x240D_Sig.nonEmpty) "pctelLTELaaScanner240DFrequency"  -> frequency0x240D_Sig.toInt  else "pctelLTELaaScanner240DFrequency"  -> "",
      if (channelNumber0x240D_Sig != null && channelNumber0x240D_Sig.nonEmpty) "pctelLTELaaScanner240DChannelNumber"  -> channelNumber0x240D_Sig.toInt  else "pctelLTELaaScanner240DChannelNumber"  -> "",
      //  if (bandwidth0x240D_Sig != null && bandwidth0x240D_Sig.nonEmpty) "pctelLTELaaScanner240DBandwidth"  -> bandwidth0x240D_Sig.split(" MHz")(0).toInt  else "pctelLTELaaScanner240DBandwidth"  -> "",
      if (carrierRSSI0x240D_sig != null && carrierRSSI0x240D_sig.nonEmpty) "pctelLTELaaScanner240DCarrierRSSI"  -> carrierRSSI0x240D_sig.toFloat  else "pctelLTELaaScanner240DCarrierRSSI"  -> "",
      if (numOfSignals0x240D_sig != null && numOfSignals0x240D_sig.nonEmpty) "pctelLTELaaScanner240DNumberOfSignals"  -> numOfSignals0x240D_sig.toInt  else "pctelLTELaaScanner240DNumberOfSignals"  -> "")

      breakable {
        for (i <- 1 to CINR0x240D_sig.size) {
          if(i>5) {
            break
          }


          sigCellIdKpis += (if (CINR0x240D_sig != null && CINR0x240D_sig(i-1)._1.nonEmpty) "pctelLTELaaScanner240DSignal" + (i-1) + "CellID" -> CINR0x240D_sig(i-1)._1.toInt else "pctelLTELaaScanner240DSignal" + i + "CellID"  -> "",
            if (rsrp0x240D_sig != null && rsrp0x240D_sig(i-1)._2.nonEmpty) "pctelLTELaaScanner240DSignal" + (i-1) + "Rsrp"  -> rsrp0x240D_sig(i-1)._2.toFloat  else "pctelLTELaaScanner240DSignal" + i + "Rsrp"  -> "",
            if (rsrq0x240D_sig != null && rsrq0x240D_sig(i-1)._2.nonEmpty) "pctelLTELaaScanner240DSignal" + (i-1) + "Rsrq"  -> rsrq0x240D_sig(i-1)._2.toFloat  else "pctelLTELaaScanner240DSignal" + i + "Rsrq"  -> "",
            if (CINR0x240D_sig != null && CINR0x240D_sig(i-1)._2.nonEmpty) "pctelLTELaaScanner240DSignal" + (i-1) + "Cinr"  -> CINR0x240D_sig(i-1)._2.toFloat  else "pctelLTELaaScanner240DSignal" + i + "Cinr"  -> "",
            if (numOfAntennaPorts0x240D_sig != null && numOfAntennaPorts0x240D_sig(i-1)._2.nonEmpty) "pctelLTELaaScanner240DSignal" + (i-1) + "NumberOfTxAntennas"  -> numOfAntennaPorts0x240D_sig(i-1)._2.toFloat  else "pctelLTELaaScanner240DSignal" + i + "NumberOfTxAntennas"  -> ""
          )

        }
      }

    }


    if((pci0x2501_collectedSig != null && pci0x2501_collectedSig.nonEmpty) && (frequency0x2501_Sig != null && frequency0x2501_Sig.nonEmpty)) {

    sigCellIdKpis += (if (ssbRSSI0x2501_sig != null && ssbRSSI0x2501_sig.nonEmpty) "GenericScanner5GNRSSBRSSI"  -> ssbRSSI0x2501_sig.toFloat else "GenericScanner5GNRSSBRSSI"  -> "",
        if (channelNumber0x2501_sig != null  && channelNumber0x2501_sig.nonEmpty) "GenericScanner5GNRChannelNumber"   -> channelNumber0x2501_sig.toInt else "GenericScanner5GNRChannelNumber"  -> "",
        if (numSSBurstDataBlocks0x2501_sig != null && numSSBurstDataBlocks0x2501_sig.nonEmpty) "NumberofSSBurstDataBlocks"  -> numSSBurstDataBlocks0x2501_sig.toInt else "NumberofSSBurstDataBlocks"  -> "")

      breakable {
        for (i <- 1 to frequency0x2501_Sig.size) {
          if(i>2) {
            break
          }
          sigCellIdKpis += (if (frequency0x2501_Sig != null && frequency0x2501_Sig(i-1)._1.nonEmpty) "GenericScanner5GNRBlock" + i + "CellID" -> frequency0x2501_Sig(i-1)._1.toInt else "GenericScanner5GNRBlock" + i + "CellID"  -> "",
            if (frequency0x2501_Sig != null && frequency0x2501_Sig(i-1)._2.nonEmpty) "GenericScanner5GNRBlock" + i + "Frequency"  -> frequency0x2501_Sig(i-1)._2.toInt  else "GenericScanner5GNRBlock" + i + "Frequency"  -> "",
            if (subCarrierSpacing0x2501_Sig != null && subCarrierSpacing0x2501_Sig(i-1)._2.nonEmpty) "GenericScanner5GNRBlock" + i + "SubCarrierSpacing"  -> subCarrierSpacing0x2501_Sig(i-1)._2.toFloat  else "GenericScanner5GNRBlock" + i + "SubCarrierSpacing"  -> "",
            if (numbeams0x2501_Sig != null && numbeams0x2501_Sig(i-1)._2.nonEmpty && i<2) "GenericScanner5GNRBlock" + i + "NumberofBeamDataBlocks"  -> numbeams0x2501_Sig(i-1)._2.toFloat  else "GenericScanner5GNRBlock" + i + "NumberofBeamDataBlocks"  -> "",
            if (numbeams0x2501_Sig != null && numbeams0x2501_Sig(i-1)._2.nonEmpty && i==2) "GenericScanner5GNRBlock" + i + "NumberofBeamDataBlocks"  -> numbeams0x2501_Sig(i-1)._2.toInt  else "GenericScanner5GNRBlock" + i + "NumberofBeamDataBlocks"  -> ""
          )

          breakable {
            if(numbeams0x2501_Sig != null && numbeams0x2501_Sig(i-1)._2.nonEmpty) {
              for (j <-1 to numbeams0x2501_Sig(i-1)._2.toInt) {
                if(j>3 || i>2) {
                  break
                }
                sigCellIdKpis += (if (beamIndex0x2501_Sig != null && beamIndex0x2501_Sig(i-1)._1.nonEmpty) "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "BeamIndex" -> beamIndex0x2501_Sig(i-1)._2.toInt else "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "BeamIndex"  -> "",
                  if (pssBrsrp0x2501_Sig != null && pssBrsrp0x2501_Sig(i-1)._2.nonEmpty) "GenericScanner5GNRBlock" + i + "Beam" + (j-1)  + "PSSRP"  -> pssBrsrp0x2501_Sig(i-1)._2.toFloat  else "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "PSSRP"  -> "",
                  if (pssBrsrq0x2501_Sig != null && pssBrsrq0x2501_Sig(i-1)._2.nonEmpty) "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "PSSRQ"  -> pssBrsrq0x2501_Sig(i-1)._2.toFloat  else "GenericScanner5GNRBlock" + i + "Beam" + (j-1)  + "PSSRQ"  -> "",
                  if (pssCinr0x2501_Sig != null && pssCinr0x2501_Sig(i-1)._2.nonEmpty) "GenericScanner5GNRBlock" + i + "Beam" + (j-1)  + "PSSCINR"  -> pssCinr0x2501_Sig(i-1)._2.toFloat  else "GenericScanner5GNRBlock" + i + "Beam" + (j-1)  + "PSSCINR" -> "",
                  if (sssBrsrp0x2501_Sig != null && sssBrsrp0x2501_Sig(i-1)._2.nonEmpty) "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "SSSRP"  -> sssBrsrp0x2501_Sig(i-1)._2.toFloat  else "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "SSSRP" -> "",
                  if (sssBrsrq0x2501_Sig != null && sssBrsrq0x2501_Sig(i-1)._2.nonEmpty) "GenericScanner5GNRBlock"+ i + "Beam" + (j-1)  + "SSSRQ"  -> sssBrsrq0x2501_Sig(i-1)._2.toFloat  else "GenericScanner5GNRBlock" + i + "Beam" + (j-1)  + "SSSRQ"  -> "",
                  if (sssCinr0x2501_Sig != null && sssCinr0x2501_Sig(i-1)._2.nonEmpty) "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "SSSCINR"  -> sssCinr0x2501_Sig(i-1)._2.toFloat  else "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "SSSCINR"  -> "",
                  if (sssDelay0x2501_Sig != null && sssDelay0x2501_Sig(i-1)._2.nonEmpty) "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "SSSDelaySpread"  -> sssDelay0x2501_Sig(i-1)._2.toFloat  else "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "SSSDelaySpread"  -> "",
                  if (ssbBrsrp0x2501_Sig != null && ssbBrsrp0x2501_Sig(i-1)._2.nonEmpty) "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "SSBRP"  -> ssbBrsrp0x2501_Sig(i-1)._2.toFloat  else "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "SSBRP"  -> "",
                  if (ssbBrsrq0x2501_Sig != null && ssbBrsrq0x2501_Sig(i-1)._2.nonEmpty) "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "SSBRQ"  -> ssbBrsrq0x2501_Sig(i-1)._2.toFloat  else "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "SSBRQ" -> "",
                  if (ssbCinr0x2501_Sig != null && ssbCinr0x2501_Sig(i-1)._2.nonEmpty) "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "SSBCINR"  -> ssbCinr0x2501_Sig(i-1)._2.toFloat  else "GenericScanner5GNRBlock" + i + "Beam" + (j-1) + "SSBCINR"   -> ""

                )

              }
            }

          }

        }
      }

    }
    if(centerFreq0x240C != null) {
      sigCellIdKpis += (

        if (centerFreq0x240C != null) "TSMWLTEScanner240CCenterFrequency"  -> centerFreq0x240C  else "TSMWLTEScanner240CCenterFrequency"  -> "",
        if (centerFreq0x240C != null && numSignals0x240C != null) "TSMWLTEScanner240CNumberofSignals"  -> numSignals0x240C  else "TSMWLTEScanner240CNumberofSignals"  -> "")

      var counter = 0
      var totalAntennaCount = 0
      breakable {
        for (i <- 1 to pci0x240C.size) {
          if(i>5) {
            break
    }
          // var antennaCount = numAntennas0x240C(i-1).toInt
          sigCellIdKpis += (if (pci0x240C != null && pci0x240C(i-1) != "NA") "TSMWLTEScanner240CSignal" + (i-1) + "CellID" -> pci0x240C(i-1).toInt else "TSMWLTEScanner240CSignal" + (i-1) + "CellID"  -> "",
            if (rsrp0x240C != null  && rsrp0x240C(i-1) != "NA") "TSMWLTEScanner240CSignal" + (i-1) + "Rsrp"  -> rsrp0x240C(i-1).toFloat else "TSMWLTEScanner240CSignal" + (i-1) + "Rsrp"  -> "",
            if (rsrq0x240C != null && rsrq0x240C(i-1) != "NA") "TSMWLTEScanner240CSignal" + (i-1) + "Rsrq"  -> rsrq0x240C(i-1).toFloat else "TSMWLTEScanner240CSignal" + (i-1) + "Rsrq"  -> "",
            if (rscinr0x240C != null  && rscinr0x240C(i-1) != "NA") "TSMWLTEScanner240CSignal" + (i-1) + "Cinr"  -> rscinr0x240C(i-1).toFloat else "TSMWLTEScanner240CSignal" + (i-1) + "Cinr"  -> "",
            if (numAntennas0x240C != null  && numAntennas0x240C(i-1) != "NA") "TSMWLTEScanner240CSignal" + (i-1) + "NumberOfTxAntennas"  -> numAntennas0x240C(i-1).toInt else "TSMWLTEScanner240CSignal" + (i-1) + "NumberOfTxAntennas"  -> "")

          //          if(antennaCount>0) {
          //
          //            val antrsp = antennaRsrp0x240C.slice(totalAntennaCount, numAntennas0x240C(i-1).toInt + totalAntennaCount)
          //            val antrsq = antennaRsrq0x240C.slice(totalAntennaCount, numAntennas0x240C(i-1).toInt + totalAntennaCount)
          //            val antCinr = antennaCinr0x240C.slice(totalAntennaCount, numAntennas0x240C(i-1).toInt + totalAntennaCount)
          //            breakable {
          //              for (j <-1 to antrsp.size) {
          //                if(i>5 || j>4) {
          //                  break
          //                }
          //                sigCellIdKpis += (if (antrsp != null && antrsp(j-1) != "NA") "TSMWLTEScanner240CSignal" + (i-1) + "Ant" + (j-1) + "Rsrp" -> antrsp(j-1).toFloat else "TSMWLTEScanner240CSignal" + (i-1) + "Ant" + (j-1) + "Rsrp"  -> "",
          //                  if (antrsq != null && antrsq(j-1) != "NA") "TSMWLTEScanner240CSignal" + (i-1) + "Ant" + (j-1)  + "Rsrq"  -> antrsq(j-1).toFloat  else "TSMWLTEScanner240CSignal" + (i-1) + "Ant" + (j-1)  + "Rsrq"  -> "",
          //                  if (antCinr != null && antCinr(j-1) != "NA") "TSMWLTEScanner240CSignal" + (i-1) + "Ant" + (j-1)  + "Cinr"  -> antCinr(j-1).toFloat  else "TSMWLTEScanner240CSignal" + (i-1) + "Ant" + (j-1)  + "Cinr"  -> ""
          //                )
          //              }
          //            }
          //            totalAntennaCount += antennaCount
          //          }
        }
      }

    }

  }

    sigCellIdKpis
  }

  def getTraceScannerKPIs(row:Row): mutable.Map[String, Any] = {
    val indexMap: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()

    val channelfrequency: java.lang.Integer = if (!row.isNullAt(row.fieldIndex("freqLTETopNSig"))) row.getInt(row.fieldIndex("freqLTETopNSig")) else null
    val cellId: List[String] = if (!row.isNullAt(row.fieldIndex("cellIdLTETopNSig"))) row.getSeq(row.fieldIndex("cellIdLTETopNSig")).toList else null
    val refPower: List[String] = if (!row.isNullAt(row.fieldIndex("primaryRsrpLTETopNSig"))) row.getSeq(row.fieldIndex("primaryRsrpLTETopNSig")).toList else null
    val refQuality: List[String] = if (!row.isNullAt(row.fieldIndex("primaryRsrqLTETopNSig"))) row.getSeq(row.fieldIndex("primaryRsrqLTETopNSig")).toList else null
    val cinr: List[String] = if (!row.isNullAt(row.fieldIndex("cinrLTETopNSig"))) row.getSeq(row.fieldIndex("cinrLTETopNSig")).toList else null
    val channelNumber: java.lang.Integer = if (!row.isNullAt(row.fieldIndex("channelNumberLTETopNSig"))) row.getInt(row.fieldIndex("channelNumberLTETopNSig")) else null
    val bandwidth: String = if (!row.isNullAt(row.fieldIndex("bandwidthLTETopNSig"))) row.getString(row.fieldIndex("bandwidthLTETopNSig")) else null

    if (channelfrequency != null && channelfrequency > 0) {

      if (channelfrequency != null) indexMap.put("pctelLTETopNSignalFrequency", channelfrequency.toInt)
      if (channelNumber != null) indexMap.put("pctelLTETopNSignalNumber", channelNumber)
      if (bandwidth != null) indexMap.put("pctelLTETopNSignalBandwidth", bandwidth.split("MHz")(0).toFloat)
      if (cellId != null) indexMap.put("pctelLTETopNSignalNumSignals", cellId.size)

      if (cellId != null) {
        breakable {
          for (i <- 1 to cellId.size) {
            if (i > 8) {
              break
            }
            if (cellId != null && isValidNumeric(cellId(i - 1), Constants.CONST_NUMBER)) indexMap.put("pctelLTETopNSignal" + (i - 1) + "CellID", cellId(i - 1).toInt)
            if (refPower != null && isValidNumeric(refPower(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelLTETopNSignal" + (i - 1) + "RefPower", refPower(i - 1).toFloat)
            if (refQuality != null && isValidNumeric(refQuality(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelLTETopNSignal" + (i - 1) + "RefQuality", refQuality(i - 1).toFloat)
            if (cinr != null && isValidNumeric(cinr(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelLTETopNSignal" + (i - 1) + "CINR", cinr(i - 1).toFloat)
          }
        }
      }
    }

    val signalNumber: String = if (!row.isNullAt(row.fieldIndex("nr_topnSignal"))) row.getString(row.fieldIndex("nr_topnSignal")) else null
    val channelFrequencyNRTopNSig: java.lang.Integer = if (!row.isNullAt(row.fieldIndex("freqNRTopNSignal"))) row.getInt(row.fieldIndex("freqNRTopNSignal")) else null
    val channelNumberNRTopNSig: java.lang.Integer = if (!row.isNullAt(row.fieldIndex("channelNumber_NRTopNSignal"))) row.getInt(row.fieldIndex("channelNumber_NRTopNSignal")) else null
    val numCellsNRTopNSig: java.lang.Integer = if (!row.isNullAt(row.fieldIndex("numCells_NRTopNSignal"))) row.getInt(row.fieldIndex("numCells_NRTopNSignal")) else null
    val ssbRssiNRTopNSig: java.lang.Double = if (!row.isNullAt(row.fieldIndex("ssbRssi_NRTopNSignal"))) row.getDouble(row.fieldIndex("ssbRssi_NRTopNSignal")) else null
    val bandwidthNRTopNSig: String = if (!row.isNullAt(row.fieldIndex("band_NRTopNSignal"))) row.getString(row.fieldIndex("band_NRTopNSignal")) else null
    val subCarrierSpacingNRTopNSig: java.lang.Integer = if (!row.isNullAt(row.fieldIndex("subCarrierSpacing_NRTopNSignal"))) row.getInt(row.fieldIndex("subCarrierSpacing_NRTopNSignal")) else null

    val pssCinrNRTopNSig = if (!row.isNullAt(row.fieldIndex("psscinr_nrTopNSignal"))) row.getString(row.fieldIndex("psscinr_nrTopNSignal")) else null
    val pssRefPowerNRTopNSig: String = if (!row.isNullAt(row.fieldIndex("pssrsrpnr_TopNSignal"))) row.getString(row.fieldIndex("pssrsrpnr_TopNSignal")) else null
    val pssRefQualityNRTopNSig: String = if (!row.isNullAt(row.fieldIndex("pssrsrqnr_TopNSignal"))) row.getString(row.fieldIndex("pssrsrqnr_TopNSignal")) else null

    val sssCinrNRTopNSig: String = if (!row.isNullAt(row.fieldIndex("ssscinrnr_TopNSignal"))) row.getString(row.fieldIndex("ssscinrnr_TopNSignal")) else null
    val sssRefPowerNRTopNSig: String = if (!row.isNullAt(row.fieldIndex("sssrsrpnr_TopNSignal"))) row.getString(row.fieldIndex("sssrsrpnr_TopNSignal")) else null
    val sssRefQualityNRTopNSig: String = if (!row.isNullAt(row.fieldIndex("sssrsrqnr_TopNSignal"))) row.getString(row.fieldIndex("sssrsrqnr_TopNSignal")) else null

    val ssbCinrNRTopNSig: String = if (!row.isNullAt(row.fieldIndex("ssbcinrnr_TopNSignal"))) row.getString(row.fieldIndex("ssbcinrnr_TopNSignal")) else null
    val ssbRefPowerNRTopNSig: String = if (!row.isNullAt(row.fieldIndex("ssbrsrpnr_TopNSignal"))) row.getString(row.fieldIndex("ssbrsrpnr_TopNSignal")) else null
    val ssbRefQualityNRTopNSig: String = if (!row.isNullAt(row.fieldIndex("ssbrsrqnr_TopNSignal"))) row.getString(row.fieldIndex("ssbrsrqnr_TopNSignal")) else null

    val cellIdNRTopNSig: String = if (!row.isNullAt(row.fieldIndex("cellIdnr_TopNSignal"))) row.getString(row.fieldIndex("cellIdnr_TopNSignal")) else null
    val beamIndexNRTopNSig: String = if (!row.isNullAt(row.fieldIndex("beamIndexnr_TopNSignal"))) row.getString(row.fieldIndex("beamIndexnr_TopNSignal")) else null

    if (numCellsNRTopNSig != null) {
      if (numCellsNRTopNSig > 0) {
        val sigNumNRTopNSig0 = signalNumber.split(";")(0).toInt
        val cellIdNRTopNSig0 = cellIdNRTopNSig.split(";")(0).toInt
        val pssCinrNRTopNSig0 = pssCinrNRTopNSig.split(";")(0).split(",").toList
        val pssRefPowerNRTopNSig0 = pssRefPowerNRTopNSig.split(";")(0).split(",").toList
        val pssRefQualityNRTopNSig0 = pssRefQualityNRTopNSig.split(";")(0).split(",").toList
        val sssCinrNRTopNSig0 = sssCinrNRTopNSig.split(";")(0).split(",").toList
        val sssRefPowerNRTopNSig0 = sssRefPowerNRTopNSig.split(";")(0).split(",").toList
        val sssRefQualityNRTopNSig0 = sssRefQualityNRTopNSig.split(";")(0).split(",").toList
        val ssbCinrNRTopNSig0 = ssbCinrNRTopNSig.split(";")(0).split(",").toList
        val ssbRefPowerNRTopNSig0 = ssbRefPowerNRTopNSig.split(";")(0).split(",").toList
        val ssbRefQualityNRTopNSig0 = ssbRefQualityNRTopNSig.split(";")(0).split(",").toList
        val beamIndexNRTopNSig0 = beamIndexNRTopNSig.split(";")(0).split(",").toList
        //        logger.info(s" sigNumNRTopNSig0=$sigNumNRTopNSig0 cellIdNRTopNSig0=$cellIdNRTopNSig0 pssCinrNRTopNSig0=$pssCinrNRTopNSig0")

        fillBeamData(sigNumNRTopNSig0, pssCinrNRTopNSig0, pssRefPowerNRTopNSig0, pssRefQualityNRTopNSig0,
          sssCinrNRTopNSig0, sssRefPowerNRTopNSig0, sssRefQualityNRTopNSig0,
          ssbCinrNRTopNSig0, ssbRefPowerNRTopNSig0, ssbRefQualityNRTopNSig0, beamIndexNRTopNSig0)
        if (cellIdNRTopNSig0 != null) indexMap.put("pctelNRTopNSignal" + sigNumNRTopNSig0 + "CellId", cellIdNRTopNSig0)
      }
    }

    if (numCellsNRTopNSig != null) {
      if (numCellsNRTopNSig > 1) {
        // println("num signals>>>" + signalNumber + "channel freq>>>>" + channelFrequencyNRTopNSig + "Timestamp>>>" + getTimestamp(row.getString(row.fieldIndex("dateWithoutMillis"))))
        val sigNumNRTopNSig1 = signalNumber.split(";")(1).toInt
        val cellIdNRTopNSig1 = cellIdNRTopNSig.split(";")(1).toInt
        val pssCinrNRTopNSig1 = pssCinrNRTopNSig.split(";")(1).split(",").toList
        val pssRefPowerNRTopNSig1 = pssRefPowerNRTopNSig.split(";")(1).split(",").toList
        val pssRefQualityNRTopNSig1 = pssRefQualityNRTopNSig.split(";")(1).split(",").toList
        val sssCinrNRTopNSig1 = sssCinrNRTopNSig.split(";")(1).split(",").toList
        val sssRefPowerNRTopNSig1 = sssRefPowerNRTopNSig.split(";")(1).split(",").toList
        val sssRefQualityNRTopNSig1 = sssRefQualityNRTopNSig.split(";")(1).split(",").toList
        val ssbCinrNRTopNSig1 = ssbCinrNRTopNSig.split(";")(1).split(",").toList
        val ssbRefPowerNRTopNSig1 = ssbRefPowerNRTopNSig.split(";")(1).split(",").toList
        val ssbRefQualityNRTopNSig1 = ssbRefQualityNRTopNSig.split(";")(1).split(",").toList
        val beamIndexNRTopNSig1 = beamIndexNRTopNSig.split(";")(1).split(",").toList

        fillBeamData(sigNumNRTopNSig1, pssCinrNRTopNSig1, pssRefPowerNRTopNSig1, pssRefQualityNRTopNSig1,
          sssCinrNRTopNSig1, sssRefPowerNRTopNSig1, sssRefQualityNRTopNSig1,
          ssbCinrNRTopNSig1, ssbRefPowerNRTopNSig1, ssbRefQualityNRTopNSig1, beamIndexNRTopNSig1)
        if (cellIdNRTopNSig1 != null) indexMap.put("pctelNRTopNSignal" + sigNumNRTopNSig1 + "CellId", cellIdNRTopNSig1)
      }
    }

    if (channelFrequencyNRTopNSig != null && channelFrequencyNRTopNSig > 0) {
      if (channelFrequencyNRTopNSig != null) indexMap.put("pctelNRTopNSignalFrequency", channelFrequencyNRTopNSig)
      if (channelNumberNRTopNSig != null) indexMap.put("pctelNRTopNSignalNumber", channelNumberNRTopNSig)
      if (numCellsNRTopNSig != null) indexMap.put("pctelNRTopNSignalNumSignals", numCellsNRTopNSig)
      if (ssbRssiNRTopNSig != null) indexMap.put("pctelNRTopNSignalSsbRssi", ssbRssiNRTopNSig)
      if (bandwidthNRTopNSig != null) indexMap.put("pctelNRTopNSignalBandwidth", bandwidthNRTopNSig.split("MHz")(0).toFloat)
      if (subCarrierSpacingNRTopNSig != null) indexMap.put("pctelNRTopNSignalSubCarrierSpacing", subCarrierSpacingNRTopNSig)
      //      logger.info(s" channelFrequencyNRTopNSig=$channelFrequencyNRTopNSig channelNumberNRTopNSig=$channelNumberNRTopNSig numCellsNRTopNSig=$numCellsNRTopNSig")
    }

    def fillBeamData(signalNumber: Integer, pssCinr: List[String], pssRsrp: List[String], pssRsrq: List[String], sssCinr: List[String], sssRsrp: List[String], sssRsrq: List[String],
                     ssbcinr: List[String], ssbRsrp: List[String], ssbRsrq: List[String], beamIndex: List[String]): scala.collection.mutable.Map[String, Any] = {
      if (pssCinr != null) {
        breakable {
          for (i <- 1 to pssCinr.size) {
            if (i > 3) {
              break
            }
            if (pssCinr != null && isValidNumeric(pssCinr(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelNRTopNSignal" + signalNumber + "Beam" + (i - 1) + "PssCinr", round(pssCinr(i - 1).toFloat, 2))
            if (pssRsrp != null && isValidNumeric(pssRsrp(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelNRTopNSignal" + signalNumber + "Beam" + (i - 1) + "PssRefPower", round(pssRsrp(i - 1).toFloat, 2))
            if (pssRsrq != null && isValidNumeric(pssRsrq(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelNRTopNSignal" + signalNumber + "Beam" + (i - 1) + "PssRefQuality", round(pssRsrq(i - 1).toFloat, 2))
            if (sssCinr != null && isValidNumeric(sssCinr(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelNRTopNSignal" + signalNumber + "Beam" + (i - 1) + "SssCinr", round(sssCinr(i - 1).toFloat, 2))
            if (sssRsrp != null && isValidNumeric(sssRsrp(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelNRTopNSignal" + signalNumber + "Beam" + (i - 1) + "SssRefPower", round(sssRsrp(i - 1).toFloat, 2))
            if (sssRsrq != null && isValidNumeric(sssRsrq(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelNRTopNSignal" + signalNumber + "Beam" + (i - 1) + "SssRefQuality", round(sssRsrq(i - 1).toFloat, 2))
            if (ssbcinr != null && isValidNumeric(ssbcinr(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelNRTopNSignal" + signalNumber + "Beam" + (i - 1) + "SsbCinr", round(ssbcinr(i - 1).toFloat, 2))
            if (ssbRsrp != null && isValidNumeric(ssbRsrp(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelNRTopNSignal" + signalNumber + "Beam" + (i - 1) + "SsbRefPower", round(ssbRsrp(i - 1).toFloat, 2))
            if (ssbRsrq != null && isValidNumeric(ssbRsrq(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelNRTopNSignal" + signalNumber + "Beam" + (i - 1) + "SsbRefQuality", round(ssbRsrq(i - 1).toFloat, 2))
            if (beamIndex != null && isValidNumeric(beamIndex(i - 1), Constants.CONST_NUMBER)) indexMap.put("pctelNRTopNSignal" + signalNumber + "Beam" + (i - 1) + "BeamIndex", beamIndex(i - 1).toInt)
          }
        }
      }
      indexMap
    }

    val channelfrequencyBlindScanLTE: java.lang.Integer = if (!row.isNullAt(row.fieldIndex("freqLTEBlindScan"))) row.getInt(row.fieldIndex("freqLTEBlindScan")) else null
    val cellIdBlindScanLTE: List[String] = if (!row.isNullAt(row.fieldIndex("cellIdLTEBlindScan"))) row.getSeq(row.fieldIndex("cellIdLTEBlindScan")).toList else null
    val refPowerBlindScanLTE: List[String] = if (!row.isNullAt(row.fieldIndex("primaryRsrpLTEBlindScan"))) row.getSeq(row.fieldIndex("primaryRsrpLTEBlindScan")).toList else null
    val refQualityBlindScanLTE: List[String] = if (!row.isNullAt(row.fieldIndex("primaryRsrqLTEBlindScan"))) row.getSeq(row.fieldIndex("primaryRsrqLTEBlindScan")).toList else null
    val cinrBlindScanLTE: List[String] = if (!row.isNullAt(row.fieldIndex("cinrLTEBlindScan"))) row.getSeq(row.fieldIndex("cinrLTEBlindScan")).toList else null
    val channelNumberBlindScanLTE: java.lang.Integer = if (!row.isNullAt(row.fieldIndex("channelNumberLTEBlindScan"))) row.getInt(row.fieldIndex("channelNumberLTEBlindScan")) else null
    val bandwidthBlindScanLTE: String = if (!row.isNullAt(row.fieldIndex("bandwidthLTEBlindScan"))) row.getString(row.fieldIndex("bandwidthLTEBlindScan")) else null

    if (channelfrequencyBlindScanLTE != null && channelfrequencyBlindScanLTE > 0) {

      if (channelfrequencyBlindScanLTE != null) indexMap.put("pctelLTEBlindScanFrequency", channelfrequencyBlindScanLTE.toInt)
      if (channelNumberBlindScanLTE != null) indexMap.put("pctelLTEBlindScanNumber", channelNumberBlindScanLTE)
      if (bandwidthBlindScanLTE != null) indexMap.put("pctelLTEBlindScanBandwidth", bandwidthBlindScanLTE.split("MHz")(0).toFloat)
      if (cellIdBlindScanLTE != null) indexMap.put("pctelLTEBlindScanNumSignals", cellIdBlindScanLTE.size)
      //      logger.info(s" cellIdBlindScanLTE=$cellIdBlindScanLTE refPowerBlindScanLTE=$refPowerBlindScanLTE refQualityBlindScanLTE=$refQualityBlindScanLTE cinrBlindScanLTE=$cinrBlindScanLTE ")

      if (cellIdBlindScanLTE != null) {
        breakable {
          for (i <- 1 to cellIdBlindScanLTE.size) {
            if (i > 8) {
              break
            }
            if (cellIdBlindScanLTE != null && cellIdBlindScanLTE.size >= i && isValidNumeric(cellIdBlindScanLTE(i - 1), Constants.CONST_NUMBER)) indexMap.put("pctelLTEBlindScan" + (i - 1) + "CellID", cellIdBlindScanLTE(i - 1).toInt)
            if (refPowerBlindScanLTE != null && refPowerBlindScanLTE.size >= i && isValidNumeric(refPowerBlindScanLTE(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelLTEBlindScan" + (i - 1) + "RefPower", round(parseFloat(refPowerBlindScanLTE(i - 1)), 2))
            if (refQualityBlindScanLTE != null && refQualityBlindScanLTE.size >= i && isValidNumeric(refQualityBlindScanLTE(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelLTEBlindScan" + (i - 1) + "RefQuality", round(parseFloat(refQualityBlindScanLTE(i - 1)), 2))
            if (cinrBlindScanLTE != null && cinrBlindScanLTE.size >= i && isValidNumeric(cinrBlindScanLTE(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelLTEBlindScan" + (i - 1) + "CINR", round(parseFloat(cinrBlindScanLTE(i - 1)), 2))
          }
        }
      }
    }

    val channelfrequencyBlindScanNR: java.lang.Integer = if (!row.isNullAt(row.fieldIndex("freqNRBlindScan"))) row.getInt(row.fieldIndex("freqNRBlindScan")) else null
    val cellIdBlindScanNR: List[String] = if (!row.isNullAt(row.fieldIndex("cellIdNRBlindScan"))) row.getSeq(row.fieldIndex("cellIdNRBlindScan")).toList else null
    val refPowerBlindScanNR: List[String] = if (!row.isNullAt(row.fieldIndex("primaryRsrpNRBlindScan"))) row.getSeq(row.fieldIndex("primaryRsrpNRBlindScan")).toList else null
    val refQualityBlindScanNR: List[String] = if (!row.isNullAt(row.fieldIndex("primaryRsrqNRBlindScan"))) row.getSeq(row.fieldIndex("primaryRsrqNRBlindScan")).toList else null
    val cinrBlindScanNR: List[String] = if (!row.isNullAt(row.fieldIndex("cinrNRBlindScan"))) row.getSeq(row.fieldIndex("cinrNRBlindScan")).toList else null
    val channelNumberBlindScanNR: java.lang.Integer = if (!row.isNullAt(row.fieldIndex("channelNumberNRBlindScan"))) row.getInt(row.fieldIndex("channelNumberNRBlindScan")) else null
    val bandwidthBlindScanNR: String = if (!row.isNullAt(row.fieldIndex("bandwidthNRBlindScan"))) row.getString(row.fieldIndex("bandwidthNRBlindScan")) else null
    //    logger.info(s" cellIdBlindScanNR=$cellIdBlindScanNR ")

    if (channelfrequencyBlindScanNR != null && channelfrequencyBlindScanNR > 0) {

      if (channelfrequencyBlindScanNR != null) indexMap.put("pctelNRBlindScanFrequency", channelfrequencyBlindScanNR.toInt)
      if (channelNumberBlindScanNR != null) indexMap.put("pctelNRBlindScanNumber", channelNumberBlindScanNR)
      if (bandwidthBlindScanNR != null) indexMap.put("pctelNRBlindScanBandwidth", bandwidthBlindScanNR.split("MHz")(0).toFloat)
      if (cellIdBlindScanNR != null) indexMap.put("pctelNRBlindScanNumSignals", cellIdBlindScanNR.size)

      if (cellIdBlindScanNR != null) {
        breakable {
          for (i <- 1 to cellIdBlindScanNR.size) {
            if (i > 8) {
              break
            }
            if (cellIdBlindScanNR != null && cellIdBlindScanNR.size >= i && isValidNumeric(cellIdBlindScanNR(i - 1), Constants.CONST_NUMBER)) indexMap.put("pctelNRBlindScan" + (i - 1) + "CellID", cellIdBlindScanNR(i - 1).toInt)
            if (refPowerBlindScanNR != null && refPowerBlindScanNR.size >= i && isValidNumeric(refPowerBlindScanNR(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelNRBlindScan" + (i - 1) + "RefPower", round(parseFloat(refPowerBlindScanNR(i - 1)), 2))
            if (refQualityBlindScanNR != null && refQualityBlindScanNR.size >= i && isValidNumeric(refQualityBlindScanNR(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelNRBlindScan" + (i - 1) + "RefQuality", round(parseFloat(refQualityBlindScanNR(i - 1)), 2))
            if (cinrBlindScanNR != null && cinrBlindScanNR.size >= i && isValidNumeric(cinrBlindScanNR(i - 1), Constants.CONST_FLOAT)) indexMap.put("pctelNRBlindScan" + (i - 1) + "CINR", round(parseFloat(cinrBlindScanNR(i - 1)), 2))
          }
        }
      }
    }

    indexMap
  }

  def getAutoTestTestKpis(row: Row): mutable.Map[String, Any] = {
    val indexMap: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()
    val nr_rsrq: java.lang.Float = if (!row.isNullAt(row.fieldIndex("nr_rsrq"))) row.getFloat(row.fieldIndex("nr_rsrq")) else null
    val nr_rsrp: java.lang.Float = if (!row.isNullAt(row.fieldIndex("nr_rsrp"))) row.getFloat(row.fieldIndex("nr_rsrp")) else null
    val nr_rssnr: java.lang.Float = if (!row.isNullAt(row.fieldIndex("nr_rssnr"))) row.getFloat(row.fieldIndex("nr_rssnr")) else null
    val lte_rsrq: java.lang.Float = if (!row.isNullAt(row.fieldIndex("lte_rsrq"))) row.getFloat(row.fieldIndex("lte_rsrq")) else null
    val lte_rsrp: java.lang.Float = if (!row.isNullAt(row.fieldIndex("lte_rsrp"))) row.getFloat(row.fieldIndex("lte_rsrp")) else null
    val lte_rssnr: java.lang.Float = if (!row.isNullAt(row.fieldIndex("lte_rssnr"))) row.getFloat(row.fieldIndex("lte_rssnr")) else null
    val lte_enbId: Integer = if (!row.isNullAt(row.fieldIndex("lte_enbId"))) row.getInt(row.fieldIndex("lte_enbId")) else null
    val cellId: Integer = if (!row.isNullAt(row.fieldIndex("cellId"))) row.getInt(row.fieldIndex("cellId")) else null
    val startTime: String = if (!row.isNullAt(row.fieldIndex("startTime"))) row.getString(row.fieldIndex("startTime")) else null
    val testName: String = if (!row.isNullAt(row.fieldIndex("testName"))) row.getString(row.fieldIndex("testName")) else null
    val resultType: String = if (!row.isNullAt(row.fieldIndex("resultType"))) row.getString(row.fieldIndex("resultType")) else null
    val avg_nr_rsrq: java.lang.Float = if (!row.isNullAt(row.fieldIndex("avg_nr_rsrq"))) row.getFloat(row.fieldIndex("avg_nr_rsrq")) else null
    val avg_nr_rsrp: java.lang.Float = if (!row.isNullAt(row.fieldIndex("avg_nr_rsrp"))) row.getFloat(row.fieldIndex("avg_nr_rsrp")) else null
    val avg_nr_rssnr: java.lang.Float = if (!row.isNullAt(row.fieldIndex("avg_nr_rssnr"))) row.getFloat(row.fieldIndex("avg_nr_rssnr")) else null
    val avg_lte_rsrq: java.lang.Float = if (!row.isNullAt(row.fieldIndex("avg_lte_rsrq"))) row.getFloat(row.fieldIndex("avg_lte_rsrq")) else null
    val avg_lte_rsrp: java.lang.Float = if (!row.isNullAt(row.fieldIndex("avg_lte_rsrp"))) row.getFloat(row.fieldIndex("avg_lte_rsrp")) else null
    val avg_lte_rssnr: java.lang.Float = if (!row.isNullAt(row.fieldIndex("avg_lte_rssnr"))) row.getFloat(row.fieldIndex("avg_lte_rssnr")) else null

    val task_time_median: java.lang.Float = if (!row.isNullAt(row.fieldIndex("task_time_median"))) row.getFloat(row.fieldIndex("task_time_median")) else null
    val task_time_05p: java.lang.Float = if (!row.isNullAt(row.fieldIndex("task_time_05p"))) row.getFloat(row.fieldIndex("task_time_05p")) else null
    val task_time_95p: java.lang.Float = if (!row.isNullAt(row.fieldIndex("task_time_95p"))) row.getFloat(row.fieldIndex("task_time_95p")) else null
    val access_success: java.lang.Float = if (!row.isNullAt(row.fieldIndex("access_success"))) row.getFloat(row.fieldIndex("access_success")) else null
    val task_success: java.lang.Float = if (!row.isNullAt(row.fieldIndex("task_success"))) row.getFloat(row.fieldIndex("task_success")) else null
    val ttc_median: java.lang.Float = if (!row.isNullAt(row.fieldIndex("ttc_median"))) row.getFloat(row.fieldIndex("ttc_median")) else null
    val ttc_05p: java.lang.Float = if (!row.isNullAt(row.fieldIndex("ttc_05p"))) row.getFloat(row.fieldIndex("ttc_05p")) else null
    val ttc_95p: java.lang.Float = if (!row.isNullAt(row.fieldIndex("ttc_95p"))) row.getFloat(row.fieldIndex("ttc_95p")) else null
    val iteration_id: java.lang.Integer = if (!row.isNullAt(row.fieldIndex("iteration_id"))) row.getInt(row.fieldIndex("iteration_id")) else null
    val seq_id: java.lang.Integer = if (!row.isNullAt(row.fieldIndex("seq_id"))) row.getInt(row.fieldIndex("seq_id")) else null
    val pingRtt: java.lang.Integer = if (!row.isNullAt(row.fieldIndex("pingRtt"))) row.getInt(row.fieldIndex("pingRtt")) else null
    val downloadTput: java.lang.Float = if (!row.isNullAt(row.fieldIndex("downloadTput"))) row.getFloat(row.fieldIndex("downloadTput")) else null
    val uploadTput: java.lang.Float = if (!row.isNullAt(row.fieldIndex("uploadTput"))) row.getFloat(row.fieldIndex("uploadTput")) else null

    if (nr_rsrq != null) { if(nr_rsrq>0.0) indexMap.put("nr_rsrq", nr_rsrq) }
    if (nr_rsrp != null)  { if(nr_rsrp>0.0) indexMap.put("nr_rsrp", nr_rsrp) }
    if (nr_rssnr != null) { if(nr_rssnr>0.0) indexMap.put("nr_rssnr", nr_rssnr) }
    if (lte_rsrq != null) { if(lte_rsrq>0.0) indexMap.put("lte_rsrq", lte_rsrq) }
    if (lte_rsrp != null) { if(lte_rsrp>0.0) indexMap.put("lte_rsrp", lte_rsrp) }
    if (lte_rssnr != null) { if(lte_rssnr>0.0) indexMap.put("lte_rssnr", lte_rssnr) }
    if (lte_enbId != null) { if(lte_enbId>0) indexMap.put("lte_enbId", lte_enbId) }
    if (cellId != null) { if(cellId>0) indexMap.put("cellId", cellId) }
    if (startTime != null) indexMap.put("startTime", startTime)
    if (testName != null) indexMap.put("testName", testName)
    if (resultType != null) indexMap.put("resultType", resultType)
    if (avg_nr_rsrq != null) { if(avg_nr_rsrq>0.0) indexMap.put("avg_nr_rsrq", avg_nr_rsrq) }
    if (avg_nr_rsrp != null) { if(avg_nr_rsrp>0.0) indexMap.put("avg_nr_rsrp", avg_nr_rsrp) }
    if (avg_nr_rssnr != null) { if(avg_nr_rssnr>0.0) indexMap.put("avg_nr_rssnr", avg_nr_rssnr) }
    if (avg_lte_rsrq != null) { if(avg_lte_rsrq>0.0) indexMap.put("avg_lte_rsrq", avg_lte_rsrq) }
    if (avg_lte_rsrp != null) { if(avg_lte_rsrp>0.0) indexMap.put("avg_lte_rsrp", avg_lte_rsrp) }
    if (avg_lte_rssnr != null) { if(avg_lte_rssnr>0.0) indexMap.put("avg_lte_rssnr", avg_lte_rssnr) }

    if (task_time_median != null) { if(task_time_median>0.0) indexMap.put("task_time_median", task_time_median) }
    if (task_time_05p != null) { if(task_time_05p>0.0) indexMap.put("task_time_05p", task_time_05p) }
    if (task_time_95p != null) { if(task_time_95p>0.0) indexMap.put("task_time_95p", task_time_95p) }
    if (access_success != null) { if(access_success>0.0) indexMap.put("access_success", access_success) }
    if (task_success != null) { if(task_success>0.0) indexMap.put("task_success", task_success) }
    if (ttc_median != null) { if(ttc_median>0.0) indexMap.put("ttc_median", ttc_median) }
    if (ttc_05p != null) { if(ttc_05p>0.0) indexMap.put("ttc_05p", ttc_05p) }
    if (ttc_95p != null) { if(ttc_95p>0.0) indexMap.put("ttc_95p", ttc_95p) }
    if (iteration_id != null) { if(iteration_id>0) indexMap.put("iteration_id", iteration_id) }
    if (seq_id != null) { if(seq_id>0) indexMap.put("TestSeq_id", seq_id) }
    if (pingRtt != null) { if(pingRtt>0) indexMap.put("pingRtt", pingRtt) }
    if (downloadTput != null) { if(downloadTput>0.0) indexMap.put("downloadTput", downloadTput) }
    if (uploadTput != null) { if(uploadTput>0.0) indexMap.put("uploadTput", uploadTput) }

    indexMap
  }

  def getTypeName(fileType: String, indexPrefix: String = ""): String = indexPrefix+"_doc"

  def getIndexName(timeStamp: String, Type: String, defaultIndex: String, osName: String): String = {
    var indexName: String = ""
    try {
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val parsedDate = dateFormat.parse(timeStamp)
      val cal: Calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
      cal.setTimeInMillis(parsedDate.getTime)
      var index: String = defaultIndex
      if (StringUtils.equalsIgnoreCase(Type.toLowerCase, Constants.CONST_SIG)) {
        index = "scanner"
      }
      else if (StringUtils.containsIgnoreCase(osName, Constants.CONST_IOS_NAME)) {
        index = "dmatps"
      }
      else if(StringUtils.containsIgnoreCase(Type.toLowerCase,Constants.CONST_VZKPIS)) {
        index = "vzkpis"
      }
      if ((cal.get(Calendar.MONTH) == 11) && (cal.get(Calendar.WEEK_OF_YEAR) == 1)) {
        cal.add(Calendar.YEAR, 1)
        indexName = index + "-" + cal.get(Calendar.YEAR) + "-" + cal.get(Calendar.WEEK_OF_YEAR)
      }
      else indexName = index + "-" + cal.get(Calendar.YEAR) + "-" + cal.get(Calendar.WEEK_OF_YEAR)
    }
    catch {
      case ge: Exception =>
        logger.error("ES Exception occured while executing getIndexName : " + ge.getMessage)
        println("ES Exception occured while executing getIndexName : " + ge.getMessage)
        logger.error("ES Exception occured while executing getIndexName : " + ge.printStackTrace())
    }
    indexName
  }
}


