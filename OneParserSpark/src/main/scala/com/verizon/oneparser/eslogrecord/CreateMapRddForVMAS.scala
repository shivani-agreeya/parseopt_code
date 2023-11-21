package com.verizon.oneparser.eslogrecord

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import com.dmat.qc.parser.ParseUtils
import com.esri.webmercator._
import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.common.Constants.{GeometryDto, gNodeIdInfo}
import com.verizon.oneparser.common.{CommonDaoImpl, Constants, LteIneffEventInfo}
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.ProcessDMATRecords._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.immutable.{List, Nil}
import scala.collection.mutable.Map
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}
import scala.math.max

object CreateMapRddForVMAS extends Serializable with LazyLogging {

  implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
  implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]



  def compress[A](l: List[A]):List[A] = l.foldRight(List[A]()) {
    case (e, ls) if (ls.isEmpty || ls.head != e) => e::ls
    case (e, ls) => ls
  }

  def getMapRdd(joinedAllTables: DataFrame, logRecordJoinedDF: LogRecordJoinedDF, repartitionNo: Int,reportFlag: Boolean, commonConfigParams:CommonConfigParameters): Dataset[Map[String, Any]] = {
    val mapRDD = joinedAllTables.repartition(repartitionNo.toInt).mapPartitions(iter => {

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

      def getCurrentdateTimeStamp: Timestamp = {
        val today = Calendar.getInstance()
        val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val now: String = timeFormat.format(today.getTime)
        val currentTime = java.sql.Timestamp.valueOf(now)
        currentTime
      }

      def getThrouput(Thruput: Double, timeSpan: Double): Double = {
        var throughput: Double = 0
        if (timeSpan > 0) {
          throughput = (Thruput * 8) * (1000 / timeSpan)
          throughput /= Constants.Mbps
        }
        throughput
      }

      iter.map(row => {
        var x: String = ""
        var y: String = ""
        val fileName = row.get(row.fieldIndex("fileName")).toString
        val fileNameWithExt = Constants.getDlfFromSeq(fileName)
        var fileType = fileNameWithExt.split('.')(1).toUpperCase

        if(fileType != "SIG") {
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
        } else {
          x = if (!row.isNullAt(row.fieldIndex("Longitude_sig"))) {
            row.getString(row.fieldIndex("Longitude_sig"))
          } else  ""
          y = if (!row.isNullAt(row.fieldIndex("Latitude_sig"))) {
            row.getString(row.fieldIndex("Latitude_sig"))
          } else  ""
        }


        var mx: java.lang.Double = 0.0
        if (x != null && !x.isEmpty) {
          mx = if (x.toDouble > -180 && x.toDouble < 180) x.toDouble toMercatorX else mx
        }
        var my: java.lang.Double = 0.0
        if (y != null && !y.isEmpty) {
          my = if(y.toDouble > -90 && y.toDouble <90) y.toDouble toMercatorY else my
        }

        val numRecords0xB139: Int = if (!row.isNullAt(row.fieldIndex("numRecords0xB139"))) row.getInt(row.fieldIndex("numRecords0xB139")) else 0
        val numRecords0xB173: Integer = if (!row.isNullAt(row.fieldIndex("numRecords0xB173"))) row.getInt(row.fieldIndex("numRecords0xB173")) else null
        val numRecords0xB16E: Int = if (!row.isNullAt(row.fieldIndex("numRecords0xB16E"))) row.getInt(row.fieldIndex("numRecords0xB16E")) else 0
        val numRecords0xB16F: Int = if (!row.isNullAt(row.fieldIndex("numRecords0xB16F"))) row.getInt(row.fieldIndex("numRecords0xB16F")) else 0
        val cqiCw0: Integer = if (!row.isNullAt(row.fieldIndex("cQICW0"))) row.getInt(row.fieldIndex("cQICW0")) else null
        val cqiCw1: Integer = if (!row.isNullAt(row.fieldIndex("cQICW1"))) row.getInt(row.fieldIndex("cQICW1")) else null
        val carrierIndex0xB126: String = if (!row.isNullAt(row.fieldIndex("carrierIndex0xB126"))) row.getString(row.fieldIndex("carrierIndex0xB126")) else ""

        var caIndex0xB126: String = null
        if (carrierIndex0xB126 != null) {
          if (carrierIndex0xB126 == "PCC" || carrierIndex0xB126 == "PCELL")
            caIndex0xB126 = "Pcc"
          if (carrierIndex0xB126 == "SCC1")
            caIndex0xB126 = "Scc1"
          if (carrierIndex0xB126 == "SCC2")
            caIndex0xB126 = "Scc2"
          if (carrierIndex0xB126 == "SCC3")
            caIndex0xB126 = "Scc3"
          if (carrierIndex0xB126 == "SCC4")
            caIndex0xB126 = "Scc4"
          if (carrierIndex0xB126 == "SCC5")
            caIndex0xB126 = "Scc5"
        }

        val lteeNBId: java.lang.Integer = if (!row.isNullAt(row.fieldIndex("LteeNBId_deriv"))) row.getInt(row.fieldIndex("LteeNBId_deriv")) else null
        val ulcarrierIndex: String = if (!row.isNullAt(row.fieldIndex("ulcarrierIndex")) && row.getList(row.fieldIndex("ulcarrierIndex")).size() > 0) row.getList(row.fieldIndex("ulcarrierIndex")).get(0).toString else ""

        val LteCellID: Int = if (!row.isNullAt(row.fieldIndex("LteCellID_deriv"))) row.getInt(row.fieldIndex("LteCellID_deriv")) else 0
        val PowerHeadRoomList: List[Long] = if (!row.isNullAt(row.fieldIndex("powerHeadRoom"))) row.getSeq(row.fieldIndex("powerHeadRoom")).toList else null
        val modulationType: List[String] = if (!row.isNullAt(row.fieldIndex("modulationType")) && row.getList(row.fieldIndex("modulationType")).size() > 0) row.getSeq(row.fieldIndex("modulationType")).toList else null

        val macDlNumSamples: Integer = if (!row.isNullAt(row.fieldIndex("MacDlNumSamples"))) row.getInt(row.fieldIndex("MacDlNumSamples")) else null
        val macDlSfnSize: Integer = if (!row.isNullAt(row.fieldIndex("MacDlSfn"))) row.getList(row.fieldIndex("MacDlSfn")).size() else null
        val macDlSubSfnSize: Integer = if (!row.isNullAt(row.fieldIndex("MacDlSubSfn"))) row.getList(row.fieldIndex("MacDlSubSfn")).size() else null
        val macDlLastSfn: Integer = if (!row.isNullAt(row.fieldIndex("MacDlSfn")) && row.getList(row.fieldIndex("MacDlSfn")).size() > 0) row.getList(row.fieldIndex("MacDlSfn")).get(macDlSfnSize - 1) else null
        val macDlLastSubSfn: Integer = if (!row.isNullAt(row.fieldIndex("MacDlSubSfn")) && row.getList(row.fieldIndex("MacDlSubSfn")).size() > 0) row.getList(row.fieldIndex("MacDlSubSfn")).get(macDlSubSfnSize - 1) else null
        val macDlfirstSfn: Integer = if (!row.isNullAt(row.fieldIndex("MacDlSfn")) && row.getList(row.fieldIndex("MacDlSfn")).size() > 0) row.getList(row.fieldIndex("MacDlSfn")).get(0) else null
        val macDlFirstSubSfn: Integer = if (!row.isNullAt(row.fieldIndex("MacDlSubSfn")) && row.getList(row.fieldIndex("MacDlSubSfn")).size() > 0) row.getList(row.fieldIndex("MacDlSubSfn")).get(0) else null
        val macDlTbs: List[Integer] = if (!row.isNullAt(row.fieldIndex("MacDlTbs"))) row.getSeq(row.fieldIndex("MacDlTbs")).toList else null

        val macUlNumSamples: Integer = if (!row.isNullAt(row.fieldIndex("MacUlNumSamples"))) row.getInt(row.fieldIndex("MacUlNumSamples")) else null
        val macUlSfnSize: Integer = if (!row.isNullAt(row.fieldIndex("MacUlSfn"))) row.getList(row.fieldIndex("MacUlSfn")).size() else null
        val macUlSubSfnSize: Integer = if (!row.isNullAt(row.fieldIndex("MacUlSubSfn"))) row.getList(row.fieldIndex("MacUlSubSfn")).size() else null
        val macUlLastSfn: Integer = if (!row.isNullAt(row.fieldIndex("MacUlSfn")) && row.getList(row.fieldIndex("MacUlSfn")).size() > 0) row.getList(row.fieldIndex("MacUlSfn")).get(macUlSfnSize - 1) else null
        val macUlLastSubFn: Integer = if (!row.isNullAt(row.fieldIndex("MacUlSubSfn")) && row.getList(row.fieldIndex("MacUlSubSfn")).size() > 0) row.getList(row.fieldIndex("MacUlSubSfn")).get(macUlSubSfnSize - 1) else null
        val macUlFirstSfn: Integer = if (!row.isNullAt(row.fieldIndex("MacUlSfn")) && row.getList(row.fieldIndex("MacUlSfn")).size() > 0) row.getList(row.fieldIndex("MacUlSfn")).get(0) else null
        val macUlFirstSubFn: Integer = if (!row.isNullAt(row.fieldIndex("MacUlSubSfn")) && row.getList(row.fieldIndex("MacUlSubSfn")).size() > 0) row.getList(row.fieldIndex("MacUlSubSfn")).get(0) else null
        val macUlGrant: List[Integer] = if (!row.isNullAt(row.fieldIndex("MacUlGrant"))) row.getSeq(row.fieldIndex("MacUlGrant")).toList else null

        var servingPCI: scala.collection.immutable.Map[Integer,GeometryDto] = null
        var neighboringPCI: scala.collection.immutable.Map[Integer,GeometryDto] = null

        if(logRecordJoinedDF.kpiGeoDistToCellsQuery){
          servingPCI = if (!row.isNullAt(row.fieldIndex("servingPciInfo"))) row.getMap(row.fieldIndex("servingPciInfo")).asInstanceOf[scala.collection.immutable.Map[Integer,GeometryDto]] else null
          neighboringPCI = if (!row.isNullAt(row.fieldIndex("NeighboringPciInfo"))) row.getMap(row.fieldIndex("NeighboringPciInfo")).asInstanceOf[scala.collection.immutable.Map[Integer,GeometryDto]] else null
        }

        var gnbidInfo: scala.collection.immutable.Map[Integer,gNodeIdInfo] = null

        if(logRecordJoinedDF.gNbidGeoDistToCellsQuery) {
          gnbidInfo = if (!row.isNullAt(row.fieldIndex("gNBidInfo"))) row.getMap(row.fieldIndex("gNBidInfo")).asInstanceOf[scala.collection.immutable.Map[Integer,gNodeIdInfo]] else null
        }
        var pdcpDlTotalBytes: java.lang.Long = null
        var prePdcpDlTotalBytes: java.lang.Long = null
        var pdcpDlTimeStamp: java.lang.Long = null
        var prePdcpDlTimeStamp: java.lang.Long = null
        var pdcpUlTotalBytes: java.lang.Long = null
        var prePdcpUlTotalBytes: java.lang.Long = null
        var pdcpUlTimeStamp: java.lang.Long = null
        var prePdcpUlTimeStamp: java.lang.Long = null
        var rlcDlTotalBytes: java.lang.Long = null
        var prerlcDlTotalBytes: java.lang.Long = null
        var rlcDlTimeStamp: java.lang.Long = null
        var prerlcDlTimeStamp: java.lang.Long = null
        var rlcUlTotalBytes: java.lang.Long = null
        var prerlcUlTotalBytes: java.lang.Long = null
        var rlcUlTimeStamp: java.lang.Long = null
        var prerlcUlTimeStamp: java.lang.Long = null

        var rlcDlBytes0xb087v49: List[java.lang.Long] = null
        var prerlcDlBytes0xb087v49: List[java.lang.Long] = null
        var rlcDlRbConfig0xb087v49: List[Integer] = null
        var prerlcDlRbConfig0xb087v49: List[Integer] = null
        var rlcDlNumRbs0xb087v49: java.lang.Integer = null
        var prerlcDlNumRbs0xb087v49: java.lang.Integer = null
        var rlcDlTimeStamp0xb087v49: java.lang.Long = null
        var preRlcDlTimeStamp0xb087v49: java.lang.Long = null

        rlcDlBytes0xb087v49 = if (!row.isNullAt(row.fieldIndex("rlcDlBytes"))) row.getSeq(row.fieldIndex("rlcDlBytes")).toList else null
        prerlcDlBytes0xb087v49 = if (!row.isNullAt(row.fieldIndex("prerlcDlBytes"))) row.getSeq(row.fieldIndex("prerlcDlBytes")).toList else null
        rlcDlRbConfig0xb087v49 = if (!row.isNullAt(row.fieldIndex("rlcRbConfigIndex"))) row.getSeq(row.fieldIndex("rlcRbConfigIndex")).toList else null
        prerlcDlRbConfig0xb087v49 = if (!row.isNullAt(row.fieldIndex("prerlcRbConfigIndex"))) row.getSeq(row.fieldIndex("prerlcRbConfigIndex")).toList else null
        rlcDlNumRbs0xb087v49 = if (!row.isNullAt(row.fieldIndex("rlcDlNumRBs"))) row.getInt(row.fieldIndex("rlcDlNumRBs")).asInstanceOf[Integer] else null
        prerlcDlNumRbs0xb087v49 = if (!row.isNullAt(row.fieldIndex("prerlcDlNumRBs"))) row.getInt(row.fieldIndex("prerlcDlNumRBs")).asInstanceOf[Integer]else null
        rlcDlTimeStamp0xb087v49 = if (!row.isNullAt(row.fieldIndex("rlcDlTimeStamp0xb087v49"))) row.getLong(row.fieldIndex("rlcDlTimeStamp0xb087v49")) else null
        preRlcDlTimeStamp0xb087v49 = if (!row.isNullAt(row.fieldIndex("prerlcDlTimeStamp0xb087v49"))) row.getLong(row.fieldIndex("prerlcDlTimeStamp0xb087v49")) else null

        pdcpDlTotalBytes = if (!row.isNullAt(row.fieldIndex("pdcpDlTotalBytes"))) row.getLong(row.fieldIndex("pdcpDlTotalBytes")) else null
        prePdcpDlTotalBytes = if (!row.isNullAt(row.fieldIndex("prePdcpDlTotalBytes"))) row.getLong(row.fieldIndex("prePdcpDlTotalBytes")) else null
        pdcpDlTimeStamp = if (!row.isNullAt(row.fieldIndex("pdcpDlTimeStamp"))) row.getLong(row.fieldIndex("pdcpDlTimeStamp")) else null
        prePdcpDlTimeStamp = if (!row.isNullAt(row.fieldIndex("prePdcpDlTimeStamp"))) row.getLong(row.fieldIndex("prePdcpDlTimeStamp")) else null
        pdcpUlTotalBytes = if (!row.isNullAt(row.fieldIndex("pdcpUlTotalBytes"))) row.getLong(row.fieldIndex("pdcpUlTotalBytes")) else null
        prePdcpUlTotalBytes = if (!row.isNullAt(row.fieldIndex("prePdcpUlTotalBytes"))) row.getLong(row.fieldIndex("prePdcpUlTotalBytes")) else null
        pdcpUlTimeStamp = if (!row.isNullAt(row.fieldIndex("pdcpUlTimeStamp"))) row.getLong(row.fieldIndex("pdcpUlTimeStamp")) else null
        prePdcpUlTimeStamp = if (!row.isNullAt(row.fieldIndex("prePdcpUlTimeStamp"))) row.getLong(row.fieldIndex("prePdcpUlTimeStamp")) else null

        rlcDlTotalBytes = if (!row.isNullAt(row.fieldIndex("rlcDlTotalBytes"))) row.getLong(row.fieldIndex("rlcDlTotalBytes")) else null
        prerlcDlTotalBytes = if (!row.isNullAt(row.fieldIndex("prerlcDlTotalBytes"))) row.getLong(row.fieldIndex("prerlcDlTotalBytes")) else null
        rlcDlTimeStamp = if (!row.isNullAt(row.fieldIndex("rlcDlTimeStamp"))) row.getLong(row.fieldIndex("rlcDlTimeStamp")) else null
        prerlcDlTimeStamp = if (!row.isNullAt(row.fieldIndex("prerlcDlTimeStamp"))) row.getLong(row.fieldIndex("prerlcDlTimeStamp")) else null
        rlcUlTotalBytes = if (!row.isNullAt(row.fieldIndex("rlcUlTotalBytes"))) row.getLong(row.fieldIndex("rlcUlTotalBytes")) else null
        prerlcUlTotalBytes = if (!row.isNullAt(row.fieldIndex("prerlcUlTotalBytes"))) row.getLong(row.fieldIndex("prerlcUlTotalBytes")) else null
        rlcUlTimeStamp = if (!row.isNullAt(row.fieldIndex("rlcUlTimeStamp"))) row.getLong(row.fieldIndex("rlcUlTimeStamp")) else null
        prerlcUlTimeStamp = if (!row.isNullAt(row.fieldIndex("prerlcUlTimeStamp"))) row.getLong(row.fieldIndex("prerlcUlTimeStamp")) else null

        var PUCCH_Tx_Power0xB16F: Int = if (!row.isNullAt(row.fieldIndex("PUCCH_Tx_Power0xB16F")) && row.getList(row.fieldIndex("PUCCH_Tx_Power0xB16F")).size() >= numRecords0xB16F) row.getList(row.fieldIndex("PUCCH_Tx_Power0xB16F")).get(numRecords0xB16F - 1).asInstanceOf[Int] else 0
        var DLPathLoss0xB16F: Int = if (!row.isNullAt(row.fieldIndex("DLPathLoss0xB16F")) && row.getList(row.fieldIndex("DLPathLoss0xB16F")).size() >= numRecords0xB16F && numRecords0xB16F > 0) row.getList(row.fieldIndex("DLPathLoss0xB16F")).get(numRecords0xB16F - 1).asInstanceOf[Int] else 0

        var p0_NominalPUCCH: String = ""

        var p0_NominalPUCCHVal: Integer = null
        var pucchAdjTxPwr: java.lang.Float = null
        var pucchTargetMobPwr: java.lang.Float = null
        var LTE_SSAC_Present: String = null
        var lteRrcConRestRej: Integer = null
        var lteRrcConRej: Integer = null
        var lteRrcConRelCount: java.lang.Long = null
        var lteRrcConReqCount: java.lang.Long = null
        var lteRrcConSetupCount: java.lang.Long = null
        var lteRrcConSetupCmpCount: java.lang.Long = null
        var lteSecModeCmdCount: java.lang.Long = null
        var lteSecModeCmpCount: java.lang.Long = null
        var lteRrcConReconfigCount: java.lang.Long = null
        var lteRrcConReconfCmpCount: java.lang.Long = null
        var lteUeCapEnqCount: java.lang.Long = null
        var lteUeCapInfoCount: java.lang.Long = null
        var nrRrcReConfigCount: java.lang.Long = null
        var nrRbConfigCount: java.lang.Long = null
        var nrRrcReConfigCmpCount: java.lang.Long = null
        var lteHoCmdCount: java.lang.Long = null
        var lteRrcConRestReqCount: java.lang.Long = null
        var lteRrcConRestCmpCount: java.lang.Long = null
        var lteRrcConRestCount: java.lang.Long = null
        var nrState: String = null
        var nrFailureType: String = null

        //5G SCG SP cell info
        var nrSpCellPci: Integer = null
        var nrSpCellDlFreq: Integer = null
        var nrSpCellBand: Integer = null
        var nrSpCellDuplexMode: String = null
        var nrSpCellDlBw: java.lang.Double = null
        var nrSpCellUlBw: java.lang.Double = null
        var nrSpCellUlFreq: Integer = null
        var MCC:String = ""
        var MNC:String = ""

        MCC = if (!row.isNullAt(row.fieldIndex("MCC"))) row.getInt(row.fieldIndex("MCC")).toString else ""
        MNC = if (!row.isNullAt(row.fieldIndex("MNC"))) row.getInt(row.fieldIndex("MNC")).toString else ""

        //                rRccOtaSubtitle = if (!row.isNullAt(row.fieldIndex("rRcOtaSubtitle"))) row.getString(row.fieldIndex("rRcOtaSubtitle")) else ""
        p0_NominalPUCCH = if (!row.isNullAt(row.fieldIndex("p0_NominalPUCCH"))) row.getString(row.fieldIndex("p0_NominalPUCCH")) else ""
        p0_NominalPUCCHVal = if (isValidNumeric(p0_NominalPUCCH, Constants.CONST_NUMBER)) p0_NominalPUCCH.toInt else null
        pucchAdjTxPwr = getPucchAdjTargetPwr(PUCCH_Tx_Power0xB16F, DLPathLoss0xB16F, p0_NominalPUCCHVal, "AdjTxPwr")
        pucchTargetMobPwr = getPucchAdjTargetPwr(PUCCH_Tx_Power0xB16F, DLPathLoss0xB16F, p0_NominalPUCCHVal, "TargetMobPwr")
        LTE_SSAC_Present = if (!row.isNullAt(row.fieldIndex("ssac_BarringForMMTEL")) && row.getBoolean(row.fieldIndex("ssac_BarringForMMTEL"))) "SSAC ON" else "SSAC OFF"
        lteRrcConRestRej = if (!row.isNullAt(row.fieldIndex("lteRrcConRestRej"))) row.getInt(row.fieldIndex("lteRrcConRestRej")) else null
        lteRrcConRej = if (!row.isNullAt(row.fieldIndex("lteRrcConRej"))) row.getInt(row.fieldIndex("lteRrcConRej")) else null
        nrSpCellPci = if (!row.isNullAt(row.fieldIndex("nrSpCellPci"))) row.getInt(row.fieldIndex("nrSpCellPci")) else null
        nrSpCellDlFreq = if (!row.isNullAt(row.fieldIndex("nrSpCellDlFreq"))) row.getInt(row.fieldIndex("nrSpCellDlFreq")) else null
        nrSpCellBand = if (!row.isNullAt(row.fieldIndex("nrSpCellBand"))) row.getInt(row.fieldIndex("nrSpCellBand")) else null
        nrSpCellDuplexMode = if (!row.isNullAt(row.fieldIndex("nrSpCellDuplexMode"))) row.getString(row.fieldIndex("nrSpCellDuplexMode")) else null
        nrSpCellDlBw = if (!row.isNullAt(row.fieldIndex("nrSpCellDlBw"))) row.getDouble(row.fieldIndex("nrSpCellDlBw")) else null
        nrSpCellUlBw = if (!row.isNullAt(row.fieldIndex("nrSpCellUlBw"))) row.getDouble(row.fieldIndex("nrSpCellUlBw")) else null
        nrSpCellUlFreq = if (!row.isNullAt(row.fieldIndex("nrSpCellUlFreq"))) row.getInt(row.fieldIndex("nrSpCellUlFreq")) else null
        lteRrcConRelCount = if (!row.isNullAt(row.fieldIndex("lteRrcConRelCnt"))) row.getLong(row.fieldIndex("lteRrcConRelCnt")) else null
        lteRrcConReqCount = if (!row.isNullAt(row.fieldIndex("lteRrcConReqCnt"))) row.getLong(row.fieldIndex("lteRrcConReqCnt")) else null
        lteRrcConSetupCount = if (!row.isNullAt(row.fieldIndex("lteRrcConSetupCnt"))) row.getLong(row.fieldIndex("lteRrcConSetupCnt")) else null
        lteRrcConSetupCmpCount = if (!row.isNullAt(row.fieldIndex("lteRrcConSetupCmpCnt"))) row.getLong(row.fieldIndex("lteRrcConSetupCmpCnt")) else null
        lteSecModeCmdCount = if (!row.isNullAt(row.fieldIndex("lteSecModeCmdCnt"))) row.getLong(row.fieldIndex("lteSecModeCmdCnt")) else null
        lteSecModeCmpCount = if (!row.isNullAt(row.fieldIndex("lteSecModeCmpCnt"))) row.getLong(row.fieldIndex("lteSecModeCmpCnt")) else null
        lteRrcConReconfigCount = if (!row.isNullAt(row.fieldIndex("lteRrcConReconfigCnt"))) row.getLong(row.fieldIndex("lteRrcConReconfigCnt")) else null
        lteRrcConReconfCmpCount = if (!row.isNullAt(row.fieldIndex("lteRrcConReconfCmpCnt"))) row.getLong(row.fieldIndex("lteRrcConReconfCmpCnt")) else null
        lteUeCapEnqCount = if (!row.isNullAt(row.fieldIndex("lteUeCapEnqCnt"))) row.getLong(row.fieldIndex("lteUeCapEnqCnt")) else null
        lteUeCapInfoCount = if (!row.isNullAt(row.fieldIndex("lteUeCapInfoCnt"))) row.getLong(row.fieldIndex("lteUeCapInfoCnt")) else null
        nrRrcReConfigCount = if (!row.isNullAt(row.fieldIndex("nrRrcReConfigCnt"))) row.getLong(row.fieldIndex("nrRrcReConfigCnt")) else null
        nrRbConfigCount = if (!row.isNullAt(row.fieldIndex("nrRbConfigCnt"))) row.getLong(row.fieldIndex("nrRbConfigCnt")) else null
        nrRrcReConfigCmpCount = if (!row.isNullAt(row.fieldIndex("nrRrcReConfigCmpCnt"))) row.getLong(row.fieldIndex("nrRrcReConfigCmpCnt")) else null
        lteHoCmdCount = if (!row.isNullAt(row.fieldIndex("lteHoCmdCnt"))) row.getLong(row.fieldIndex("lteHoCmdCnt")) else null
        lteRrcConRestReqCount = if (!row.isNullAt(row.fieldIndex("lteRrcConRestReqCnt"))) row.getLong(row.fieldIndex("lteRrcConRestReqCnt")) else null
        lteRrcConRestCmpCount = if (!row.isNullAt(row.fieldIndex("lterrcconrestcmpCnt"))) row.getLong(row.fieldIndex("lterrcconrestcmpCnt")) else null
        lteRrcConRestCount = if (!row.isNullAt(row.fieldIndex("lteRrcConRestCnt"))) row.getLong(row.fieldIndex("lteRrcConRestCnt")) else null
        nrState = if (!row.isNullAt(row.fieldIndex("nrState"))) row.getString(row.fieldIndex("nrState")) else null
        nrFailureType = if (!row.isNullAt(row.fieldIndex("nrFailureType"))) row.getString(row.fieldIndex("nrFailureType")) else null

        val EmmSubState: String = if (!row.isNullAt(row.fieldIndex("emmSubState"))) row.getString(row.fieldIndex("emmSubState")) else ""
        val frameNum: List[Int] = if (!row.isNullAt(row.fieldIndex("frameNum"))) row.getSeq(row.fieldIndex("frameNum")).toList else null
        val SubFrameNum: List[Int] = if (!row.isNullAt(row.fieldIndex("subFrameNum"))) row.getSeq(row.fieldIndex("subFrameNum")).toList else null
        val currentSFNSF: List[Int] = if (!row.isNullAt(row.fieldIndex("currentSFNSF"))) row.getSeq(row.fieldIndex("currentSFNSF")).toList else null

        var inbldngAccuracy: String = null
        var inbldngAltitude: String = null
        var inbldngImageId: Int = 0
        var inbldngUserMark: String = null
        var inbldngX: Double = 0
        var inbldngY: Double = 0

        // Setting inbuilding values
        if (logRecordJoinedDF.kpiisinbuilding) {
          inbldngAccuracy = if (!row.isNullAt(row.fieldIndex("inbldAccuracy"))) row.getString(row.fieldIndex("inbldAccuracy")) else ""
          inbldngAltitude = if (!row.isNullAt(row.fieldIndex("inbldAltitude"))) row.getString(row.fieldIndex("inbldAltitude")) else ""
          inbldngImageId = if (!row.isNullAt(row.fieldIndex("inbldImgId"))) row.getString(row.fieldIndex("inbldImgId")).toInt else 0
          inbldngUserMark = if (!row.isNullAt(row.fieldIndex("inbldUserMark"))) row.getString(row.fieldIndex("inbldUserMark")) else ""
          inbldngX = if (!row.isNullAt(row.fieldIndex("interpolated_x"))) row.getDouble(row.fieldIndex("interpolated_x")) else 0
          inbldngY = if (!row.isNullAt(row.fieldIndex("interpolated_y"))) row.getDouble(row.fieldIndex("interpolated_y")) else 0
        }

        val Gsm_Lai: List[Byte] = if (!row.isNullAt(row.fieldIndex("Lai"))) row.getSeq(row.fieldIndex("Lai")).toList else null
        val servingCellIndex: List[String] = if (!row.isNullAt(row.fieldIndex("ServingCellIndex"))) row.getSeq(row.fieldIndex("ServingCellIndex")).toList else null
        val tbsize0xB139: List[Int] = if (!row.isNullAt(row.fieldIndex("TB_Size0xB139"))) row.getSeq(row.fieldIndex("TB_Size0xB139")).toList else null
        val tbsize0xB173: List[Int] = if (!row.isNullAt(row.fieldIndex("TB_Size0xB173"))) row.getSeq(row.fieldIndex("TB_Size0xB173")).toList else null
        val CRCResult: List[String] = if (!row.isNullAt(row.fieldIndex("CrcResult"))) row.getSeq(row.fieldIndex("CrcResult")).toList else null
        val sysMode: Integer = if (!row.isNullAt(row.fieldIndex("mSYS_MODE"))) row.getInt(row.fieldIndex("mSYS_MODE")) else null
        val SrvStatus: Integer = if (!row.isNullAt(row.fieldIndex("mSRV_STATUS"))) row.getInt(row.fieldIndex("mSRV_STATUS")) else null
        val SrvStatus1: Integer = if (!row.isNullAt(row.fieldIndex("mSRV1_STATUS"))) row.getInt(row.fieldIndex("mSRV1_STATUS")) else null
        val HdrHybrid: Int = if (!row.isNullAt(row.fieldIndex("mHdrHybrid"))) row.getInt(row.fieldIndex("mHdrHybrid")) else 0

        val sysMode0_0x184E: Integer = if (!row.isNullAt(row.fieldIndex("stk_SYS_MODE0_0x184E"))) row.getInt(row.fieldIndex("stk_SYS_MODE0_0x184E")) else null
        val sysMode1_0x184E: Integer = if (!row.isNullAt(row.fieldIndex("stk_SYS_MODE1_0x184E"))) row.getInt(row.fieldIndex("stk_SYS_MODE1_0x184E")) else null
        val SrvStatus0_0x184E: Integer = if (!row.isNullAt(row.fieldIndex("SRV_STATUS0x184E"))) row.getInt(row.fieldIndex("SRV_STATUS0x184E")) else null
        val sysIDx184E: Int = if (!row.isNullAt(row.fieldIndex("sys_ID0x184E"))) row.getInt(row.fieldIndex("sys_ID0x184E")) else 0
        val sysModeOperational_0x184E: Int = if (!row.isNullAt(row.fieldIndex("sys_Mode_Operational_0x184E"))) row.getInt(row.fieldIndex("sys_Mode_Operational_0x184E")) else 0

        var version_B193: Int = 0
        var SubPacketSize: Int = 0
        var servingCellIndex_B193: String = null
        var earfcn_b193: String = null
        var RSRP: Array[(String, String)] = null
        var RSRPRx_0: Array[(String, String)] = null
        var RSRPRx_1: Array[(String, String)] = null
        var RSRPRx_2: Array[(String, String)] = null
        var RSRPRx_3: Array[(String, String)] = null
        var RSRQ: Array[(String, String)] = null
        var RSRQRx_0: Array[(String, String)] = null
        var RSRQRx_1: Array[(String, String)] = null
        var RSRQRx_2: Array[(String, String)] = null
        var RSRQRx_3: Array[(String, String)] = null
        var RSSI: Array[(String, String)] = null
        var RSSIRx_0: Array[(String, String)] = null
        var RSSIRx_1: Array[(String, String)] = null
        var RSSIRx_2: Array[(String, String)] = null
        var RSSIRx_3: Array[(String, String)] = null
        var FTLSNRRx_0: Array[(String, String)] = null
        var FTLSNRRx_1: Array[(String, String)] = null
        var FTLSNRRx_2: Array[(String, String)] = null
        var FTLSNRRx_3: Array[(String, String)] = null
        var PCI: Array[(String, Int)] = null
        var horxd_mode: Array[(String, Int)] = null
        var ValidRx: Array[(String, String)] = null

        var refPower_sig: Array[(String, String)] = null
        var refQuality_sig: Array[(String, String)] = null
        var cinr_sig: Array[(String, String)] = null
        var cellIdSig: String = null
        var refPowerSig: String = null
        var refQualitySig: String = null
        var cinrSig: String = null
        var frequencySig: String =  null
        var channelNumberSig: String = null
        var bandwidthSig: String = null
        var hexlogcodeSig: String = null



        version_B193 = if (!row.isNullAt(row.fieldIndex("version_B193"))) row.getInt(row.fieldIndex("version_B193")) else 0
        SubPacketSize = if (!row.isNullAt(row.fieldIndex("SubPacketSize_B193"))) {
          if (row.getList(row.fieldIndex("SubPacketSize_B193")).size() > 0) row.getList(row.fieldIndex("SubPacketSize_B193")).get(0).toString.toInt else 0
        } else 0
        var PhysicalCellID_b193: String = if (!row.isNullAt(row.fieldIndex("PhysicalCellID_B193"))) row.getString(row.fieldIndex("PhysicalCellID_B193")) else null
        earfcn_b193 = if (!row.isNullAt(row.fieldIndex("EARFCN"))) row.getString(row.fieldIndex("EARFCN")) else null
        var horxd_mode_b193: String = if (!row.isNullAt(row.fieldIndex("horxd_mode"))) row.getString(row.fieldIndex("horxd_mode")) else null
        var ValidRx_b193: String = if (!row.isNullAt(row.fieldIndex("validRx"))) row.getString(row.fieldIndex("validRx")) else null
        servingCellIndex_B193 = if (!row.isNullAt(row.fieldIndex("ServingCellIndex_B193"))) row.getString(row.fieldIndex("ServingCellIndex_B193")) else null
        var RSRP_b193: String = if (!row.isNullAt(row.fieldIndex("InstMeasuredRSRP"))) row.getString(row.fieldIndex("InstMeasuredRSRP")) else null
        var RSRPRx0_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSRPRx_0"))) row.getString(row.fieldIndex("InstRSRPRx_0")) else null
        var RSRPRx1_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSRPRx_1"))) row.getString(row.fieldIndex("InstRSRPRx_1")) else null
        var RSRPRx2_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSRPRx_2"))) row.getString(row.fieldIndex("InstRSRPRx_2")) else null
        var RSRPRx3_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSRPRx_3"))) row.getString(row.fieldIndex("InstRSRPRx_3")) else null
        var RSRQ_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSRQ"))) row.getString(row.fieldIndex("InstRSRQ")) else null
        var RSRQRx0_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSRQRx_0"))) row.getString(row.fieldIndex("InstRSRQRx_0")) else null
        var RSRQRx1_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSRQRx_1"))) row.getString(row.fieldIndex("InstRSRQRx_1")) else null
        var RSRQRx2_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSRQRx_2"))) row.getString(row.fieldIndex("InstRSRQRx_2")) else null
        var RSRQRx3_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSRQRx_3"))) row.getString(row.fieldIndex("InstRSRQRx_3")) else null
        var RSSI_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSSI"))) row.getString(row.fieldIndex("InstRSSI")) else null
        var RSSIRx0_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSSIRx_0"))) row.getString(row.fieldIndex("InstRSSIRx_0")) else null
        var RSSIRx1_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSSIRx_1"))) row.getString(row.fieldIndex("InstRSSIRx_1")) else null
        var RSSIRx2_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSSIRx_2"))) row.getString(row.fieldIndex("InstRSSIRx_2")) else null
        var RSSIRx3_b193: String = if (!row.isNullAt(row.fieldIndex("InstRSSIRx_3"))) row.getString(row.fieldIndex("InstRSSIRx_3")) else null
        var FTLSNRRx0_b193: String = if (!row.isNullAt(row.fieldIndex("FTLSNRRx_0"))) row.getString(row.fieldIndex("FTLSNRRx_0")) else null
        var FTLSNRRx1_b193: String = if (!row.isNullAt(row.fieldIndex("FTLSNRRx_1"))) row.getString(row.fieldIndex("FTLSNRRx_1")) else null
        var FTLSNRRx2_b193: String = if (!row.isNullAt(row.fieldIndex("FTLSNRRx_2"))) row.getString(row.fieldIndex("FTLSNRRx_2")) else null
        var FTLSNRRx3_b193: String = if (!row.isNullAt(row.fieldIndex("FTLSNRRx_3"))) row.getString(row.fieldIndex("FTLSNRRx_3")) else null

        //logger.info("0xB193: SubPacketSize= " + SubPacketSize + "; PhysicalCellID_b193= " + PhysicalCellID_b193 + "; servingCellIndex_B193= " + servingCellIndex_B193 + "; version_B193= " + version_B193 + "; RSRPRx0_b193= " + RSRPRx0_b193 + "; RSRQ_b193= " + RSRQ_b193)

        if(fileType=="sig") {
          cellIdSig = if (!row.isNullAt(row.fieldIndex("cellId_collectedSig"))) row.getString(row.fieldIndex("cellId_collectedSig")) else null
          refPowerSig = if (!row.isNullAt(row.fieldIndex("RefPower_collectedSig"))) row.getString(row.fieldIndex("RefPower_collectedSig")) else null
          refQualitySig = if (!row.isNullAt(row.fieldIndex("RefQuality_collectedSig"))) row.getString(row.fieldIndex("RefQuality_collectedSig")) else null
          cinrSig = if (!row.isNullAt(row.fieldIndex("CINR_collectedSig"))) row.getString(row.fieldIndex("CINR_collectedSig")) else null
          frequencySig = if (!row.isNullAt(row.fieldIndex("frequency_sig"))) row.getString(row.fieldIndex("frequency_sig")) else null
          channelNumberSig = if (!row.isNullAt(row.fieldIndex("channelNumber_sig"))) row.getString(row.fieldIndex("channelNumber_sig")) else null
          bandwidthSig = if (!row.isNullAt(row.fieldIndex("bandwidth_sig"))) row.getString(row.fieldIndex("bandwidth_sig")) else null
          hexlogcodeSig = if (!row.isNullAt(row.fieldIndex("hexLogCode_sig"))) row.getString(row.fieldIndex("hexLogCode_sig")) else null


          if (cellIdSig != null && refPowerSig != null) refPower_sig = getNrServingCellKPIs(cellIdSig, refPowerSig)
          if (cellIdSig != null && refQualitySig != null) refQuality_sig = getNrServingCellKPIs(cellIdSig, refQualitySig)
          if (cellIdSig != null && cinrSig != null) cinr_sig = getNrServingCellKPIs(cellIdSig, cinrSig)
        }

        if (servingCellIndex_B193 != null && (SubPacketSize > 0 || (version_B193 <= 18 && version_B193 > 0))) {
          if (PhysicalCellID_b193 != null) PCI = getServingCellPCI(servingCellIndex_B193, PhysicalCellID_b193)

          if (horxd_mode_b193 != null) horxd_mode = getServingCellPCI(servingCellIndex_B193, horxd_mode_b193)
          if (ValidRx_b193 != null) ValidRx = getServingCellCaKPIs(servingCellIndex_B193, ValidRx_b193)
          if (RSRP_b193 != null) RSRP = getServingCellCaKPIs(servingCellIndex_B193, RSRP_b193)
          if (RSRPRx0_b193 != null) RSRPRx_0 = getServingCellCaKPIs(servingCellIndex_B193, RSRPRx0_b193)
          if (RSRPRx1_b193 != null) RSRPRx_1 = getServingCellCaKPIs(servingCellIndex_B193, RSRPRx1_b193)
          if (RSRPRx2_b193 != null) RSRPRx_2 = getServingCellCaKPIs(servingCellIndex_B193, RSRPRx2_b193)
          if (RSRPRx3_b193 != null) RSRPRx_3 = getServingCellCaKPIs(servingCellIndex_B193, RSRPRx3_b193)
          if (RSRQ_b193 != null) RSRQ = getServingCellCaKPIs(servingCellIndex_B193, RSRQ_b193)
          if (RSRQRx0_b193 != null) RSRQRx_0 = getServingCellCaKPIs(servingCellIndex_B193, RSRQRx0_b193)
          if (RSRQRx1_b193 != null) RSRQRx_1 = getServingCellCaKPIs(servingCellIndex_B193, RSRQRx1_b193)
          if (RSRQRx2_b193 != null) RSRQRx_2 = getServingCellCaKPIs(servingCellIndex_B193, RSRQRx2_b193)
          if (RSRQRx3_b193 != null) RSRQRx_3 = getServingCellCaKPIs(servingCellIndex_B193, RSRQRx3_b193)
          if (RSSI_b193 != null) RSSI = getServingCellCaKPIs(servingCellIndex_B193, RSSI_b193)
          if (RSSIRx0_b193 != null) RSSIRx_0 = getServingCellCaKPIs(servingCellIndex_B193, RSSIRx0_b193)
          if (RSSIRx1_b193 != null) RSSIRx_1 = getServingCellCaKPIs(servingCellIndex_B193, RSSIRx1_b193)
          if (RSSIRx2_b193 != null) RSSIRx_2 = getServingCellCaKPIs(servingCellIndex_B193, RSSIRx2_b193)
          if (RSSIRx3_b193 != null) RSSIRx_3 = getServingCellCaKPIs(servingCellIndex_B193, RSSIRx3_b193)
          if (FTLSNRRx0_b193 != null) FTLSNRRx_0 = getServingCellCaKPIs(servingCellIndex_B193, FTLSNRRx0_b193)
          if (FTLSNRRx1_b193 != null) FTLSNRRx_1 = getServingCellCaKPIs(servingCellIndex_B193, FTLSNRRx1_b193)
          if (FTLSNRRx2_b193 != null) FTLSNRRx_2 = getServingCellCaKPIs(servingCellIndex_B193, FTLSNRRx2_b193)
          if (FTLSNRRx3_b193 != null) FTLSNRRx_3 = getServingCellCaKPIs(servingCellIndex_B193, FTLSNRRx3_b193)
        }

        var LTENormalServiceCount: Integer = null
        var WCDMANormalServiceCount: Integer = null
        var GSMNormalServiceCount: Integer = null
        var CDMANormalServiceCount: Integer = null
        var NoServiceCount: Integer = null
        var ltenormalcount = row.fieldIndex("lteNormalCount")
        var wcdmanormalcount = row.fieldIndex("wcdmaNormalCount")
        var gsmnormalcount = row.fieldIndex("gsmNormalCount")
        var cdmanormalcount = row.fieldIndex("cdmaNormalCount")
        var nosrvcount = row.fieldIndex("noSrvCount")
        LTENormalServiceCount = if (!row.isNullAt(ltenormalcount) && row.getLong(ltenormalcount).toInt != 0) row.getLong(ltenormalcount).toInt else null

        WCDMANormalServiceCount = if (!row.isNullAt(wcdmanormalcount) && row.getLong(wcdmanormalcount).toInt != 0) row.getLong(wcdmanormalcount).toInt else null

        GSMNormalServiceCount = if (!row.isNullAt(gsmnormalcount) && row.getLong(gsmnormalcount).toInt != 0) row.getLong(gsmnormalcount).toInt else null

        CDMANormalServiceCount = if (!row.isNullAt(cdmanormalcount) && row.getLong(cdmanormalcount).toInt != 0) row.getLong(cdmanormalcount).toInt else null

        NoServiceCount = if (!row.isNullAt(nosrvcount) && row.getLong(cdmanormalcount).toInt != 0) row.getLong(nosrvcount).toInt else null

        var version_B975: Int = 0
        var nrdlbler: String = null
        var nrresidualbler: String = null
        var nrdlmodtype: List[String] = null
        var nrsdlfrequency_B975: String = null
        var Nrs_brsrp: String = null
        var Nrs_brsrq: String = null
        var Nrs_bsnr: String = null
        var Nrs_beamcount: String = null
        var Nrs_ccindex: String = null
        var Nrs_pci: String = null
        var Nrs_ServingPciB97F: List[Integer] = null
        var NrPdschTputVal: java.lang.Float = null
        var NRServingBeamIndexVal: java.lang.Integer = null
        var servingBeamKpis0xB97f:scala.collection.mutable.Map[String,Any] = null

        Nrs_ServingPciB97F = if (!row.isNullAt(row.fieldIndex("nrservingcellpci_0xb97f"))) row.getSeq(row.fieldIndex("nrservingcellpci_0xb97f")).toList else null
        version_B975 = if (!row.isNullAt(row.fieldIndex("version_B975"))) row.getInt(row.fieldIndex("version_B975")) else 0
        nrdlbler = if (!row.isNullAt(row.fieldIndex("nrdlbler")) && row.getList(row.fieldIndex("nrdlbler")).size() > 0) row.getList(row.fieldIndex("nrdlbler")).get(0).toString else null
        nrresidualbler = if (!row.isNullAt(row.fieldIndex("nrresidualbler")) && row.getList(row.fieldIndex("nrresidualbler")).size() > 0) row.getList(row.fieldIndex("nrresidualbler")).get(0).toString else null
        nrdlmodtype = if (!row.isNullAt(row.fieldIndex("nrdlmodtype")) && row.getList(row.fieldIndex("nrdlmodtype")).size() > 0) row.getSeq(row.fieldIndex("nrdlmodtype")).toList else null
        nrsdlfrequency_B975 = if (!row.isNullAt(row.fieldIndex("nrsdlfrequency_B975"))) row.getString(row.fieldIndex("nrsdlfrequency_B975")) else null
        NrPdschTputVal = if (!row.isNullAt(row.fieldIndex("nrpdschtput"))) row.getDouble(row.fieldIndex("nrpdschtput")).toFloat else null

        if(Nrs_ServingPciB97F != null) {
          servingBeamKpis0xB97f = getServingBeam0xb97fKpis(row)
        } else {
          Nrs_brsrp = if (!row.isNullAt(row.fieldIndex("Serving_BRSRP"))) row.getString(row.fieldIndex("Serving_BRSRP")) else null
          Nrs_brsrq = if (!row.isNullAt(row.fieldIndex("Serving_BRSRQ"))) row.getString(row.fieldIndex("Serving_BRSRQ")) else null
          Nrs_bsnr = if (!row.isNullAt(row.fieldIndex("Serving_BSNR"))) row.getString(row.fieldIndex("Serving_BSNR")) else null
          Nrs_beamcount = if (!row.isNullAt(row.fieldIndex("Serving_nrbeamcount"))) row.getString(row.fieldIndex("Serving_nrbeamcount")) else null
          Nrs_ccindex = if (!row.isNullAt(row.fieldIndex("Serving_ccindex"))) row.getString(row.fieldIndex("Serving_ccindex")) else null
          Nrs_pci = if (!row.isNullAt(row.fieldIndex("Serving_nrpci"))) row.getString(row.fieldIndex("Serving_nrpci")) else null
          NRServingBeamIndexVal = if (!row.isNullAt(row.fieldIndex("NRServing_BeamIndex"))) row.getInt(row.fieldIndex("NRServing_BeamIndex")) else null

        }

        var rrcCon5gDlSessionDataBytes: java.lang.Long = null
        if(logRecordJoinedDF.kpi5gDlDataBytesExists) {
          rrcCon5gDlSessionDataBytes = if (!row.isNullAt(row.fieldIndex("sessionDlDataBytes"))) row.getLong(row.fieldIndex("sessionDlDataBytes")) else null
        }

        val EarfcnNcell: String = if (!row.isNullAt(row.fieldIndex("EARFCN_collected"))) row.getString(row.fieldIndex("EARFCN_collected")) else null
        val PciNcell: String = if (!row.isNullAt(row.fieldIndex("PCI_collected"))) row.getString(row.fieldIndex("PCI_collected")) else null
        val RrsrpNcell: String = if (!row.isNullAt(row.fieldIndex("Rsrp_collected"))) row.getString(row.fieldIndex("Rsrp_collected")) else null
        val RsrqNcell: String = if (!row.isNullAt(row.fieldIndex("Rsrq_collected"))) row.getString(row.fieldIndex("Rsrq_collected")) else null

        val VoLTE_Jitter: String = if (!row.isNullAt(row.fieldIndex("vvqJitter"))) row.getString(row.fieldIndex("vvqJitter")) else ""
        val VoLTE_Vvq: String = if (!row.isNullAt(row.fieldIndex("vvq"))) row.getString(row.fieldIndex("vvq")) else ""
        val nrScgCfgEvt: Integer =  if (!row.isNullAt(row.fieldIndex("nrScgCfgEvt"))) row.getInt(row.fieldIndex("nrScgCfgEvt")) else null

        if(nrScgCfgEvt != null) {
          println("nrScg event>>>>> " + nrScgCfgEvt)
        }

        var rtpdlsnPacketLoss: java.lang.Long = null
        var rtpPacketLoss_3Seconds: java.lang.Long = null

        if (logRecordJoinedDF.kpiDmatIpExists) {
          rtpdlsnPacketLoss = if (!row.isNullAt(row.fieldIndex("rtpdlsnCountAgg"))) row.getLong(row.fieldIndex("rtpdlsnCountAgg")) else null
          rtpPacketLoss_3Seconds = if (!row.isNullAt(row.fieldIndex("rtpPacketLoss_3Seconds"))) row.getLong(row.fieldIndex("rtpPacketLoss_3Seconds")) else null
        }

        var VoLTETriggerEvtRtpGap: java.lang.Integer = null
        var VoLTECallEndEvtRtpGap: java.lang.Integer = null
        var rtpGapEvt: String = null
        var VoLTECallStart: String = null
        var VoLTECallEnd: String = null
        var rtpGapIncomingDuration: Float = 0
        var rtpGapOutgoingDuration: Float = 0
        var VoLTESubtitleLast25Secs: String = null
        var VoLTESeqLast25Secs: String = null
        var NasMsgLast25Secs:String = null

        var VoLTERtpRxDeltaDelayInc: java.lang.Double = null
        var VoLTERtpRxDeltaDelayOut: java.lang.Double = null
        var VoLTERtpRxDelayInc: java.lang.Long = null
        var VoLTERtpRxDelayOut: java.lang.Long = null
        var volteCallIneffective: Boolean = false
        var rachResult0xb88a:String = null
        var nrbeamFailureLag:java.lang.Integer = null

        rachResult0xb88a = if (!row.isNullAt(row.fieldIndex("rachResult_0xb88a"))) row.getString(row.fieldIndex("rachResult_0xb88a")) else null
        nrbeamFailureLag = if (!row.isNullAt(row.fieldIndex("nrbeamFailureLag"))) row.getInt(row.fieldIndex("nrbeamFailureLag")) else null

        if(logRecordJoinedDF.kpiDmatIpExists) {
          VoLTETriggerEvtRtpGap = if (!row.isNullAt(row.fieldIndex("VoLTETriggerEvtRtpGap"))) row.getInt(row.fieldIndex("VoLTETriggerEvtRtpGap")) else null
          VoLTECallEndEvtRtpGap = if (!row.isNullAt(row.fieldIndex("VoLTECallEndEvtRtpGap"))) row.getInt(row.fieldIndex("VoLTECallEndEvtRtpGap")) else null
          rtpGapEvt = if (!row.isNullAt(row.fieldIndex("rtpGap"))) row.getString(row.fieldIndex("rtpGap")) else null

        }

        if(rachResult0xb88a != null && nrbeamFailureLag != null) {
          if(rachResult0xb88a=="ABORTED") {
            var evtInfo:LteIneffEventInfo = LteIneffEventInfo()
            volteCallIneffective = true
            val evtTimestamp = getTimestamp(row.getString(row.fieldIndex("dateWithoutMillis"))).getOrElse().asInstanceOf[Timestamp]
            evtInfo = evtInfo.copy(testId = row.getInt(row.fieldIndex("testId")),fileName = Constants.getDlfFromSeq(fileName), evtTimestamp, beamRecoveryFailEvt =1, fileLocation = row.getString(row.fieldIndex("fileLocation")))
            CommonDaoImpl.insertEventsInfoWhileES(commonConfigParams,evtInfo)
            CommonDaoImpl.updateEventsCountWhileES(commonConfigParams,evtInfo)
          }
        }

        if(logRecordJoinedDF.kpiDmatIpExists) {
          VoLTECallStart = if (!row.isNullAt(row.fieldIndex("VolteCallStart"))) row.getString(row.fieldIndex("VolteCallStart")) else null
          VoLTECallEnd = if (!row.isNullAt(row.fieldIndex("VolteCallEnd"))) row.getString(row.fieldIndex("VolteCallEnd")) else null
          rtpGapIncomingDuration = if (!row.isNullAt(row.fieldIndex("incomingGapTypeDuration"))) row.getLong(row.fieldIndex("incomingGapTypeDuration")).toFloat else 0
          rtpGapOutgoingDuration = if (!row.isNullAt(row.fieldIndex("outgoingGapTypeDuration"))) row.getLong(row.fieldIndex("outgoingGapTypeDuration")).toFloat else 0
          VoLTESubtitleLast25Secs = if (!row.isNullAt(row.fieldIndex("ipSubtitleMsgLast25Secs"))) row.getString(row.fieldIndex("ipSubtitleMsgLast25Secs")) else null
          VoLTESeqLast25Secs = if (!row.isNullAt(row.fieldIndex("sipCSeqInLast25Secs"))) row.getString(row.fieldIndex("sipCSeqInLast25Secs")) else null
          NasMsgLast25Secs = if (!row.isNullAt(row.fieldIndex("nasMsgTypeInLast25Secs"))) row.getString(row.fieldIndex("nasMsgTypeInLast25Secs")) else null
          VoLTERtpRxDeltaDelayInc = if (!row.isNullAt(row.fieldIndex("RtpRxDeltaDelayInc"))) row.getDouble(row.fieldIndex("RtpRxDeltaDelayInc")) else null
          VoLTERtpRxDeltaDelayOut = if (!row.isNullAt(row.fieldIndex("RtpRxDeltaDelayOut"))) row.getDouble(row.fieldIndex("RtpRxDeltaDelayOut")) else null
          VoLTERtpRxDelayInc = if (!row.isNullAt(row.fieldIndex("RtpRxDelayInc"))) row.getLong(row.fieldIndex("RtpRxDelayInc")) else null
          VoLTERtpRxDelayOut = if (!row.isNullAt(row.fieldIndex("RtpRxDelayOut"))) row.getLong(row.fieldIndex("RtpRxDelayOut")) else null
        }

        if(VoLTESubtitleLast25Secs != null && VoLTESeqLast25Secs != null) {

          if (checkVolteCallFailure(VoLTESubtitleLast25Secs,VoLTESeqLast25Secs,NasMsgLast25Secs)==1) {
            var evtInfo:LteIneffEventInfo = LteIneffEventInfo()
            volteCallIneffective = true
            val evtTimestamp = getTimestamp(row.getString(row.fieldIndex("dateWithoutMillis"))).getOrElse().asInstanceOf[Timestamp]
            evtInfo = evtInfo.copy(testId = row.getInt(row.fieldIndex("testId")),fileName = Constants.getDlfFromSeq(fileName), evtTimestamp, ltecallinefatmpt =1, fileLocation = row.getString(row.fieldIndex("fileLocation")))
            CommonDaoImpl.insertEventsInfoWhileES(commonConfigParams,evtInfo)
            CommonDaoImpl.updateEventsCountWhileES(commonConfigParams,evtInfo)
          }
        }

        var NrDlScg1SecTput:java.lang.Double = null
        var NrDlScg1SecPccTput:java.lang.Double = null
        var NrDlScg1SecScc1Tput:java.lang.Double = null
        var NrDlScg1SecScc2Tput:java.lang.Double = null
        var NrDlScg1SecScc3Tput:java.lang.Double = null
        var NrDlScg1SecScc4Tput:java.lang.Double = null
        var NrDlScg1SecScc5Tput:java.lang.Double = null
        var NrDlScg1SecScc6Tput	:java.lang.Double = null
        var NrDlScg1SecScc7Tput:java.lang.Double = null

        var carrierIDCount:String = null
        var carrierId0xB887: List[String] = null
        var Nrdlmcs0xB887:List[String] = null
        var carrierIDsFoAvgRb0xB887:String = null
        var numerologyForAvgRb0xB887:String = null
        var pccCarrierIdCount: String = null
        var scc1CarrierIdCount: String = null
        var scc2CarrierIdCount: String = null
        var scc3CarrierIdCount: String = null
        var scc4CarrierIdCount: String = null
        var scc5CarrierIdCount: String = null
        var scc6CarrierIdCount: String = null
        var scc7CarrierIdCount: String = null

        carrierId0xB887 = if (!row.isNullAt(row.fieldIndex("CarrierID0xB887")) &&  row.getList(row.fieldIndex("CarrierID0xB887")).size() > 0) row.getSeq(row.fieldIndex("CarrierID0xB887")).toList else null
        Nrdlmcs0xB887 = if (!row.isNullAt(row.fieldIndex("NRDLMCS0xB887")) && row.getList(row.fieldIndex("NRDLMCS0xB887")).size() > 0) row.getSeq(row.fieldIndex("NRDLMCS0xB887")).toList else null

        if(logRecordJoinedDF.kpi5gDlDataBytesExists) {
          carrierIDCount = if (!row.isNullAt(row.fieldIndex("carrierIDCount"))) row.getString(row.fieldIndex("carrierIDCount")) else null
          carrierIDsFoAvgRb0xB887 = if (!row.isNullAt(row.fieldIndex("carrierIDsFoAvgRbCal"))) row.getString(row.fieldIndex("carrierIDsFoAvgRbCal")) else null
          numerologyForAvgRb0xB887  = if (!row.isNullAt(row.fieldIndex("numerologyForAvgRbCal"))) row.getString(row.fieldIndex("numerologyForAvgRbCal")) else null


          if(carrierIDsFoAvgRb0xB887 != null && !carrierIDsFoAvgRb0xB887.isEmpty) {
            var countPerCarrierId = carrierIDsFoAvgRb0xB887.split(",").map(_.trim).toList.groupBy(i=>i).mapValues(_.size)
            pccCarrierIdCount = Try(countPerCarrierId.get("0").get.asInstanceOf[java.lang.Integer].toString).getOrElse("")
            scc1CarrierIdCount = Try(countPerCarrierId.get("1").get.asInstanceOf[java.lang.Integer].toString).getOrElse("")
            scc2CarrierIdCount = Try(countPerCarrierId.get("2").get.asInstanceOf[java.lang.Integer].toString).getOrElse("")
            scc3CarrierIdCount = Try(countPerCarrierId.get("3").get.asInstanceOf[java.lang.Integer].toString).getOrElse("")
            scc4CarrierIdCount = Try(countPerCarrierId.get("4").get.asInstanceOf[java.lang.Integer].toString).getOrElse("")
            scc5CarrierIdCount = Try(countPerCarrierId.get("5").get.asInstanceOf[java.lang.Integer].toString).getOrElse("")
            scc6CarrierIdCount = Try(countPerCarrierId.get("6").get.asInstanceOf[java.lang.Integer].toString).getOrElse("")
            scc7CarrierIdCount = Try(countPerCarrierId.get("7").get.asInstanceOf[java.lang.Integer].toString).getOrElse("")
          }
        }

        var NrDlScg5MsTput: java.lang.Double = null
        if(logRecordJoinedDF.kpiNrDlScg5MsTputExists) {
          NrDlScg5MsTput = if (!row.isNullAt(row.fieldIndex("tPUT5MilliSeconds"))) row.getDouble(row.fieldIndex("tPUT5MilliSeconds")) else null
          NrDlScg1SecTput = if (!row.isNullAt(row.fieldIndex("tPUT1Sec"))) row.getDouble(row.fieldIndex("tPUT1Sec")) else null
          NrDlScg1SecPccTput = if (!row.isNullAt(row.fieldIndex("tPUTPccSec"))) row.getDouble(row.fieldIndex("tPUTPccSec")) else null
          NrDlScg1SecScc1Tput = if (!row.isNullAt(row.fieldIndex("tPUTScc1Sec"))) row.getDouble(row.fieldIndex("tPUTScc1Sec")) else null
          NrDlScg1SecScc2Tput = if (!row.isNullAt(row.fieldIndex("tPUTScc2Sec"))) row.getDouble(row.fieldIndex("tPUTScc2Sec")) else null
          NrDlScg1SecScc3Tput = if (!row.isNullAt(row.fieldIndex("tPUTScc3Sec"))) row.getDouble(row.fieldIndex("tPUTScc3Sec")) else null
          NrDlScg1SecScc4Tput = if (!row.isNullAt(row.fieldIndex("tPUTScc4Sec"))) row.getDouble(row.fieldIndex("tPUTScc4Sec")) else null
          NrDlScg1SecScc5Tput = if (!row.isNullAt(row.fieldIndex("tPUTScc5Sec"))) row.getDouble(row.fieldIndex("tPUTScc5Sec")) else null
          NrDlScg1SecScc6Tput = if (!row.isNullAt(row.fieldIndex("tPUTScc6Sec"))) row.getDouble(row.fieldIndex("tPUTScc6Sec")) else null
          NrDlScg1SecScc7Tput = if (!row.isNullAt(row.fieldIndex("tPUTScc7Sec"))) row.getDouble(row.fieldIndex("tPUTScc7Sec")) else null
        }

        var DlL1PccTput:java.lang.Double = null
        var DlL1Scc1Tput:java.lang.Double = null
        var DlL1Scc2Tput:java.lang.Double = null
        var DlL1Scc3Tput:java.lang.Double = null
        var DlL1Scc4Tput:java.lang.Double = null
        var DlL1Scc5Tput:java.lang.Double = null
        var DlL1Scc6Tput:java.lang.Double = null
        var DlL1Scc7Tput	:java.lang.Double = null
        var overallDlL1Tput:java.lang.Double = null

        val tbSizePcellTPUT_0xB173: java.lang.Long = if (!row.isNullAt(row.fieldIndex("pcellTBSizeSum0xB173"))) row.getLong(row.fieldIndex("pcellTBSizeSum0xB173")) else null
        val tbSizeScc1TPUT_0xB173: java.lang.Long = if (!row.isNullAt(row.fieldIndex("scc1TBSizeSum0xB173"))) row.getLong(row.fieldIndex("scc1TBSizeSum0xB173")) else null
        val tbSizeScc2TPUT_0xB173: java.lang.Long = if (!row.isNullAt(row.fieldIndex("scc2TBSizeSum0xB173"))) row.getLong(row.fieldIndex("scc2TBSizeSum0xB173")) else null
        val tbSizeScc3TPUT_0xB173: java.lang.Long = if (!row.isNullAt(row.fieldIndex("scc3TBSizeSum0xB173"))) row.getLong(row.fieldIndex("scc3TBSizeSum0xB173")) else null
        val tbSizeScc4TPUT_0xB173: java.lang.Long = if (!row.isNullAt(row.fieldIndex("scc4TBSizeSum0xB173"))) row.getLong(row.fieldIndex("scc4TBSizeSum0xB173")) else null
        val tbSizeScc5TPUT_0xB173: java.lang.Long = if (!row.isNullAt(row.fieldIndex("scc5TBSizeSum0xB173"))) row.getLong(row.fieldIndex("scc5TBSizeSum0xB173")) else null
        val tbSizeScc6TPUT_0xB173: java.lang.Long = if (!row.isNullAt(row.fieldIndex("scc6TBSizeSum0xB173"))) row.getLong(row.fieldIndex("scc6TBSizeSum0xB173")) else null
        val tbSizeScc7TPUT_0xB173: java.lang.Long = if (!row.isNullAt(row.fieldIndex("scc7TBSizeSum0xB173"))) row.getLong(row.fieldIndex("scc7TBSizeSum0xB173")) else null
        val tbSizeOverallPUT_0xB173: java.lang.Long = if (!row.isNullAt(row.fieldIndex("totalTBSizeSum0xB173"))) row.getLong(row.fieldIndex("totalTBSizeSum0xB173")) else null

        val firstfn_0xB173: Integer  = if (!row.isNullAt(row.fieldIndex("firstFn_0xB173"))) row.getInt(row.fieldIndex("firstFn_0xB173")) else 0
        val firstSfn_0xB173: Integer  = if (!row.isNullAt(row.fieldIndex("firstSfn_0xB173"))) row.getInt(row.fieldIndex("firstSfn_0xB173")) else 0
        val lastfn_0xB173: Integer  = if (!row.isNullAt(row.fieldIndex("lastFn_0xB173"))) row.getInt(row.fieldIndex("lastFn_0xB173")) else 0
        val lastSfn_15kHzb173: Integer  = if (!row.isNullAt(row.fieldIndex("lastSfn_0xB173"))) row.getInt(row.fieldIndex("lastSfn_0xB173")) else 0

        val Total_elapsed_time = ParseUtils.CalculateMsFromSfnAndSubFrames(firstfn_0xB173,firstSfn_0xB173,lastfn_0xB173, lastSfn_15kHzb173)
        if(tbSizePcellTPUT_0xB173 != null) { DlL1PccTput = (tbSizePcellTPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeScc1TPUT_0xB173 != null) { DlL1Scc1Tput = (tbSizeScc1TPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeScc2TPUT_0xB173 != null) { DlL1Scc2Tput = (tbSizeScc2TPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeScc3TPUT_0xB173 != null) { DlL1Scc3Tput = (tbSizeScc3TPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeScc4TPUT_0xB173 != null) { DlL1Scc4Tput = (tbSizeScc4TPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeScc5TPUT_0xB173 != null) { DlL1Scc5Tput = (tbSizeScc5TPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeScc6TPUT_0xB173 != null) { DlL1Scc6Tput = (tbSizeScc6TPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeScc7TPUT_0xB173 != null) { DlL1Scc7Tput = (tbSizeScc7TPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeOverallPUT_0xB173 != null) { overallDlL1Tput = (tbSizeOverallPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}

        var isSysModeFrom0x184E: Boolean = false
        var isSysModeFrom0x134F: Boolean = false

        if (sysMode != null) {
          isSysModeFrom0x134F = true
        }
        if (sysMode0_0x184E != null) {
          isSysModeFrom0x184E = true
        }

        var NRS_RSRP: Array[(String, String)] = null
        var NRS_RSRQ: Array[(String, String)] = null
        var NRS_SNR: Array[(String, String)] = null
        var NRS_BeamCount: Array[(String, String)] = null
        var NRS_PCI: Array[(String, String)] = null

        if (Nrs_ccindex != null) {
          if (Nrs_brsrp != null) NRS_RSRP = getNrServingCellKPIs(Nrs_ccindex, Nrs_brsrp)
          if (Nrs_brsrq != null) NRS_RSRQ = getNrServingCellKPIs(Nrs_ccindex, Nrs_brsrq)
          if (Nrs_bsnr != null) NRS_SNR = getNrServingCellKPIs(Nrs_ccindex, Nrs_bsnr)
          if (Nrs_beamcount != null) NRS_BeamCount = getNrServingCellKPIs(Nrs_ccindex, Nrs_beamcount)
          if (Nrs_pci != null) NRS_PCI = getNrServingCellKPIs(Nrs_ccindex, Nrs_pci)
        }

        var lacId: Int = 0
        var plmnId: String = ""

        if (Gsm_Lai != null) {
          //logger.info("0x5B34 Process Dmat Gsm_Lai: "+ Gsm_Lai.size);
          //logger.info("0x5B34 Process Dmat Gsm_Lai: "+ Gsm_Lai);
          if (Gsm_Lai.size == 5) {
            var mcc: String = null
            var mnc: String = null
            var lac: String = null
            val ByteInHex0 = ParseUtils.byteToHex(Gsm_Lai(0))
            val ByteInHex1 = ParseUtils.byteToHex(Gsm_Lai(1))
            val ByteInHex2 = ParseUtils.byteToHex(Gsm_Lai(2))
            val ByteInHex3 = ParseUtils.byteToHex(Gsm_Lai(3))
            val ByteInHex4 = ParseUtils.byteToHex(Gsm_Lai(4))

            mcc = ByteInHex0.substring(1, 2) + ByteInHex0.substring(0, 1) + ByteInHex1.substring(1, 2)
            mnc = ByteInHex2.substring(1, 2) + ByteInHex2.substring(0, 1) + ByteInHex1.substring(0, 1)
            lac = ByteInHex3 + ByteInHex4
            val lacStr = "0x" + lac
            lacId = ParseUtils.intFromBytes(Gsm_Lai(3), Gsm_Lai(4))

            plmnId = "MCC/MNC: " + mcc + "/" + mnc
          }
        }

        var mccMnc: String = ""

        if ((MCC != null && !MCC.isEmpty) && (MNC != null && !MNC.isEmpty)) {
          mccMnc = MCC + "/" + MNC
        }

        var dlRlcTrupt: Double = 0

        try {
          if (rlcDlTotalBytes != null && prerlcDlTotalBytes != null && rlcDlTimeStamp != null && prerlcDlTimeStamp != null && (rlcDlTimeStamp > prerlcDlTimeStamp) && (rlcDlTotalBytes > prerlcDlTotalBytes)) {

            // logger.info("rlc: DlB = " + rlcDlTotalBytes + "; pDlTB = " + prerlcDlTotalBytes + "; TS = " + rlcDlTimeStamp + "; preTS = " + prerlcDlTimeStamp)
            var timeDiff = 0D
            var rlcDlBytesDiff = 0D
            timeDiff = (rlcDlTimeStamp.doubleValue() - prerlcDlTimeStamp.doubleValue()) / Constants.MS_IN_SEC
            rlcDlBytesDiff = rlcDlTotalBytes.doubleValue() - prerlcDlTotalBytes.doubleValue()

            dlRlcTrupt = (rlcDlBytesDiff * 8) / (Constants.Mbps * timeDiff)
          }
        } catch {
          case e: Exception =>
            logger.error("Error: Exception on rlc dl tput cal: " + row.get(row.fieldIndex("fileName")).toString)
            logger.info("rlc: Error DlB = " + rlcDlTotalBytes + "; pDlTB = " + prerlcDlTotalBytes + "; TS = " + rlcDlTimeStamp + "; preTS = " + prerlcDlTimeStamp)
        }

        try {
          if (rlcDlBytes0xb087v49 != null && prerlcDlBytes0xb087v49 != null && rlcDlRbConfig0xb087v49 != null && prerlcDlRbConfig0xb087v49 != null && (rlcDlTimeStamp0xb087v49 > preRlcDlTimeStamp0xb087v49)) {

            //   logger.info("rlc: DlB = " + rlcDlBytes0xb087v49 + "; pDlTB = " + prerlcDlBytes0xb087v49 + "; TS = " + rlcDlTimeStamp0xb087v49 + "; preTS = " + preRlcDlTimeStamp0xb087v49)
            var timeDiff = 0D
            var deltaRlcDataBytes = 0


            var rlcBytesIndexMap = (rlcDlRbConfig0xb087v49 zip rlcDlBytes0xb087v49).toMap
            var preRlcBytesIndexMap = (prerlcDlRbConfig0xb087v49 zip prerlcDlBytes0xb087v49).toMap
            var deltaRlcBytes = rlcBytesIndexMap -- preRlcBytesIndexMap.keySet

            if(deltaRlcBytes.size>0) {
              deltaRlcDataBytes += deltaRlcBytes.foldLeft(0){ case (a, (k, v)) => a+v.toInt }
            } else {
              var totalRlcBytes = rlcBytesIndexMap.foldLeft(0){ case (a, (k, v)) => a+v.toInt }
              var totalPreRlcBytes = preRlcBytesIndexMap.foldLeft(0){ case (a, (k, v)) => a+v.toInt }
              deltaRlcDataBytes += max(0, totalRlcBytes - totalPreRlcBytes)
            }

            timeDiff = (rlcDlTimeStamp0xb087v49.doubleValue() - preRlcDlTimeStamp0xb087v49.doubleValue()) / Constants.MS_IN_SEC
            dlRlcTrupt = (deltaRlcDataBytes * 8) / (Constants.Mbps * timeDiff)
          }
        } catch {
          case e: Exception =>
            logger.error("Exception for  rlc dl tput cal for 0xB087 log code version 49: " + e.getMessage)
            logger.error("Error: Exception on rlc dl tput cal for 0xB087 log code version 49: " + row.get(row.fieldIndex("fileName")).toString)
            logger.info("rlc: Error DlB 0xB087 log code version 49 = " + rlcDlBytes0xb087v49 + "; pDlTB = " + prerlcDlBytes0xb087v49 + "; TS = " + rlcDlTimeStamp0xb087v49 + "; preTS = " + preRlcDlTimeStamp0xb087v49 + "; RbConfig = " + rlcDlRbConfig0xb087v49 + "; preRbConfig = " + prerlcDlRbConfig0xb087v49)
        }

        var ulRlcTrupt: Double = 0

        try {
          if (rlcUlTotalBytes != null && prerlcUlTotalBytes != null && rlcUlTimeStamp != null && prerlcUlTimeStamp != null && (rlcUlTimeStamp > prerlcUlTimeStamp) && (rlcUlTotalBytes > prerlcUlTotalBytes)) {

            // logger.info("PDCP: DlB = " + rlcUlTotalBytes + "; pDlTB = " + prerlcUlTotalBytes + "; TS = " + rlcUlTimeStamp + "; preTS = " + prerlcUlTimeStamp)

            var timeDiff = 0D
            var rlcUlBytesDiff = 0D
            timeDiff = (rlcUlTimeStamp.doubleValue() - prerlcUlTimeStamp.doubleValue()) / Constants.MS_IN_SEC
            rlcUlBytesDiff = rlcUlTotalBytes.doubleValue() - prerlcUlTotalBytes.doubleValue()

            ulRlcTrupt = (rlcUlBytesDiff * 8) / (Constants.Mbps * timeDiff)
          }
        } catch {
          case e: Exception =>
            logger.error("Error: Exception on pdcp dl tput cal: " + row.get(row.fieldIndex("fileName")).toString)
            logger.info("RLC: Error DlB = " + rlcUlTotalBytes + "; pDlTB = " + prerlcUlTotalBytes + "; TS = " + rlcUlTimeStamp + "; preTS = " + prerlcUlTimeStamp)
        }

        var dlPdcpTrupt: Double = 0

        try {
          if (pdcpDlTotalBytes != null && prePdcpDlTotalBytes != null && pdcpDlTimeStamp != null && prePdcpDlTimeStamp != null && (pdcpDlTimeStamp > prePdcpDlTimeStamp) && (pdcpDlTotalBytes > prePdcpDlTotalBytes)) {

            // logger.info("PDCP: DlB = " + pdcpDlTotalBytes + "; pDlTB = " + prePdcpDlTotalBytes + "; TS = " + pdcpDlTimeStamp + "; preTS = " + prePdcpDlTimeStamp)

            var timeDiff = 0D
            var pdcpDlBytesDiff = 0D
            timeDiff = (pdcpDlTimeStamp.doubleValue() - prePdcpDlTimeStamp.doubleValue()) / Constants.MS_IN_SEC
            pdcpDlBytesDiff = pdcpDlTotalBytes.doubleValue() - prePdcpDlTotalBytes.doubleValue()

            dlPdcpTrupt = (pdcpDlBytesDiff * 8) / (Constants.Mbps * timeDiff)
          }
        } catch {
          case e: Exception =>
            logger.error("Error: Exception on pdcp dl tput cal: " + row.get(row.fieldIndex("fileName")).toString)
            logger.info("PDCP: Error DlB = " + pdcpDlTotalBytes + "; pDlTB = " + prePdcpDlTotalBytes + "; TS = " + pdcpDlTimeStamp + "; preTS = " + prePdcpDlTimeStamp)
        }

        var ulPdcpTrupt: Double = 0

        try {
          if (pdcpUlTotalBytes != null && prePdcpUlTotalBytes != null && pdcpUlTimeStamp != null && prePdcpUlTimeStamp != null && (pdcpUlTimeStamp > prePdcpUlTimeStamp) && (pdcpUlTotalBytes > prePdcpUlTotalBytes)) {
            // logger.info("PDCP: DlB = " + pdcpUlTotalBytes + "; pDlTB = " + prePdcpUlTotalBytes + "; TS = " + pdcpUlTimeStamp + "; preTS = " + prePdcpUlTimeStamp)

            var timeDiff = 0D
            var pdcpUlBytesDiff = 0D
            timeDiff = (pdcpUlTimeStamp.doubleValue() - prePdcpUlTimeStamp.doubleValue()) / Constants.MS_IN_SEC
            pdcpUlBytesDiff = pdcpUlTotalBytes.doubleValue() - prePdcpUlTotalBytes.doubleValue()

            ulPdcpTrupt = (pdcpUlBytesDiff * 8) / (Constants.Mbps * timeDiff)
          }
        } catch {
          case e: Exception =>
            logger.error("Error: Exception on pdcp ul tput cal: " + row.get(row.fieldIndex("fileName")).toString)
            logger.info("PDCP: Error UlB = " + pdcpUlTotalBytes + "; pUlTB = " + prePdcpUlTotalBytes + "; TS = " + pdcpUlTimeStamp + "; preTS = " + prePdcpUlTimeStamp)
        }

        var totalUlGrant = 0D

        if (macUlNumSamples != null && macUlGrant != null) {
          try {
            for (i <- 0 to macUlNumSamples - 1) {
              if (Try(macUlGrant(i)).isSuccess) {
                totalUlGrant += macUlGrant(i)
              }
            }
          }
          catch {
            case iobe: IndexOutOfBoundsException =>
              logger.error("IndexOutOfBoundsException exception occured for operation numSamplesUl&UlGrant : " + iobe.getMessage)
          }
        }

        var totalDlTbs = 0D

        if (macDlNumSamples != null && macDlTbs != null) {
          //logger.info("0xB063 PDMAT: macDlNumSamples = " + macDlNumSamples)
          try {
            for (i <- 0 to macDlNumSamples - 1) {
              if (Try(macDlTbs(i)).isSuccess) {
                totalDlTbs += macDlTbs(i)
              }
            }
            //logger.info("0xB063 PDMAT: totalDlTbs = " + totalDlTbs)
          }
          catch {
            case iobe: IndexOutOfBoundsException =>
              logger.error("IndexOutOfBoundsException exception occured for operation numSamplesDl&DlTbs : " + iobe.getMessage)
          }
        }

        var macUIThroughput: Double = 0

        if ((macUlFirstSfn != null && macUlFirstSubFn != null) && (macUlLastSfn != null && macUlLastSubFn != null)) {
          //logger.info("0xB064 PDMAT: macUlFirstSfn = " + macUlFirstSfn + "; macUlFirstSubFn = " + macUlFirstSubFn + "; macUlLastSfn = " + macUlLastSfn + "; macUlLastSubFn = " + macUlLastSubFn)

          val timeSpan = ParseUtils.CalculateMsFromSfnAndSubFrames(macUlFirstSfn, macUlFirstSubFn, macUlLastSfn, macUlLastSubFn)

          //logger.info("0xB064 PDMAT: timeSpan = " + timeSpan + "; totalUlGrant = " + totalUlGrant)

          if (totalUlGrant > 0) {
            macUIThroughput = getThrouput(totalUlGrant, timeSpan)
            //logger.info("0xB064 PDMAT: macUlThroughput = " + macUIThroughput)
          }
        }

        var macDlThroughput: Double = 0

        if ((macDlfirstSfn != null && macDlFirstSubSfn != null) && (macDlLastSfn != null && macDlLastSubSfn != null)) {
          //logger.info("0xB063 PDMAT: macDlfirstSfn = " + macDlfirstSfn + "; macDlFirstSubSfn = " + macDlFirstSubSfn + "; macDlLastSfn = " + macDlLastSfn + "; macDlLastSubSfn = " + macDlLastSubSfn)
          val timeSpan = ParseUtils.CalculateMsFromSfnAndSubFrames(macDlfirstSfn, macDlFirstSubSfn, macDlLastSfn, macDlLastSubSfn)

          //logger.info("0xB063 PDMAT: timeSpan = " + timeSpan + "; totalDlTbs = " + totalDlTbs)

          if (totalDlTbs > 0) {
            macDlThroughput = getThrouput(totalDlTbs, timeSpan)
            //logger.info("0xB063 PDMAT: macDlThroughput = " + macDlThroughput)
          }
        }

        var pUSCHSumPcc = 0
        var pUSCHPccTPUT: Double = 0
        var pUSCHSumScc1 = 0
        var pUSCHScc1TPUT: Double = 0
        var overallPUSCHTPUT: Double = 0D

        if (tbsize0xB139 != null && currentSFNSF != null) {

          if (currentSFNSF.nonEmpty) {

            for (i <- 0 to tbsize0xB139.size - 1) {
              if (ulcarrierIndex != null) {

                if (ulcarrierIndex == "PCC" || ulcarrierIndex == "PCELL") {
                  pUSCHSumPcc += tbsize0xB139(i)
                } else if (ulcarrierIndex == "SCC1") {
                  pUSCHSumScc1 += tbsize0xB139(i)
                }
              }

              val firstSfn = currentSFNSF.head / 10
              val firstSfnSubFN = currentSFNSF.head % 10

              val lastSfn = currentSFNSF(numRecords0xB139 - 1) / 10
              val lastSfnSubFN = currentSFNSF(numRecords0xB139 - 1) % 10
              var durationMs = 0D

              durationMs = ParseUtils.CalculateMsFromSfnAndSubFrames(firstSfn, firstSfnSubFN, lastSfn, lastSfnSubFN)

              if (durationMs > 0) {
                val PUSCHSum = pUSCHSumPcc + pUSCHSumScc1
                pUSCHPccTPUT = (pUSCHSumPcc * 8 * (1000 / durationMs.toFloat)) / Constants.Mbps
                pUSCHScc1TPUT = (pUSCHSumScc1 * 8 * (1000 / durationMs.toFloat)) / Constants.Mbps
                overallPUSCHTPUT = (PUSCHSum * 8 * (1000 / durationMs.toFloat)) / Constants.Mbps
              }
            }
          }
        }

        var pccPDSCHSum = 0
        var scc1PDSCHSum = 0
        var scc2PDSCHSum = 0
        var scc3PDSCHSum = 0
        var scc4PDSCHSum = 0
        var scc5PDSCHSum = 0
        var pDSCHCRCFail = 0
        var pDSCHCRCReceived = 0
        var pDSCHpCellTPUT: Double = 0
        var pDSCHsCell1TPUT: Double = 0
        var pDSCHsCell2TPUT: Double = 0
        var pDSCHsCell3TPUT: Double = 0
        var pDSCHsCell4TPUT: Double = 0
        var pDSCHsCell5TPUT: Double = 0
        var overallPDSCHTPUT: Double = 0D

        var pDSCHpccModType: Integer = null
        var pDSCHscc1ModType: Integer = null
        var pDSCHscc2ModType: Integer = null
        var pDSCHscc3ModType: Integer = null
        var pDSCHscc4ModType: Integer = null
        var pDSCHscc5ModType: Integer = null
        var pdschBLER: Float = 0

        try {
          if (numRecords0xB173 != null && numRecords0xB173 > 0) {

            //logger.info("Tput: -------------------------------------------------------------------------------------------------")
            //logger.info("Tput: dateWithoutMillis = " + getTimestamp(row.getString(row.fieldIndex("dateWithoutMillis"))))
            //logger.info("Tput: numRecords0xB173 = " + numRecords0xB173 + "; tbsize0xB173.size: " + tbsize0xB173.size)
            //logger.info("Tput: tbsize0xB173 Contents: " + tbsize0xB173)

            for (i <- 0 to numRecords0xB173 - 1) {

              if (servingCellIndex == null) {
                pccPDSCHSum += tbsize0xB173(i)
                pDSCHpccModType = getModulationType(modulationType(0))
              } else {
                if (servingCellIndex.size <= i || servingCellIndex(i) == null || servingCellIndex(i).isEmpty) {
                  logger.error("Error: Abnormal behaviour. File name: " + row.get(row.fieldIndex("fileName")).toString)
                } else {

                  // logger.info("Tput: servingCellIndex(" + i + ")= " + servingCellIndex(i) + "; tbsize0xB173(" + i + ")= " + tbsize0xB173(i))

                  servingCellIndex(i).toUpperCase match {
                    case "PCC" =>
                      if (CRCResult(i).contentEquals("Pass")) {
                        pccPDSCHSum += tbsize0xB173(i)
                        pDSCHpccModType = getModulationType(modulationType(0))
                      }
                    case "PCELL" =>
                      if (CRCResult(i).contentEquals("Pass")) {
                        pccPDSCHSum += tbsize0xB173(i)
                        pDSCHpccModType = getModulationType(modulationType(0))
                      }
                    case "SCC1" =>
                      if (CRCResult(i).contentEquals("Pass")) {
                        scc1PDSCHSum += tbsize0xB173(i)
                        pDSCHscc1ModType = getModulationType(modulationType(0))
                      }
                    case "SCC2" =>
                      if (CRCResult(i).contentEquals("Pass")) {
                        scc2PDSCHSum += tbsize0xB173(i)
                        pDSCHscc2ModType = getModulationType(modulationType(0))
                      }
                    case "SCC3" =>
                      if (CRCResult(i).contentEquals("Pass")) {
                        scc3PDSCHSum += tbsize0xB173(i)
                        pDSCHscc3ModType = getModulationType(modulationType(0))
                      }
                    case "SCC4" =>
                      if (CRCResult(i).contentEquals("Pass")) {
                        scc4PDSCHSum += tbsize0xB173(i)
                        pDSCHscc4ModType = getModulationType(modulationType(0))
                      }
                    case "SCC5" =>
                      if (CRCResult(i).contentEquals("Pass")) {
                        scc5PDSCHSum += tbsize0xB173(i)
                        pDSCHscc5ModType = getModulationType(modulationType(0))
                      }
                    case _ =>
                      logger.info("Entered into default case:  " + servingCellIndex(i))
                  }
                }
              }
            }

            if (frameNum != null && SubFrameNum != null) {

              var durationMs = 0D

              durationMs = ParseUtils.CalculateMsFromSfnAndSubFrames(frameNum.head, SubFrameNum.head, frameNum(numRecords0xB173 - 1), SubFrameNum(numRecords0xB173 - 1))

              if (durationMs > 0) {
                pDSCHpCellTPUT = (pccPDSCHSum * 8 * (1000 / durationMs)) / Constants.Mbps
                pDSCHsCell1TPUT = (scc1PDSCHSum * 8 * (1000 / durationMs)) / Constants.Mbps
                pDSCHsCell2TPUT = (scc2PDSCHSum * 8 * (1000 / durationMs)) / Constants.Mbps
                pDSCHsCell3TPUT = (scc3PDSCHSum * 8 * (1000 / durationMs)) / Constants.Mbps
                pDSCHsCell4TPUT = (scc4PDSCHSum * 8 * (1000 / durationMs)) / Constants.Mbps
                pDSCHsCell5TPUT = (scc5PDSCHSum * 8 * (1000 / durationMs)) / Constants.Mbps
                val PDSCHSum = pccPDSCHSum + scc1PDSCHSum + scc2PDSCHSum + scc3PDSCHSum + scc4PDSCHSum + scc5PDSCHSum
                overallPDSCHTPUT = (PDSCHSum * 8 * (1000 / durationMs.toFloat)) / Constants.Mbps

                //logger.info("Tput: durationMs = " + durationMs + "; frameNum.head = " + frameNum.head + "; SubFrameNum.head = " + SubFrameNum.head + "; Last frameNum = " + frameNum(numRecords0xB173 - 1) + "; Last SubFrameNum = " + SubFrameNum(numRecords0xB173 - 1))
                //logger.info("Tput: pccPDSCHSum = " + pccPDSCHSum + "; scc1PDSCHSum = " + scc1PDSCHSum + "; scc2PDSCHSum = " + scc2PDSCHSum + "; scc3PDSCHSum = " + scc3PDSCHSum + "; scc4PDSCHSum = " + scc4PDSCHSum + "; scc5PDSCHSum = " + scc5PDSCHSum)
                //logger.info("Tput: overallPDSCHTPUT = " + overallPDSCHTPUT + "; pCellTPUT = " + pCellTPUT + "; sCell1TPUT = " + sCell1TPUT + "; sCell2TPUT = " + sCell2TPUT + "; sCell3TPUT = " + sCell3TPUT + "; sCell4TPUT = " + sCell4TPUT + "; sCell5TPUT = " + sCell5TPUT)
              }
            }

            if (CRCResult != null) {
              pDSCHCRCReceived = 0
              pDSCHCRCFail = 0
              for (i <- 0 to CRCResult.size - 1) {
                pDSCHCRCReceived += 1
                if (CRCResult(i).contentEquals("Fail")) {
                  pDSCHCRCFail += 1
                }
              }
              if (pDSCHCRCReceived > 0)
                pdschBLER = (pDSCHCRCFail / pDSCHCRCReceived.toFloat) * Constants.Percent
            }

          }
        }
        catch {
          case e: Exception =>
            logger.error("Error: Exception during phy throughput calculation: " + row.get(row.fieldIndex("fileName")).toString)

        }

        val  mapWithoutMetadata = Map(
          "ObjectId" -> scala.util.Random.nextInt(),
          "TestId" -> row.getInt(row.fieldIndex("testId")),
          "FileName" -> Constants.getDlfFromSeq(fileName)) ++
          ((if (!reportFlag && row.getString(row.fieldIndex("dateWithoutMillis")) != null) Map("TimeStamp" -> getTimestamp(row.getString(row.fieldIndex("dateWithoutMillis"))).getOrElse(), "internal_index" -> getIndexName(row.getString(row.fieldIndex("dateWithoutMillis")), fileType, Constants.ES_VMAS_ROUTINE, StringUtils.EMPTY)) else Nil) ++
            (if (fileNameWithExt != null) Map("Type" -> fileType, "internal_type" -> getTypeName(fileType)) else Nil) ++
            (if(fileType != "SIG") Map("ProcessedTime" -> getCurrentdateTimeStamp) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("emailID"))) Map("EmailId" -> row.getString(row.fieldIndex("emailID"))) else Map("EmailId" -> "concessions@vzwdt.com")) ++
            (if (!row.isNullAt(row.fieldIndex("first_Name"))) Map("FirstName" -> row.getString(row.fieldIndex("first_Name"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("last_Name"))) Map("LastName" -> row.getString(row.fieldIndex("last_Name"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("mdn_logs"))) Map("MDN" -> row.getString(row.fieldIndex("mdn_logs"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("imei_logs"))) Map("Imei" -> row.getString(row.fieldIndex("imei_logs"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("technology_logs"))) Map("Technology" -> row.getString(row.fieldIndex("technology_logs"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("carrierName"))) Map("CarrierName" -> row.getString(row.fieldIndex("carrierName"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("modelName_logs"))) Map("ModelName" -> row.getString(row.fieldIndex("modelName_logs"))) else Nil) ++
            (if (VoLTERtpRxDelayInc != null) Map("VoLTERtpRxDelay" -> VoLTERtpRxDelayInc.toFloat) else Nil) ++

            (if (mx != 0 && my != 0) getLocValues(mx, my, x, y).filterKeys(Set("lat","lon")) else Nil) ++ //lat,long

            (if (lteeNBId != null) Map("LTEeNBId" -> lteeNBId) else Nil) ++
            (if (LteCellID != 0) Map("LTECellId" -> LteCellID) else Nil) ++
            (if (pDSCHpccModType != null) Map("LTEPccDlMcs" -> pDSCHpccModType) else Nil) ++
            (if (pDSCHscc1ModType != null) Map("LTEScc1DlMcs" -> pDSCHscc1ModType) else Nil) ++
            (if (VoLTE_Jitter != null && !VoLTE_Jitter.isEmpty && isValidNumeric(VoLTE_Jitter, Constants.CONST_FLOAT)) Map("VoLTERtpRxJitt" -> VoLTE_Jitter.toFloat) else Nil) ++
            (if (VoLTE_Vvq != null && !VoLTE_Vvq.isEmpty && isValidNumeric(VoLTE_Vvq, Constants.CONST_FLOAT)) Map("VoLTEVolteVVQ" -> VoLTE_Vvq.toFloat) else Nil) ++
            (if (cqiCw0 != null) Map("LTEPccCqiCw0" -> cqiCw0) else Nil) ++
            (if (cqiCw1 != null) Map("LTEPCccqiCw1" -> cqiCw1) else Nil) ++
            (if ((sysMode != null) && (isSysModeFrom0x134F && !isSysModeFrom0x184E)) {
              if (sysMode >= 0) getSysModeKPIs(sysMode, SrvStatus, HdrHybrid).filterKeys(Set("DataRat")) else Nil
            } //DataRat
            else {
              (if ((sysMode0_0x184E != null) && (isSysModeFrom0x134F || isSysModeFrom0x184E)) {
                if (sysMode0_0x184E >= 0) getSysModeKPIs0x134F(sysMode0_0x184E, sysMode1_0x184E, sysIDx184E, sysModeOperational_0x184E, EmmSubState, SrvStatus0_0x184E, SrvStatus1, HdrHybrid).filterKeys(Set("DataRat")) else Nil
              } else Nil) //DataRat
            }) ++

            (if (SubPacketSize > 0 || (version_B193 <= 18 && version_B193 > 0)) {
              val LTEServingCellsKpis = scala.collection.mutable.Map[String, Any]()
              var earfcn: Array[(String, Int)] = null

              if (servingCellIndex_B193 != null && earfcn_b193 != null) {
                earfcn = getServingCellPCI(servingCellIndex_B193, earfcn_b193)
              }

              if(PCI != null) {
                for (i <- 0 to (PCI.size - 1)) {
                  var Index: String = ""
                  var validRxIndex: String = ""
                  var servingPciDist: java.lang.Float = null

                  try {
                    if (PCI(i)._1 == "PCell") {
                      Index = "Pcc"
                    } else if (PCI(i)._1 == "SCC1") {
                      Index = "Scc1"
                      validRxIndex = "Scc1"
                    } else if (PCI(i)._1 == "SCC2") {
                      Index = "Scc2"
                      validRxIndex = "Scc2"
                    } else if (PCI(i)._1 == "SCC3") {
                      Index = "Scc3"
                      validRxIndex = "Scc3"
                    } else if (PCI(i)._1 == "SCC4") {
                      Index = "Scc4"
                      validRxIndex = "Scc4"
                    } else if (PCI(i)._1 == "SCC5") {
                      Index = "Scc5"
                      validRxIndex = "Scc5"
                    }

                    var FTLSNR: String = null
                    var bandIndicator: Integer = null

                    if (FTLSNRRx_0 != null && FTLSNRRx_1 != null && FTLSNRRx_0.size > i && FTLSNRRx_1.size > i) {
                      FTLSNR = if (FTLSNRRx_0(i)._2 > FTLSNRRx_1(i)._2) FTLSNRRx_0(i)._2 else FTLSNRRx_1(i)._2
                    }

                    if(earfcn != null && earfcn.size > i && earfcn(i)._2 != null)  {
                      bandIndicator = ParseUtils.getBandIndicator(earfcn(i)._2)
                    }


                    if (!PCI(i)._1.isEmpty) {
                      if(Index.nonEmpty) {
                        if(Index == "Pcc" && x.nonEmpty && y.nonEmpty && servingPCI != null){
                          var PciLocation = servingPCI.get(PCI(i)._2)
                          if(PciLocation.isDefined){
                            servingPciDist = DistanceFrmCellsToLoc(y.toDouble, x.toDouble, PciLocation.get.asInstanceOf[GenericRowWithSchema].getDouble(0), PciLocation.get.asInstanceOf[GenericRowWithSchema].getDouble(1))
                          }

                        }

                        if(Index == "Pcc")
                        LTEServingCellsKpis += (if (PCI(i)._2 >= 0) "LTE" + Index + "Pci" -> PCI(i)._2 else "LTE" + Index + "Pci" -> "",
                          if (RSRP != null && RSRP.size > i && RSRP(i)._2 != null && Index == "Scc1") "LTESCC1Rsrp" -> Try(RSRP(i)._2.toFloat).getOrElse("") else if (RSRP(i)._2 != null && Index != "Scc1") "LTE" + Index + "Rsrp" -> Try(RSRP(i)._2.toFloat).getOrElse("") else "LTE" + Index + "Rsrp" -> "",
                          if (RSRQ != null && RSRQ.size > i && RSRQ(i)._2 != null) "LTE" + Index + "Rsrq" -> Try(RSRQ(i)._2.toFloat).getOrElse("") else "LTE" + Index + "Rsrq" -> "",
                          if (RSSI != null && RSSI.size > i && RSSI(i)._2 != null) "LTE" + Index + "Rssi" -> Try(RSSI(i)._2.toFloat).getOrElse("") else "LTE" + Index + "Rssi" -> "",
                          if (FTLSNR != null) "LTE" + Index + "Sinr" -> Try(FTLSNR.toFloat).getOrElse("") else "LTE" + Index + "Sinr" -> "",
                          if (bandIndicator != null && bandIndicator != -1) "LTE" + Index + "BandInd" -> bandIndicator else "LTE" + Index + "BandInd" -> "")

                      }
                    }
                  } catch {
                    case ge: Exception =>
                      logger.error("ES Exception occured while executing getCaModeKPIs for Index : " + Index + " Message : " + ge.getMessage)
                      println("ES Exception occured while executing getCaModeKPIs : " + ge.getMessage)
                      logger.error("ES Exception occured while executing getCaModeKPIs Cause: " + ge.getCause())
                      logger.error("ES Exception occured while executing getCaModeKPIs StackTrace: " + ge.printStackTrace())
                  }
                }
              }
              LTEServingCellsKpis
            } else Nil) ++ (if (PciNcell != null && !PciNcell.isEmpty) {
            var Ncells = PciNcell.split(",").map(_.trim).toList.map(_.toInt)
            var Earfcn = EarfcnNcell.split(",").map(_.trim).toList.map(_.toInt)

            val NcellMap = scala.collection.mutable.Map[String, Any]()
            var neighboringPciDist: java.lang.Float = null
            var neighboringPciEnodebId: String = null
            var neighboringPciSector: java.lang.Integer = null
            var neighboringPciBand : java.lang.Integer = null

            breakable {
              for (i <- 1 to Ncells.size) {
                if (i > 20) {
                  break
                }

                if(Ncells != null && Ncells.nonEmpty && x.nonEmpty && y.nonEmpty && neighboringPCI != null){
                  var NcellPciLocation = neighboringPCI.get(PciNcell.split(",")(i - 1).toInt)
                  if(NcellPciLocation.isDefined) {
                    neighboringPciDist = DistanceFrmCellsToLoc(y.toDouble, x.toDouble, NcellPciLocation.get.asInstanceOf[GenericRowWithSchema].getDouble(0), NcellPciLocation.get.asInstanceOf[GenericRowWithSchema].getDouble(1))
                    neighboringPciEnodebId = NcellPciLocation.get.asInstanceOf[GenericRowWithSchema].getString(2)
                    neighboringPciSector = NcellPciLocation.get.asInstanceOf[GenericRowWithSchema].getInt(3)
                    neighboringPciBand = NcellPciLocation.get.asInstanceOf[GenericRowWithSchema].getInt(4)
                  }
                }

                NcellMap += (
                  if (Earfcn != null && Earfcn.nonEmpty) "NCell" + i + "EARFCN" -> EarfcnNcell.split(",")((Earfcn.size) - 1).toInt else "NCell" + i + "EARFCN" -> "",
                  if (Ncells != null && Ncells.nonEmpty) "NCell" + i + "PCI" -> PciNcell.split(",")(i - 1).toInt else "NCell" + i + "PCI" -> "",
                  if (RrsrpNcell != null && !RrsrpNcell.isEmpty) "NCell" + i + "RSRP" -> RrsrpNcell.split(",")(i - 1).toFloat else "NCell" + i + "RSRP" -> "",
                  if (RsrqNcell != null && !RsrqNcell.isEmpty) "NCell" + i + "RSRQ" -> RsrqNcell.split(",")(i - 1).toFloat else "NCell" + i + "RSRQ" -> ""//,
                )

                if (Ncells != null && Ncells.nonEmpty && neighboringPciEnodebId != null) NcellMap += "NCell" + i + "eNBId" -> neighboringPciEnodebId.split("_")(0)
                if (Ncells != null && Ncells.nonEmpty && neighboringPciEnodebId != null) NcellMap += "NCell" + i + "AtolSectorId" -> neighboringPciEnodebId.split("_")(1)
                if (Ncells != null && Ncells.nonEmpty && neighboringPciBand != null) NcellMap += "NCell" + i + "AtolBand" -> neighboringPciBand
                if (Ncells != null && Ncells.nonEmpty && neighboringPciSector != null) NcellMap += "NCell" + i + "AtolSector" -> neighboringPciSector

                neighboringPciEnodebId = null
                neighboringPciSector = null
                neighboringPciBand = null

              }
            }
            NcellMap

          } else Nil)
            )


        val MapWithoutMetaData = mapWithoutMetadata

        val mapWithMetadata =  MapWithoutMetaData ++ Map("document_metadata" -> MapWithoutMetaData.keys.toList)

        import org.json4s.jackson.Serialization
        implicit val formats = org.json4s.DefaultFormats

        val jsonStringWithoutMetaData = Serialization.write(mapWithoutMetadata -- Set("internal_type", "internal_index"))
        mapWithMetadata ++ Map("jsonString_WriteToHdfs" -> jsonStringWithoutMetaData)
      })
    })
    mapRDD
  }

  def getServingBeam0xb97fKpis(row: org.apache.spark.sql.Row): scala.collection.mutable.Map[String,Any] = {

    val indexMap: scala.collection.mutable.Map[String,Any] = scala.collection.mutable.Map()

    val  Nrs_brsrp_B97F: List[String]  = if (!row.isNullAt(row.fieldIndex("nrsbrsrp_0xb97f"))) row.getSeq(row.fieldIndex("nrsbrsrp_0xb97f")).toList else null
    val   Nrs_ccindex_B97F: List[Integer] = if (!row.isNullAt(row.fieldIndex("ccindex_0xb97f"))) row.getSeq(row.fieldIndex("ccindex_0xb97f")).toList else null
    val  Nrs_brsrq_B97F: List[String] = if (!row.isNullAt(row.fieldIndex("nrsbrsq_0xb97f"))) row.getSeq(row.fieldIndex("nrsbrsq_0xb97f")).toList else null
    val  Nrs_beamcount_B97F: List[Integer] = if (!row.isNullAt(row.fieldIndex("nrbeamcount_0xb97f"))) row.getSeq(row.fieldIndex("nrbeamcount_0xb97f")).toList else null
    val  Nrs_pci_B97F: List[Integer] = if (!row.isNullAt(row.fieldIndex("nrpci_0xb97f"))) row.getSeq(row.fieldIndex("nrpci_0xb97f")).toList else null
    val  Nrs_servingbeamindex_B97F: List[Integer] = if (!row.isNullAt(row.fieldIndex("nrservingbeamindex_0xb97f"))) row.getSeq(row.fieldIndex("nrservingbeamindex_0xb97f")).toList else null
    val  Nrs_servingpci_B97F: List[Integer] = if (!row.isNullAt(row.fieldIndex("nrservingcellpci_0xb97f"))) row.getSeq(row.fieldIndex("nrservingcellpci_0xb97f")).toList else null

    var servingbeamindex:Integer = null
    if(Nrs_servingbeamindex_B97F != null) { if(Nrs_servingbeamindex_B97F.size>0) servingbeamindex = Nrs_servingbeamindex_B97F(0) }

    if(Nrs_beamcount_B97F != null && Nrs_servingbeamindex_B97F != null && servingbeamindex != null) {
      var counter = 0
      var totalBeamCount =0
      for (i <-0 to  Nrs_beamcount_B97F.size -1) {
        var beamCount = Nrs_beamcount_B97F(i).toInt
        var servingcellIndex = Nrs_ccindex_B97F(0)
        var sevingcellpci = Nrs_servingpci_B97F(0)

        if(i<1 && servingcellIndex==0) {
          var pci = Nrs_pci_B97F.take(beamCount)
          val bsrspMap = pci.zip(Nrs_brsrp_B97F.take(beamCount)).toMap
          val bsrsqMap = pci.zip(Nrs_brsrq_B97F.take(beamCount)).toMap
          if(servingbeamindex.toInt != 255) {
            if(bsrspMap!=null) { if(bsrspMap.contains(sevingcellpci)) { if(isValidNumeric(bsrspMap(sevingcellpci),"FLOAT")) { indexMap.put("NRSBRSRP",bsrspMap(sevingcellpci).toFloat) }}}
            if(bsrsqMap!=null) { if(bsrsqMap.contains(sevingcellpci))  { if(isValidNumeric(bsrsqMap(sevingcellpci),"FLOAT")) {indexMap.put("NRSBRSRQ",bsrsqMap(sevingcellpci).toFloat) }}}
            if(beamCount!=null) indexMap.put("NRBEAMCOUNT",beamCount)
            if(sevingcellpci!=null) indexMap.put("NRSPHYCELLID",sevingcellpci)
            if(servingbeamindex!=null) indexMap.put("NRServingBeamIndex",servingbeamindex.toInt)
          }
        } else if(i>=1 && i==servingcellIndex) {
          var pci = Nrs_pci_B97F.slice(totalBeamCount, Nrs_beamcount_B97F(i).toInt + totalBeamCount)
          val bsrspMap = pci.zip(Nrs_brsrp_B97F.slice(totalBeamCount, Nrs_beamcount_B97F(i).toInt + totalBeamCount)).toMap
          val bsrsqMap = pci.zip(Nrs_brsrq_B97F.slice(totalBeamCount, Nrs_beamcount_B97F(i).toInt + totalBeamCount)).toMap
          if(bsrspMap.contains(sevingcellpci)) {
            if(servingbeamindex.toInt != 255){
              if(bsrspMap!=null) { if(bsrspMap.contains(sevingcellpci)) { if(isValidNumeric(bsrspMap(sevingcellpci),"FLOAT")) { indexMap.put("NRSBRSRP",bsrspMap(sevingcellpci).toFloat) }}}
              if(bsrsqMap!=null) { if(bsrsqMap.contains(sevingcellpci))  { if(isValidNumeric(bsrsqMap(sevingcellpci),"FLOAT")) {indexMap.put("NRSBRSRQ",bsrsqMap(sevingcellpci).toFloat) }}}
              if(beamCount!=null) indexMap.put("NRBEAMCOUNT",beamCount)
              if(sevingcellpci!=null) indexMap.put("NRSPHYCELLID",sevingcellpci)
              if(servingbeamindex!=null) indexMap.put("NRServingBeamIndex",servingbeamindex.toInt)
            }
          }

        }
        totalBeamCount += beamCount
      }
    }
    indexMap
  }

  def getTypeName(fileType: String, indexPrefix: String = ""): String = if (fileType == "SIG") indexPrefix+"sacnnerdoc" else indexPrefix+"_doc"
  def getIndexName(timeStamp: String, Type: String, defaultIndex: String,osName:String): String = {
    var indexName: String = ""
    try {
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val parsedDate = dateFormat.parse(timeStamp)
      val cal: Calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
      cal.setTimeInMillis(parsedDate.getTime)
      var index: String = defaultIndex
      if(StringUtils.equalsIgnoreCase(Type.toLowerCase,Constants.CONST_SIG)){
        index = "scanner"
      }
      else if(StringUtils.containsIgnoreCase(osName,Constants.CONST_IOS_NAME)){
        index = "dmatps"
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
