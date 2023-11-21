package com.verizon.oneparser.eslogrecord

import java.lang
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import com.dmat.qc.parser.ParseUtils
import com.esri.webmercator._
import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.common.Constants.{GeometryDto, StateRegionCounty, gNodeIdInfo}
import com.verizon.oneparser.common.{CommonDaoImpl, Constants, LVFileMetaInfo, LteIneffEventInfo, ncellKpiInfo}
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.ProcessDMATRecords._
import com.verizon.oneparser.eslogrecord.CreateMapRddForSigFile.getTimestampPG
import com.verizon.oneparser.parselogs.LogRecordParser5G.parseDouble
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import java.time.ZonedDateTime
import scala.collection.immutable.{List, Nil}
import scala.collection.mutable
import scala.collection.mutable.Map
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}
import scala.math.max

object CreateMapRdd extends Serializable with LazyLogging {

  implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
  implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]


  def compress[A](l: List[A]): List[A] = l.foldRight(List[A]()) {
    case (e, ls) if (ls.isEmpty || ls.head != e) => e :: ls
    case (e, ls) => ls
  }

  def getMapRdd(joinedAllTables: DataFrame, logRecordJoinedDF: LogRecordJoinedDF, repartitionNo: Int, reportFlag: Boolean, commonConfigParams: CommonConfigParameters): Dataset[Map[String, Any]] = {
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
        var fileType: String = ""
        var area = "";

        fileType = fileNameWithExt.split('.')(1).toUpperCase
        if(fileType == "SIG") fileType = "DLF"
        if(fileName != null && fileType == "DML_DLF") {
          var market = fileName.split("__")(1)
          if(market.nonEmpty) area = market.split("-")(0)
        }

        def checkSIG = {
        // if(fileType=="sig") { fileType = "SIG" }
        if (fileType != "SIG") {
          x = if (!row.isNullAt(row.fieldIndex("Longitude"))) {
            row.getString(row.fieldIndex("Longitude"))
          } else if (!row.isNullAt(row.fieldIndex("Longitude2"))) {
            row.getString(row.fieldIndex("Longitude2"))
          } else ""
          y = if (!row.isNullAt(row.fieldIndex("Latitude"))) {
            row.getString(row.fieldIndex("Latitude"))
          } else if (!row.isNullAt(row.fieldIndex("Latitude2"))) {
            row.getString(row.fieldIndex("Latitude2"))
          } else ""
          //TODO dont execute for reports
          if (!reportFlag) {
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
              } else ""
            }

          }
          }
        }
        checkSIG

        var mx: lang.Double = 0.0
        if (x != null && !x.isEmpty) {
          mx = if (x.toDouble > -180 && x.toDouble < 180) x.toDouble toMercatorX else mx
        }

        var my: lang.Double = 0.0
        if (y != null && !y.isEmpty) {
          my = if (y.toDouble > -90 && y.toDouble < 90) y.toDouble toMercatorY else my
        }

        val vz_regions: String = if (!row.isNullAt(row.fieldIndex("vz_regions"))) row.getString(row.fieldIndex("vz_regions")) else null
        val CountryCode: String = if (!row.isNullAt(row.fieldIndex("CountryCode"))) row.getString(row.fieldIndex("CountryCode")) else null
        val stusps: String = if (!row.isNullAt(row.fieldIndex("stusps"))) row.getString(row.fieldIndex("stusps")) else null
        val countyns: String = if (!row.isNullAt(row.fieldIndex("countyns"))) row.getString(row.fieldIndex("countyns")) else null
        val countLogcodeB126: Long = if (!row.isNullAt(row.fieldIndex("countLogcodeB126"))) row.getLong(row.fieldIndex("countLogcodeB126")) else 0
        val numRecords0xB139: Int = if (!row.isNullAt(row.fieldIndex("numRecords0xB139"))) row.getInt(row.fieldIndex("numRecords0xB139")) else 0
        val numRecords0xB172: Int = if (!row.isNullAt(row.fieldIndex("numRecords0xB172"))) row.getInt(row.fieldIndex("numRecords0xB172")) else 0
        val numRecords0xB173: Integer = if (!row.isNullAt(row.fieldIndex("numRecords0xB173"))) row.getInt(row.fieldIndex("numRecords0xB173")) else null
        val numRecords0xB16E: Int = if (!row.isNullAt(row.fieldIndex("numRecords0xB16E"))) row.getInt(row.fieldIndex("numRecords0xB16E")) else 0
        val numRecords0xB16F: Int = if (!row.isNullAt(row.fieldIndex("numRecords0xB16F"))) row.getInt(row.fieldIndex("numRecords0xB16F")) else 0
        val numRecords0xB18A: Int = if (!row.isNullAt(row.fieldIndex("numRecords0xB18A"))) row.getInt(row.fieldIndex("numRecords0xB18A")) else 0
        val carrierIndex: String = if (!row.isNullAt(row.fieldIndex("carrierIndex"))) row.getString(row.fieldIndex("carrierIndex")) else ""
        val carrierIndex0xB14D: String = if (!row.isNullAt(row.fieldIndex("carrierIndex0xB14D"))) row.getString(row.fieldIndex("carrierIndex0xB14D")) else ""
        val cqiCw0: Integer = if (!row.isNullAt(row.fieldIndex("cQICW0"))) row.getInt(row.fieldIndex("cQICW0")) else null
        val cqiCw1: Integer = if (!row.isNullAt(row.fieldIndex("cQICW1"))) row.getInt(row.fieldIndex("cQICW1")) else null
        val scc1CqiCw0: Integer = if (!row.isNullAt(row.fieldIndex("scc1CQICW0"))) row.getInt(row.fieldIndex("scc1CQICW0")) else null
        val scc1CqiCw1: Integer = if (!row.isNullAt(row.fieldIndex("scc1CQICW1"))) row.getInt(row.fieldIndex("scc1CQICW1")) else null
        val scc2CqiCw0: Integer = if (!row.isNullAt(row.fieldIndex("scc2CQICW0"))) row.getInt(row.fieldIndex("scc2CQICW0")) else null
        val scc2CqiCw1: Integer = if (!row.isNullAt(row.fieldIndex("scc2CQICW1"))) row.getInt(row.fieldIndex("scc2CQICW1")) else null
        val scc3CqiCw0: Integer = if (!row.isNullAt(row.fieldIndex("scc3CQICW0"))) row.getInt(row.fieldIndex("scc3CQICW0")) else null
        val scc3CqiCw1: Integer = if (!row.isNullAt(row.fieldIndex("scc3CQICW1"))) row.getInt(row.fieldIndex("scc3CQICW1")) else null
        val scc4CqiCw0: Integer = if (!row.isNullAt(row.fieldIndex("scc4CQICW0"))) row.getInt(row.fieldIndex("scc4CQICW0")) else null
        val scc4CqiCw1: Integer = if (!row.isNullAt(row.fieldIndex("scc4CQICW1"))) row.getInt(row.fieldIndex("scc4CQICW1")) else null
        val scc5CqiCw0: Integer = if (!row.isNullAt(row.fieldIndex("scc5CQICW0"))) row.getInt(row.fieldIndex("scc5CQICW0")) else null
        val scc5CqiCw1: Integer = if (!row.isNullAt(row.fieldIndex("scc5CQICW1"))) row.getInt(row.fieldIndex("scc5CQICW1")) else null
        //val riInt: Int = if (!row.isNullAt(row.fieldIndex("rankIndexInt"))) row.getInt(row.fieldIndex("rankIndexInt")) else 0
        //val riStr: String = if (!row.isNullAt(row.fieldIndex("rankIndexStr"))) row.getString(row.fieldIndex("rankIndexStr")) else ""
        val pccRi: Integer = if (!row.isNullAt(row.fieldIndex("pccRankIndex"))) row.getInt(row.fieldIndex("pccRankIndex")) else null
        val scc1Ri: Integer = if (!row.isNullAt(row.fieldIndex("scc1RankIndex"))) row.getInt(row.fieldIndex("scc1RankIndex")) else null
        val scc2Ri: Integer = if (!row.isNullAt(row.fieldIndex("scc2RankIndex"))) row.getInt(row.fieldIndex("scc2RankIndex")) else null
        val scc3Ri: Integer = if (!row.isNullAt(row.fieldIndex("scc3RankIndex"))) row.getInt(row.fieldIndex("scc3RankIndex")) else null
        val scc4Ri: Integer = if (!row.isNullAt(row.fieldIndex("scc4RankIndex"))) row.getInt(row.fieldIndex("scc4RankIndex")) else null
        val scc5Ri: Integer = if (!row.isNullAt(row.fieldIndex("scc5RankIndex"))) row.getInt(row.fieldIndex("scc5RankIndex")) else null
        val carrierIndex0xB14E: String = if (!row.isNullAt(row.fieldIndex("carrierIndex0xB14E"))) row.getString(row.fieldIndex("carrierIndex0xB14E")) else ""
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

        val rankIndexStr0xB14E: String = if (!row.isNullAt(row.fieldIndex("rankIndexStr0xB14E"))) row.getString(row.fieldIndex("rankIndexStr0xB14E")) else ""
        val ulFreq: Integer = if (!row.isNullAt(row.fieldIndex("ulFreq"))) row.getInt(row.fieldIndex("ulFreq")) else null
        val dlFreq: Integer = if (!row.isNullAt(row.fieldIndex("dlFreq"))) row.getInt(row.fieldIndex("dlFreq")) else null
        val ulBandwidth: String = if (!row.isNullAt(row.fieldIndex("ulBandwidth"))) row.getString(row.fieldIndex("ulBandwidth")) else ""
        val dlBandwidth: String = if (!row.isNullAt(row.fieldIndex("dlBandwidth"))) row.getString(row.fieldIndex("dlBandwidth")) else ""
        val trackingAreaCode: Integer = if (!row.isNullAt(row.fieldIndex("trackingAreaCode"))) row.getInt(row.fieldIndex("trackingAreaCode")) else null
        val lteeNBId: Integer = if (!row.isNullAt(row.fieldIndex("LteeNBId_deriv"))) row.getInt(row.fieldIndex("LteeNBId_deriv")) else null
        val cellId0xB0C0: String = if (!row.isNullAt(row.fieldIndex("CellIdentity_deriv"))) row.getString(row.fieldIndex("CellIdentity_deriv")) else null
        val modulation0: String = if (!row.isNullAt(row.fieldIndex("modStream0")) && row.getList(row.fieldIndex("modStream0")).size() > 0) row.getList(row.fieldIndex("modStream0")).get(0) else ""
        val modulation1: String = if (!row.isNullAt(row.fieldIndex("modStream1")) && row.getList(row.fieldIndex("modStream1")).size() > 0) row.getList(row.fieldIndex("modStream1")).get(0) else ""
        val pmiIndex: Integer = if (!row.isNullAt(row.fieldIndex("pmiIndex"))) row.getInt(row.fieldIndex("pmiIndex")) else null
        val spatialRank: String = if (!row.isNullAt(row.fieldIndex("spatialRank")) && row.getList(row.fieldIndex("spatialRank")).size() > 0) row.getList(row.fieldIndex("spatialRank")).get(0) else ""
        val numTxAntennas: String = if (!row.isNullAt(row.fieldIndex("numTxAntennas")) && row.getList(row.fieldIndex("numTxAntennas")).size() > 0) row.getList(row.fieldIndex("numTxAntennas")).get(0) else ""
        val numRxAntennas: String = if (!row.isNullAt(row.fieldIndex("numRxAntennas")) && row.getList(row.fieldIndex("numRxAntennas")).size() > 0) row.getList(row.fieldIndex("numRxAntennas")).get(0) else ""
        val version: Int = if (!row.isNullAt(row.fieldIndex("version"))) row.getInt(row.fieldIndex("version")) else 0
        val ModulationUl: String = if (!row.isNullAt(row.fieldIndex("modUL")) && row.getList(row.fieldIndex("modUL")).size() > 0) row.getList(row.fieldIndex("modUL")).get(0).toString else null
        val PuschTxPwr: Integer = if (!row.isNullAt(row.fieldIndex("PuschTxPower_deriv")) && row.getList(row.fieldIndex("PuschTxPower_deriv")).size() > 0) row.getList(row.fieldIndex("PuschTxPower_deriv")).get(0) else null
        val PushPrb: Integer = if (!row.isNullAt(row.fieldIndex("prb")) && row.getList(row.fieldIndex("prb")).size() > 0) row.getList(row.fieldIndex("prb")).get(0) else null
        val ulcarrierIndex: String = if (!row.isNullAt(row.fieldIndex("ulcarrierIndex")) && row.getList(row.fieldIndex("ulcarrierIndex")).size() > 0) row.getList(row.fieldIndex("ulcarrierIndex")).get(0).toString else ""
        val timingAdvance: Integer = if (!row.isNullAt(row.fieldIndex("timingAdvance"))) row.getInt(row.fieldIndex("timingAdvance")) else null

        val CDMA_PilotPN: Integer = if (!row.isNullAt(row.fieldIndex("pilotPNasList")) && row.getList(row.fieldIndex("pilotPNasList")).size() > 0) row.getList(row.fieldIndex("pilotPNasList")).get(0) else null
        val cdmaRfTxPowerVal: lang.Float = if (!row.isNullAt(row.fieldIndex("cdmaRfTxPower"))) row.getFloat(row.fieldIndex("cdmaRfTxPower")) else null
        val TxGainAdjVal: Integer = if (!row.isNullAt(row.fieldIndex("txGainAdj"))) row.getInt(row.fieldIndex("txGainAdj")) else null
        val CdmaTxStatusVal: String = if (!row.isNullAt(row.fieldIndex("cdmaTxStatus"))) row.getString(row.fieldIndex("cdmaTxStatus")) else null
        val NewCellCause: String = if (!row.isNullAt(row.fieldIndex("newCellCause"))) row.getString(row.fieldIndex("newCellCause")) else ""
        val lteAttRej: Integer = if (!row.isNullAt(row.fieldIndex("lteAttRej"))) row.getInt(row.fieldIndex("lteAttRej")) else null
        val lteServiceRej: Integer = if (!row.isNullAt(row.fieldIndex("lteServiceRej"))) row.getInt(row.fieldIndex("lteServiceRej")) else null
        val ltePdnRej: Integer = if (!row.isNullAt(row.fieldIndex("ltePdnRej"))) row.getInt(row.fieldIndex("ltePdnRej")) else null
        val lteAuthRej: Integer = if (!row.isNullAt(row.fieldIndex("lteAuthRej"))) row.getInt(row.fieldIndex("lteAuthRej")) else null
        val lteTaRej: Integer = if (!row.isNullAt(row.fieldIndex("lteTaRej"))) row.getInt(row.fieldIndex("lteTaRej")) else null

        val lteActDefContAcptCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteActDefContAcptCnt"))) row.getLong(row.fieldIndex("lteActDefContAcptCnt")) else null
        val lteDeActContAcptCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteDeActContAcptCnt"))) row.getLong(row.fieldIndex("lteDeActContAcptCnt")) else null
        val lteEsmInfoResCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteEsmInfoResCnt"))) row.getLong(row.fieldIndex("lteEsmInfoResCnt")) else null
        val ltePdnConntReqount: lang.Long = if (!row.isNullAt(row.fieldIndex("ltePdnConntReqCnt"))) row.getLong(row.fieldIndex("ltePdnConntReqCnt")) else null
        val ltePdnDisconReqCount: lang.Long = if (!row.isNullAt(row.fieldIndex("ltePdnDisconReqCnt"))) row.getLong(row.fieldIndex("ltePdnDisconReqCnt")) else null

        val lteActDefBerContReqCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteActDefBerContReqCnt"))) row.getLong(row.fieldIndex("lteActDefBerContReqCnt")) else null
        val lteDeActBerContReqCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteDeActBerContReqCnt"))) row.getLong(row.fieldIndex("lteDeActBerContReqCnt")) else null
        val lteEsmInfoReqCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteEsmInfoReqCnt"))) row.getLong(row.fieldIndex("lteEsmInfoReqCnt")) else null
        val lteAttCmpCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteAttCmpCnt"))) row.getLong(row.fieldIndex("lteAttCmpCnt")) else null
        val lteAttReqCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteAttReqCnt"))) row.getLong(row.fieldIndex("lteAttReqCnt")) else null

        val lteDetachReqCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteDetachReqCnt"))) row.getLong(row.fieldIndex("lteDetachReqCnt")) else null
        val lteSrvReqCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteSrvReqCnt"))) row.getLong(row.fieldIndex("lteSrvReqCnt")) else null
        val lteAttAcptCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteAttAcptCnt"))) row.getLong(row.fieldIndex("lteAttAcptCnt")) else null
        val lteEmmInfoCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteEmmInfoCnt"))) row.getLong(row.fieldIndex("lteEmmInfoCnt")) else null
        val lteTaReqCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteTaReqCnt"))) row.getLong(row.fieldIndex("lteTaReqCnt")) else null
        val lteTaAcptCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteTaAcptCnt"))) row.getLong(row.fieldIndex("lteTaAcptCnt")) else null
        val lteEsmModifyEpsReqCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteEsmModifyEpsReqCnt"))) row.getLong(row.fieldIndex("lteEsmModifyEpsReqCnt")) else null
        val lteEsmModifyEpsRejCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteEsmModifyEpsRejCnt"))) row.getLong(row.fieldIndex("lteEsmModifyEpsRejCnt")) else null
        val lteEsmModifyEpsAccptCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteEsmModifyEpsAccptCnt"))) row.getLong(row.fieldIndex("lteEsmModifyEpsAccptCnt")) else null
        val lteActDefContRejCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteActDefContRejCnt"))) row.getLong(row.fieldIndex("lteActDefContRejCnt")) else null
        val lteActDedContRejCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteActDedContRejCnt"))) row.getLong(row.fieldIndex("lteActDedContRejCnt")) else null
        val lteActDedBerContReqCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteActDedBerContReqCnt"))) row.getLong(row.fieldIndex("lteActDedBerContReqCnt")) else null
        val lteActDedContAcptCount: lang.Long = if (!row.isNullAt(row.fieldIndex("lteActDedContAcptCnt"))) row.getLong(row.fieldIndex("lteActDedContAcptCnt")) else null
        val LteCellID: Int = if (!row.isNullAt(row.fieldIndex("LteCellID_deriv"))) row.getInt(row.fieldIndex("LteCellID_deriv")) else 0
        val femtoStatus: Integer = getFemtoStatus(LteCellID)
        val nasMsgType: String = if (!row.isNullAt(row.fieldIndex("NAS_MsgType"))) row.getString(row.fieldIndex("NAS_MsgType")) else ""
        val Qci: String = if (!row.isNullAt(row.fieldIndex("qci"))) row.getString(row.fieldIndex("qci")) else ""
        val PdnApnName: String = if (!row.isNullAt(row.fieldIndex("pdnApnName"))) row.getString(row.fieldIndex("pdnApnName")) else ""
        val PdnReqType: String = if (!row.isNullAt(row.fieldIndex("pdnReqType"))) row.getString(row.fieldIndex("pdnReqType")) else ""
        val PdnType: String = if (!row.isNullAt(row.fieldIndex("pdnType"))) row.getString(row.fieldIndex("pdnType")) else ""
        val pdnEsmCause: String = if (!row.isNullAt(row.fieldIndex("esmCause"))) row.getString(row.fieldIndex("esmCause")) else ""

        val VoPSStatus: String = if (!row.isNullAt(row.fieldIndex("voPSStatus"))) row.getString(row.fieldIndex("voPSStatus")) else ""
        val AttRejCause: String = if (!row.isNullAt(row.fieldIndex("attRejCause"))) row.getString(row.fieldIndex("attRejCause")) else ""
        val ServRejCause: String = if (!row.isNullAt(row.fieldIndex("servRejCause"))) row.getString(row.fieldIndex("servRejCause")) else ""
        val EmmTaRejCause: String = if (!row.isNullAt(row.fieldIndex("emmTaRejCause"))) row.getString(row.fieldIndex("emmTaRejCause")) else ""
        val EmmAuthRejCause: String = if (!row.isNullAt(row.fieldIndex("emmAuthRejCause"))) row.getString(row.fieldIndex("emmAuthRejCause")) else ""
        val Imsi: String = if (!row.isNullAt(row.fieldIndex("imsi_QComm2"))) row.getString(row.fieldIndex("imsi_QComm2")) else ""
        val Imei: String = if (!row.isNullAt(row.fieldIndex("imei_QComm2"))) row.getString(row.fieldIndex("imei_QComm2")) else ""
        val EpsMobileIdType: String = if (!row.isNullAt(row.fieldIndex("epsMobileIdType"))) row.getString(row.fieldIndex("epsMobileIdType")) else ""


        val TaType: String = if (!row.isNullAt(row.fieldIndex("tatype"))) row.getString(row.fieldIndex("tatype")) else ""
        val AttachType: String = if (!row.isNullAt(row.fieldIndex("attachType"))) row.getString(row.fieldIndex("attachType")) else ""
        val dcnrVal: String = if (!row.isNullAt(row.fieldIndex("dcnr"))) row.getString(row.fieldIndex("dcnr")) else ""
        val n1ModeVal: String = if (!row.isNullAt(row.fieldIndex("n1Mode"))) row.getString(row.fieldIndex("n1Mode")) else ""
        val numOfLayers: Integer = if (!row.isNullAt(row.fieldIndex("numLayers")) && row.getList(row.fieldIndex("numLayers")).size() > 0) row.getList(row.fieldIndex("numLayers")).get(0) else null
        val PucchGi: Integer = if (!row.isNullAt(row.fieldIndex("Gi")) && row.getList(row.fieldIndex("Gi")).size() > 0) row.getList(row.fieldIndex("Gi")).get(0) else null
        val gsmArfcn: Integer = if (!row.isNullAt(row.fieldIndex("bcchArfcn"))) row.getInt(row.fieldIndex("bcchArfcn")) else null
        val PowerHeadRoomList: List[Long] = if (!row.isNullAt(row.fieldIndex("powerHeadRoom"))) row.getSeq(row.fieldIndex("powerHeadRoom")).toList else null
        val PowerHeadRoomNewList = if (PowerHeadRoomList != null) PowerHeadRoomList.filter(_ != 232) else null
        val PowerHeadRoom = if (PowerHeadRoomNewList != null && PowerHeadRoomNewList.nonEmpty) PowerHeadRoomNewList.last.toInt else null
        val evdoRxPwr: lang.Double = if (!row.isNullAt(row.fieldIndex("evdoRxPwr")) && row.getList(row.fieldIndex("evdoRxPwr")).size() > 0) row.getList(row.fieldIndex("evdoRxPwr")).get(0) else null
        val evdoTxPwr: lang.Double = if (!row.isNullAt(row.fieldIndex("evdoTxPwr")) && row.getList(row.fieldIndex("evdoTxPwr")).size() > 0) row.getList(row.fieldIndex("evdoTxPwr")).get(0) else null
        val timingAdvanceEvt: Integer = if (!row.isNullAt(row.fieldIndex("timingAdvanceEvt"))) row.getInt(row.fieldIndex("timingAdvanceEvt")) else null
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

        var servingPCI: Predef.Map[Integer,GeometryDto] = null
        var neighboringPCI: Predef.Map[Integer,GeometryDto] = null

        if (logRecordJoinedDF.kpiGeoDistToCellsQuery) {
          servingPCI = if (!row.isNullAt(row.fieldIndex("servingPciInfo"))) row.getMap(row.fieldIndex("servingPciInfo")).asInstanceOf[Predef.Map[Integer,GeometryDto]] else null
          neighboringPCI = if (!row.isNullAt(row.fieldIndex("NeighboringPciInfo"))) row.getMap(row.fieldIndex("NeighboringPciInfo")).asInstanceOf[Predef.Map[Integer,GeometryDto]] else null
        }

        var gnbidInfo: Predef.Map[Integer,gNodeIdInfo] = null
        if(logRecordJoinedDF.gNbidGeoDistToCellsQuery) {
          gnbidInfo = if (!row.isNullAt(row.fieldIndex("gNBidInfo"))) row.getMap(row.fieldIndex("gNBidInfo")).asInstanceOf[Predef.Map[Integer,gNodeIdInfo]] else null
        }
        var pdcpDlTotalBytes: lang.Long = null
        var prePdcpDlTotalBytes: lang.Long = null
        var pdcpDlTimeStamp: lang.Long = null
        var prePdcpDlTimeStamp: lang.Long = null
        var pdcpUlTotalBytes: lang.Long = null
        var prePdcpUlTotalBytes: lang.Long = null
        var pdcpUlTimeStamp: lang.Long = null
        var prePdcpUlTimeStamp: lang.Long = null
        var rlcDlTotalBytes: lang.Long = null
        var prerlcDlTotalBytes: lang.Long = null
        var rlcDlTimeStamp: lang.Long = null
        var prerlcDlTimeStamp: lang.Long = null
        var rlcUlTotalBytes: lang.Long = null
        var prerlcUlTotalBytes: lang.Long = null
        var rlcUlTimeStamp: lang.Long = null
        var prerlcUlTimeStamp: lang.Long = null

        var rlcDlBytes0xb087v49: List[lang.Long] = null
        var prerlcDlBytes0xb087v49: List[lang.Long] = null
        var rlcDlRbConfig0xb087v49: List[Integer] = null
        var prerlcDlRbConfig0xb087v49: List[Integer] = null
        var rlcDlNumRbs0xb087v49: Integer = null
        var prerlcDlNumRbs0xb087v49: Integer = null
        var rlcDlTimeStamp0xb087v49: lang.Long = null
        var preRlcDlTimeStamp0xb087v49: lang.Long = null
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

        var PUCCH_Tx_Power0xB16E: Int = if (!row.isNullAt(row.fieldIndex("PUCCH_Tx_Power0xB16E")) && row.getList(row.fieldIndex("PUCCH_Tx_Power0xB16E")).size() >= numRecords0xB16E) row.getList(row.fieldIndex("PUCCH_Tx_Power0xB16E")).get(numRecords0xB16E - 1).asInstanceOf[Int] else 0
        var PUCCH_Tx_Power0xB16F: Int = if (!row.isNullAt(row.fieldIndex("PUCCH_Tx_Power0xB16F")) && row.getList(row.fieldIndex("PUCCH_Tx_Power0xB16F")).size() >= numRecords0xB16F) row.getList(row.fieldIndex("PUCCH_Tx_Power0xB16F")).get(numRecords0xB16F - 1).asInstanceOf[Int] else 0
        var DLPathLoss0xB16E: Int = if (!row.isNullAt(row.fieldIndex("DLPathLoss0xB16E")) && row.getList(row.fieldIndex("DLPathLoss0xB16E")).size() >= numRecords0xB16E && numRecords0xB16E > 0) row.getList(row.fieldIndex("DLPathLoss0xB16E")).get(numRecords0xB16E - 1).asInstanceOf[Int] else 0
        var DLPathLoss0xB16F: Int = if (!row.isNullAt(row.fieldIndex("DLPathLoss0xB16F")) && row.getList(row.fieldIndex("DLPathLoss0xB16F")).size() >= numRecords0xB16F && numRecords0xB16F > 0) row.getList(row.fieldIndex("DLPathLoss0xB16F")).get(numRecords0xB16F - 1).asInstanceOf[Int] else 0

        var p0_NominalPUCCH: String = ""
        var upperLayerIndication_r15: String = null

        var p0_NominalPUCCHVal: Integer = null
        var pucchAdjTxPwr: lang.Float = null
        var pucchTargetMobPwr: lang.Float = null
        var LTE_SSAC_Present: String = null
        var lteRrcConRestRej: Integer = null
        var lteRrcConRej: Integer = null
        var lteRrcConRelCount: lang.Long = null
        var lteRrcConReqCount: lang.Long = null
        var lteRrcConSetupCount: lang.Long = null
        var lteRrcConSetupCmpCount: lang.Long = null
        var lteSecModeCmdCount: lang.Long = null
        var lteSecModeCmpCount: lang.Long = null
        var lteRrcConReconfigCount: lang.Long = null
        var lteRrcConReconfCmpCount: lang.Long = null
        var lteUeCapEnqCount: lang.Long = null
        var lteUeCapInfoCount: lang.Long = null
        var nrRrcReConfigCount: lang.Long = null
        var nrRbConfigCount: lang.Long = null
        var nrRrcReConfigCmpCount: lang.Long = null
        var lteHoCmdCount: lang.Long = null
        var lteRrcConRestReqCount: lang.Long = null
        var lteRrcConRestCmpCount: lang.Long = null
        var lteRrcConRestCount: lang.Long = null
        var nrState: String = null
        var nrFailureType: String = null
        var lteRrcConSetup: Integer = null
        var postRrcConSetupCmp: Integer = null
        var nrPrimaryPath: String = null
        var nrUlDataSplitThreshold: String = null
        var nrSRSTxSwitchConfig: String = null

        //5G SCG SP cell info
        var nrSpCellPci: Integer = null
        var nrSpCellDlFreq: Integer = null
        var nrSpCellBand: Integer = null
        var nrSpCellDuplexMode: String = null
        var nrSpCellDlBw: lang.Double = null
        var nrSpCellUlBw: lang.Double = null
        var nrSpCellUlFreq: Integer = null
        var MCC: String = ""
        var MNC: String = ""
        var nrSpcellPci_gnbid:Integer = null

        MCC = if (!row.isNullAt(row.fieldIndex("MCC"))) row.getInt(row.fieldIndex("MCC")).toString else ""
        MNC = if (!row.isNullAt(row.fieldIndex("MNC"))) row.getInt(row.fieldIndex("MNC")).toString else ""

        //                rRccOtaSubtitle = if (!row.isNullAt(row.fieldIndex("rRcOtaSubtitle"))) row.getString(row.fieldIndex("rRcOtaSubtitle")) else ""
        p0_NominalPUCCH = if (!row.isNullAt(row.fieldIndex("p0_NominalPUCCH"))) row.getString(row.fieldIndex("p0_NominalPUCCH")) else ""
        p0_NominalPUCCHVal = if (isValidNumeric(p0_NominalPUCCH, Constants.CONST_NUMBER)) p0_NominalPUCCH.toInt else null
        pucchAdjTxPwr = getPucchAdjTargetPwr(PUCCH_Tx_Power0xB16F, DLPathLoss0xB16F, p0_NominalPUCCHVal, "AdjTxPwr")
        pucchTargetMobPwr = getPucchAdjTargetPwr(PUCCH_Tx_Power0xB16F, DLPathLoss0xB16F, p0_NominalPUCCHVal, "TargetMobPwr")
        LTE_SSAC_Present = if (!row.isNullAt(row.fieldIndex("ssac_BarringForMMTEL")) && row.getBoolean(row.fieldIndex("ssac_BarringForMMTEL"))) "SSAC ON" else "SSAC OFF"
        upperLayerIndication_r15 = if (!row.isNullAt(row.fieldIndex("upperLayerIndication_r15"))) row.getString(row.fieldIndex("upperLayerIndication_r15")) else null
        lteRrcConRestRej = if (!row.isNullAt(row.fieldIndex("lteRrcConRestRej"))) row.getInt(row.fieldIndex("lteRrcConRestRej")) else null
        lteRrcConRej = if (!row.isNullAt(row.fieldIndex("lteRrcConRej"))) row.getInt(row.fieldIndex("lteRrcConRej")) else null
        nrSpCellPci = if (!row.isNullAt(row.fieldIndex("nrSpCellPci"))) row.getInt(row.fieldIndex("nrSpCellPci")) else null
        nrSpcellPci_gnbid = if (!row.isNullAt(row.fieldIndex("nrSpcellPci"))) row.getInt(row.fieldIndex("nrSpcellPci")) else null
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
        lteRrcConSetup = if (!row.isNullAt(row.fieldIndex("RrcConSetup"))) row.getInt(row.fieldIndex("RrcConSetup")) else null
        postRrcConSetupCmp = if (!row.isNullAt(row.fieldIndex("postRrcConSetupCmp"))) row.getInt(row.fieldIndex("postRrcConSetupCmp")) else null
        nrPrimaryPath = if (!row.isNullAt(row.fieldIndex("NRPrimaryPath"))) row.getString(row.fieldIndex("NRPrimaryPath")) else null
        nrUlDataSplitThreshold = if (!row.isNullAt(row.fieldIndex("NRUlDataSplitThreshold"))) row.getString(row.fieldIndex("NRUlDataSplitThreshold")) else null
        nrSRSTxSwitchConfig = if (!row.isNullAt(row.fieldIndex("nrSRSTxSwitchConfig"))) row.getString(row.fieldIndex("nrSRSTxSwitchConfig")) else null


        val EmmState: String = if (!row.isNullAt(row.fieldIndex("emmState"))) row.getString(row.fieldIndex("emmState")) else ""
        val EmmSubState: String = if (!row.isNullAt(row.fieldIndex("emmSubState"))) row.getString(row.fieldIndex("emmSubState")) else ""
        val frameNum: List[Int] = if (!row.isNullAt(row.fieldIndex("frameNum"))) row.getSeq(row.fieldIndex("frameNum")).toList else null
        val SubFrameNum: List[Int] = if (!row.isNullAt(row.fieldIndex("subFrameNum"))) row.getSeq(row.fieldIndex("subFrameNum")).toList else null
        val currentSFNSF: List[Int] = if (!row.isNullAt(row.fieldIndex("currentSFNSF"))) row.getSeq(row.fieldIndex("currentSFNSF")).toList else null
        val CDMAChannel: Integer = if (!row.isNullAt(row.fieldIndex("channel0x1017"))) row.getInt(row.fieldIndex("channel0x1017")) else if (!row.isNullAt(row.fieldIndex("channel0x119A"))) row.getInt(row.fieldIndex("channel0x119A")) else null
        val pilotPN: Integer = if (!row.isNullAt(row.fieldIndex("pilotPN"))) row.getInt(row.fieldIndex("pilotPN")) else null
        val hrpdState: String = if (!row.isNullAt(row.fieldIndex("hrpdState"))) row.getString(row.fieldIndex("hrpdState")) else ""
        val pdnRat: String = if (!row.isNullAt(row.fieldIndex("pdnRat"))) row.getString(row.fieldIndex("pdnRat")) else ""
        val WCDMA_cellID: Integer = if (!row.isNullAt(row.fieldIndex("wcdmaCellID"))) row.getInt(row.fieldIndex("wcdmaCellID")) else null
        val WCDMA_RAC_Id: Integer = if (!row.isNullAt(row.fieldIndex("racId"))) row.getInt(row.fieldIndex("racId")) else null
        val WCDMA_LAC_Id: Integer = if (!row.isNullAt(row.fieldIndex("lacId"))) row.getInt(row.fieldIndex("lacId")) else null
        val WCDMA_PSC: Integer = if (!row.isNullAt(row.fieldIndex("psc"))) row.getInt(row.fieldIndex("psc")) else null
        val GSM_cellID: Integer = if (!row.isNullAt(row.fieldIndex("gsmCellID"))) row.getInt(row.fieldIndex("gsmCellID")) else null
        val GSM_l2State: String = if (!row.isNullAt(row.fieldIndex("l2State"))) row.getString(row.fieldIndex("l2State")) else null
        val GSM_rrState: String = if (!row.isNullAt(row.fieldIndex("rrState"))) row.getString(row.fieldIndex("rrState")) else null
        val GSM_MsMaxTxPwr: Integer = if (!row.isNullAt(row.fieldIndex("txPower"))) row.getInt(row.fieldIndex("txPower")) else null
        val GSM_MsMinRxLev: Integer = if (!row.isNullAt(row.fieldIndex("rxLev"))) row.getInt(row.fieldIndex("rxLev")) else null
        val GSM_BSIC_BCC: Integer = if (!row.isNullAt(row.fieldIndex("bsicBcc"))) row.getInt(row.fieldIndex("bsicBcc")) else null
        val GSM_BSIC_NCC: Integer = if (!row.isNullAt(row.fieldIndex("bsicNcc"))) row.getInt(row.fieldIndex("bsicNcc")) else null

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

        val lteTransModeUl: Integer = if (!row.isNullAt(row.fieldIndex("txModeUl"))) row.getInt(row.fieldIndex("txModeUl")) else null
        val lteTransModeDl: Integer = if (!row.isNullAt(row.fieldIndex("txModeDl"))) row.getInt(row.fieldIndex("txModeDl")) else null
        val Gsm_Lai: List[Byte] = if (!row.isNullAt(row.fieldIndex("Lai"))) row.getSeq(row.fieldIndex("Lai")).toList else null
        val servingCellIndex: List[String] = if (!row.isNullAt(row.fieldIndex("ServingCellIndex"))) row.getSeq(row.fieldIndex("ServingCellIndex")).toList else null
        val logpacket2servingCellIndex: List[String] = if (!row.isNullAt(row.fieldIndex("LogPacket2_b173ServingCell"))) row.getSeq(row.fieldIndex("LogPacket2_b173ServingCell")).toList else null
        val logpacket3servingCellIndex: List[String] = if (!row.isNullAt(row.fieldIndex("LogPacket3_b173ServingCell"))) row.getSeq(row.fieldIndex("LogPacket3_b173ServingCell")).toList else null
        val tbsize0xB139: List[Int] = if (!row.isNullAt(row.fieldIndex("TB_Size0xB139"))) row.getSeq(row.fieldIndex("TB_Size0xB139")).toList else null
        val tbsize0xB173: List[Int] = if (!row.isNullAt(row.fieldIndex("TB_Size0xB173"))) row.getSeq(row.fieldIndex("TB_Size0xB173")).toList else null
        val CRCResult: List[String] = if (!row.isNullAt(row.fieldIndex("CrcResult"))) row.getSeq(row.fieldIndex("CrcResult")).toList else null
        val numPRB0xB173: List[String] = if (!row.isNullAt(row.fieldIndex("numPRB0xB173"))) row.getSeq(row.fieldIndex("numPRB0xB173")).toList else null
        val sysMode: Integer = if (!row.isNullAt(row.fieldIndex("mSYS_MODE"))) row.getInt(row.fieldIndex("mSYS_MODE")) else null
        val SrvStatus: Integer = if (!row.isNullAt(row.fieldIndex("mSRV_STATUS"))) row.getInt(row.fieldIndex("mSRV_STATUS")) else null
        val SrvStatus1: Integer = if (!row.isNullAt(row.fieldIndex("mSRV1_STATUS"))) row.getInt(row.fieldIndex("mSRV1_STATUS")) else null
        //        val CDMASid: Int = if (!row.isNullAt(row.fieldIndex("CDMA_SID"))) row.getInt(row.fieldIndex("CDMA_SID")) else 0
        //        val CDMANid: Int = if (!row.isNullAt(row.fieldIndex("CDMA_NID"))) row.getInt(row.fieldIndex("CDMA_NID")) else 0
        val mECIO: Int = if (!row.isNullAt(row.fieldIndex("mECIO"))) row.getInt(row.fieldIndex("mECIO")) else 0
        val mECIO_EvDO: Int = if (!row.isNullAt(row.fieldIndex("ECIO_EvDo"))) row.getInt(row.fieldIndex("ECIO_EvDo")) else 0
        val SINR_EvDo: Int = if (!row.isNullAt(row.fieldIndex("SINR_EvDo"))) row.getInt(row.fieldIndex("SINR_EvDo")) else 0
        //val RSSI_wcdma: Int = if (!row.isNullAt(row.fieldIndex("RSSI_wcdma"))) row.getInt(row.fieldIndex("RSSI_wcdma")) else 0
        //val GW_RSCP: Int = if (!row.isNullAt(row.fieldIndex("GW_RSCP"))) row.getInt(row.fieldIndex("GW_RSCP")) else 0
        val bandClass: Int = if (!row.isNullAt(row.fieldIndex("bandClass_QComm2"))) row.getInt(row.fieldIndex("bandClass_QComm2")) else 0
        val HdrHybrid: Int = if (!row.isNullAt(row.fieldIndex("mHdrHybrid"))) row.getInt(row.fieldIndex("mHdrHybrid")) else 0
        val RoamStatus: Integer = if (!row.isNullAt(row.fieldIndex("ROAM_STATUS"))) row.getInt(row.fieldIndex("ROAM_STATUS")) else null
        val imsStatus: String = if (!row.isNullAt(row.fieldIndex("imsStatus"))) row.getString(row.fieldIndex("imsStatus")) else null
        val imsFeatureTag: String = if (!row.isNullAt(row.fieldIndex("imsFeatureTag"))) row.getString(row.fieldIndex("imsFeatureTag")) else null

        val sysMode0_0x184E: Integer = if (!row.isNullAt(row.fieldIndex("stk_SYS_MODE0_0x184E"))) row.getInt(row.fieldIndex("stk_SYS_MODE0_0x184E")) else null
        val sysMode1_0x184E: Integer = if (!row.isNullAt(row.fieldIndex("stk_SYS_MODE1_0x184E"))) row.getInt(row.fieldIndex("stk_SYS_MODE1_0x184E")) else null
        val SrvStatus0_0x184E: Integer = if (!row.isNullAt(row.fieldIndex("SRV_STATUS0x184E"))) row.getInt(row.fieldIndex("SRV_STATUS0x184E")) else null
        val ECIO0x184E: Float = if (!row.isNullAt(row.fieldIndex("mECIO_0x184E"))) row.getFloat(row.fieldIndex("mECIO_0x184E")) else 0
        val ECIO_EvDo0x184E: Float = if (!row.isNullAt(row.fieldIndex("ECIO_EvDo_0x184E"))) row.getFloat(row.fieldIndex("ECIO_EvDo_0x184E")) else 0
        val SINR_EvDo0x184E: Int = if (!row.isNullAt(row.fieldIndex("SINR_EvDo_0x184E"))) row.getInt(row.fieldIndex("SINR_EvDo_0x184E")) else 0
        //val RSSI_wcdma0x184E: Float = if (!row.isNullAt(row.fieldIndex("RSSI_wcdma_0x184E"))) row.getFloat(row.fieldIndex("RSSI_wcdma_0x184E")) else 0
        //val GW_RSCP0x184E: Int = if (!row.isNullAt(row.fieldIndex("GW_RSCP_0x184E"))) row.getInt(row.fieldIndex("GW_RSCP_0x184E")) else 0
        val sysIDx184E: Int = if (!row.isNullAt(row.fieldIndex("sys_ID0x184E"))) row.getInt(row.fieldIndex("sys_ID0x184E")) else 0
        val sysModeOperational_0x184E: Int = if (!row.isNullAt(row.fieldIndex("sys_Mode_Operational_0x184E"))) row.getInt(row.fieldIndex("sys_Mode_Operational_0x184E")) else 0

        val cdma_Sid_0x184E: Integer = if (!row.isNullAt(row.fieldIndex("CDMA_SID_0x184E"))) row.getInt(row.fieldIndex("CDMA_SID_0x184E")) else null
        val cdma_Nid_x184E: Integer = if (!row.isNullAt(row.fieldIndex("CDMA_NID_0x184E"))) row.getInt(row.fieldIndex("CDMA_NID_0x184E")) else null
        val Cdma_EcIo_0x184E: lang.Float = if (!row.isNullAt(row.fieldIndex("CdmaEcIo_0x184E"))) row.getFloat(row.fieldIndex("CdmaEcIo_0x184E")) else null
        val band_class_0x184E: String = if (!row.isNullAt(row.fieldIndex("BandClass_0x184E"))) row.getString(row.fieldIndex("BandClass_0x184E")) else null

        val evDo_ECIO_0x184E: lang.Float = if (!row.isNullAt(row.fieldIndex("EvDoECIO_0x184E"))) row.getFloat(row.fieldIndex("EvDoECIO_0x184E")) else null
        val evDo_SINR_0x184E: Integer = if (!row.isNullAt(row.fieldIndex("EvDoSINR_0x184E"))) row.getInt(row.fieldIndex("EvDoSINR_0x184E")) else null
        val wcdma_Rssi: lang.Float = if (!row.isNullAt(row.fieldIndex("WcdmaRssi_0x184E"))) row.getFloat(row.fieldIndex("WcdmaRssi_0x184E")) else null
        val wcdma_Rscp: Integer = if (!row.isNullAt(row.fieldIndex("WcdmaRscp_0x184E"))) row.getInt(row.fieldIndex("WcdmaRscp_0x184E")) else null
        val wcdma_Ecio: lang.Float = if (!row.isNullAt(row.fieldIndex("WcdmaEcio_0x184E"))) row.getFloat(row.fieldIndex("WcdmaEcio_0x184E")) else null
        val lteHandoverSuccessCount = if (!row.isNullAt(row.fieldIndex("lteHandoverSuccessCnt"))) row.getLong(row.fieldIndex("lteHandoverSuccessCnt")) else null
        //logger.info("WcdmaRssi_0x184E = " + wcdma_Rssi)

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
        var ServingIndexSize_0xB173: Int = 0
        var servingIndexSize_0xb193:Int = 0
        var numofsamples_B193:Int = 0
        var isLteRrcState:Boolean = false

        //        var refPower_sig: Array[(String, String)] = null
        //        var refQuality_sig: Array[(String, String)] = null
        //        var cinr_sig: Array[(String, String)] = null
        //        var cellIdSig: String = null
        //        var refPowerSig: String = null
        //        var refQualitySig: String = null
        //        var cinrSig: String = null
        //        var frequencySig: String =  null
        //        var channelNumberSig: String = null
        //        var bandwidthSig: String = null
        //        var hexlogcodeSig: String = null


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

        //        if(fileType=="sig") {
        //          cellIdSig = if (!row.isNullAt(row.fieldIndex("cellId_collectedSig"))) row.getString(row.fieldIndex("cellId_collectedSig")) else null
        //          refPowerSig = if (!row.isNullAt(row.fieldIndex("RefPower_collectedSig"))) row.getString(row.fieldIndex("RefPower_collectedSig")) else null
        //          refQualitySig = if (!row.isNullAt(row.fieldIndex("RefQuality_collectedSig"))) row.getString(row.fieldIndex("RefQuality_collectedSig")) else null
        //          cinrSig = if (!row.isNullAt(row.fieldIndex("CINR_collectedSig"))) row.getString(row.fieldIndex("CINR_collectedSig")) else null
        //          frequencySig = if (!row.isNullAt(row.fieldIndex("frequency_sig"))) row.getString(row.fieldIndex("frequency_sig")) else null
        //          channelNumberSig = if (!row.isNullAt(row.fieldIndex("channelNumber_sig"))) row.getString(row.fieldIndex("channelNumber_sig")) else null
        //          bandwidthSig = if (!row.isNullAt(row.fieldIndex("bandwidth_sig"))) row.getString(row.fieldIndex("bandwidth_sig")) else null
        //          hexlogcodeSig = if (!row.isNullAt(row.fieldIndex("hexLogCode_sig"))) row.getString(row.fieldIndex("hexLogCode_sig")) else null
        //
        //
        //          if (cellIdSig != null && refPowerSig != null) refPower_sig = getNrServingCellKPIs(cellIdSig, refPowerSig)
        //          if (cellIdSig != null && refQualitySig != null) refQuality_sig = getNrServingCellKPIs(cellIdSig, refQualitySig)
        //          if (cellIdSig != null && cinrSig != null) cinr_sig = getNrServingCellKPIs(cellIdSig, cinrSig)
        //        }

        if (servingCellIndex_B193 != null) {
          var servingCells = servingCellIndex_B193.split(",").map(_.trim).toList
          servingIndexSize_0xb193 = servingCells.distinct.size
          numofsamples_B193 = servingCells.size
          if(numofsamples_B193>5) isLteRrcState = true
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
        var NrPdschTputVal: lang.Float = null
        var NRServingBeamIndexVal: Integer = null
        var NrBeamRsrpLastLogPacket: String = null
        var Nrs_ServingPciB975: String = null
//        var PhychanBitMask_0xB884: String = null
//        var PUSCHDLPathLoss_0xB884: String = null
//        var PUCCHDLPathLoss_0xB884: String = null
//        var SRSDLPathLoss_0xB884: String = null
//        var PRACPathLoss_0xB884: String = null
//        var PUSCHTxPwr_0xB884: String = null
        var phychanBitMaskVal: String = null
        var servingBeamKpis0xB97f:mutable.Map[String,Any] = null
        var bandType0xB97F:String = null
        var scc1BandType0xB97F:String = null
        var nrConnectivityType:List[String] = null
        var nrConnectivityMode:String = null
        var nrband0xB97F:Integer = null
        var nrbandScc10xB97F:Integer = null
        var nrPccArfcn0xB97F:Integer = null
        var nrScc1Arfcn0xB97F:Integer = null
        var nrSetupAttempt:String = null
        var nrSetupSuccess:String = null
        var nrSetupFailure:String = null
     //   var nrsetupForNrScgState:String = null
        var NrReleaseEvt:Integer = null
        var NrSetupSuccessCnt:lang.Long = null
        var NrSetupFailureCnt:lang.Long  = null
        var NrReleaseCnt:lang.Long  = null
        var NrSetupAttemptCnt:lang.Long = null
        var Scc1BandType:String = null
        var nrCCIndex0xB97F:List[Integer] = null
        var validServingIndex:Boolean = false

        Nrs_ServingPciB975 = if (!row.isNullAt(row.fieldIndex("Serving_nrpci"))) row.getString(row.fieldIndex("Serving_nrpci")) else ""
        Nrs_ServingPciB97F = if (!row.isNullAt(row.fieldIndex("nrservingcellpci_0xb97f"))) row.getSeq(row.fieldIndex("nrservingcellpci_0xb97f")).toList else null
        version_B975 = if (!row.isNullAt(row.fieldIndex("version_B975"))) row.getInt(row.fieldIndex("version_B975")) else 0
        nrdlbler = if (!row.isNullAt(row.fieldIndex("nrdlbler")) && row.getList(row.fieldIndex("nrdlbler")).size() > 0) row.getList(row.fieldIndex("nrdlbler")).get(0).toString else null
        nrresidualbler = if (!row.isNullAt(row.fieldIndex("nrresidualbler")) && row.getList(row.fieldIndex("nrresidualbler")).size() > 0) row.getList(row.fieldIndex("nrresidualbler")).get(0).toString else null
        nrdlmodtype = if (!row.isNullAt(row.fieldIndex("nrdlmodtype")) && row.getList(row.fieldIndex("nrdlmodtype")).size() > 0) row.getSeq(row.fieldIndex("nrdlmodtype")).toList else null
        nrsdlfrequency_B975 = if (!row.isNullAt(row.fieldIndex("nrsdlfrequency_B975"))) row.getString(row.fieldIndex("nrsdlfrequency_B975")) else null
        NrPdschTputVal = if (!row.isNullAt(row.fieldIndex("nrpdschtput"))) row.getDouble(row.fieldIndex("nrpdschtput")).toFloat else null
//        PhychanBitMask_0xB884 = if (!row.isNullAt(row.fieldIndex("PhychanBitMask_0xB884")) && row.getList(row.fieldIndex("PhychanBitMask_0xB884")).size() > 0) row.getList(row.fieldIndex("PhychanBitMask_0xB884")).get(0) else null
//        PUSCHDLPathLoss_0xB884 = if (!row.isNullAt(row.fieldIndex("PUSCHDLPathLoss_0xB884")) && row.getList(row.fieldIndex("PUSCHDLPathLoss_0xB884")).size() > 0) row.getList(row.fieldIndex("PUSCHDLPathLoss_0xB884")).get(0) else null
//        PUCCHDLPathLoss_0xB884 = if (!row.isNullAt(row.fieldIndex("PUCCHDLPathLoss_0xB884")) && row.getList(row.fieldIndex("PUCCHDLPathLoss_0xB884")).size() > 0) row.getList(row.fieldIndex("PUCCHDLPathLoss_0xB884")).get(0) else null
//        SRSDLPathLoss_0xB884 = if (!row.isNullAt(row.fieldIndex("SRSDLPathLoss_0xB884")) && row.getList(row.fieldIndex("SRSDLPathLoss_0xB884")).size() > 0) row.getList(row.fieldIndex("SRSDLPathLoss_0xB884")).get(0) else null
//        PRACPathLoss_0xB884 = if (!row.isNullAt(row.fieldIndex("PRACPathLoss_0xB884")) && row.getList(row.fieldIndex("PRACPathLoss_0xB884")).size() > 0) row.getList(row.fieldIndex("PRACPathLoss_0xB884")).get(0) else null
//        PUSCHTxPwr_0xB884 = if (!row.isNullAt(row.fieldIndex("PUSCHTxPwr_0xB884")) && row.getList(row.fieldIndex("PUSCHTxPwr_0xB884")).size() > 0) row.getList(row.fieldIndex("PUSCHTxPwr_0xB884")).get(0) else null
        val PUSCHDLPathLoss_0xB8D2 = if (!row.isNullAt(row.fieldIndex("PUSCHDLPathLoss_0xB8D2")) ) row.getString(row.fieldIndex("PUSCHDLPathLoss_0xB8D2")) else null
        val PUCCHDLPathLoss_0xB8D2 = if (!row.isNullAt(row.fieldIndex("PUCCHDLPathLoss_0xB8D2")) ) row.getString(row.fieldIndex("PUCCHDLPathLoss_0xB8D2")) else null
        val SRSDLPathLoss_0xB8D2 = if (!row.isNullAt(row.fieldIndex("SRSDLPathLoss_0xB8D2")) ) row.getString(row.fieldIndex("SRSDLPathLoss_0xB8D2")) else null
        val PRACPathLoss_0xB8D2 = if (!row.isNullAt(row.fieldIndex("PRACPathLoss_0xB8D2")) ) row.getString(row.fieldIndex("PRACPathLoss_0xB8D2")) else null
        val PUSCHTxPwr_0xB8D2 = if (!row.isNullAt(row.fieldIndex("PUSCHTxPwr_0xB8D2")) ) row.getString(row.fieldIndex("PUSCHTxPwr_0xB8D2")) else null

        //
        val NRSNR_0xB8D8: List[String] = if (!row.isNullAt(row.fieldIndex("NRSNR_0xB8D8")) && row.getList(row.fieldIndex("NRSNR_0xB8D8")).size() > 1) row.getSeq(row.fieldIndex("NRSNR_0xB8D8")).toList else List.empty

        val NRSBSNR_0xB8DD: lang.Double = if (!row.isNullAt(row.fieldIndex("NRSBSNR_0xB8DD")) ) row.getDouble(row.fieldIndex("NRSBSNR_0xB8DD")) else null
        val NRSBSNRRx0_0xB8DD: lang.Double = if (!row.isNullAt(row.fieldIndex("NRSBSNRRx0_0xB8DD")) ) row.getDouble(row.fieldIndex("NRSBSNRRx0_0xB8DD")) else null
        val NRSBSNRRx1_0xB8DD: lang.Double = if (!row.isNullAt(row.fieldIndex("NRSBSNRRx1_0xB8DD")) ) row.getDouble(row.fieldIndex("NRSBSNRRx1_0xB8DD")) else null
        val NRSBSNRRx2_0xB8DD: lang.Double = if (!row.isNullAt(row.fieldIndex("NRSBSNRRx2_0xB8DD")) ) row.getDouble(row.fieldIndex("NRSBSNRRx2_0xB8DD")) else null
        val NRSBSNRRx3_0xB8DD: lang.Double = if (!row.isNullAt(row.fieldIndex("NRSBSNRRx3_0xB8DD")) ) row.getDouble(row.fieldIndex("NRSBSNRRx3_0xB8DD")) else null

        bandType0xB97F = if (!row.isNullAt(row.fieldIndex("nrbandtype0xB97F"))) row.getString(row.fieldIndex("nrbandtype0xB97F")) else null
        scc1BandType0xB97F = if (!row.isNullAt(row.fieldIndex("nrbandtypeScc10xB97F"))) row.getString(row.fieldIndex("nrbandtypeScc10xB97F")) else null
        nrConnectivityType = if (!row.isNullAt(row.fieldIndex("nrconnectivitytype0xB825")) &&  row.getList(row.fieldIndex("nrconnectivitytype0xB825")).size() > 0) row.getSeq(row.fieldIndex("nrconnectivitytype0xB825")).toList else null
        nrband0xB97F = if (!row.isNullAt(row.fieldIndex("nrbandind0xB97F"))) row.getInt(row.fieldIndex("nrbandind0xB97F")) else null
        nrbandScc10xB97F = if (!row.isNullAt(row.fieldIndex("nrbandScc10xB97F"))) row.getInt(row.fieldIndex("nrbandScc10xB97F")) else null
        nrConnectivityMode = if (!row.isNullAt(row.fieldIndex("nrconnectivitymode0xB825"))) row.getString(row.fieldIndex("nrconnectivitymode0xB825")) else null
        nrPccArfcn0xB97F = if (!row.isNullAt(row.fieldIndex("nrPccArfcn0xB97F"))) row.getInt(row.fieldIndex("nrPccArfcn0xB97F")) else null
        nrScc1Arfcn0xB97F = if (!row.isNullAt(row.fieldIndex("nrScc1Arfcn0xB97F"))) row.getInt(row.fieldIndex("nrScc1Arfcn0xB97F")) else null
        nrCCIndex0xB97F = if (!row.isNullAt(row.fieldIndex("ccindex_0xb97f"))) row.getSeq(row.fieldIndex("ccindex_0xb97f")).toList else null

        if(nrCCIndex0xB97F != null && nrCCIndex0xB97F.nonEmpty) {
          if(nrCCIndex0xB97F.head != 255)  validServingIndex = true
        }
        if(scc1BandType0xB97F != null) {
          if(scc1BandType0xB97F.split(" ")(1) == "(FR1)") {
            if(scc1BandType0xB97F.split(" ")(0) == "C-Band") {
              Scc1BandType = "+ 5G_C-Band"
            } else if(scc1BandType0xB97F.split(" ")(0) == "DSS") {
              Scc1BandType = "+ 5G_NW"
            }
          }
        }
        if(logRecordJoinedDF.kpiisRachExists) {
        nrSetupAttempt = if (!row.isNullAt(row.fieldIndex("Nrsetupattempt"))) row.getString(row.fieldIndex("Nrsetupattempt")) else null
        nrSetupSuccess = if (!row.isNullAt(row.fieldIndex("Nrsetupsuccess"))) row.getString(row.fieldIndex("Nrsetupsuccess")) else null
        nrSetupFailure = if (!row.isNullAt(row.fieldIndex("Nrsetupfailure"))) row.getString(row.fieldIndex("Nrsetupfailure")) else null
      //  nrsetupForNrScgState = if (!row.isNullAt(row.fieldIndex("nrsetupFornrscgstate"))) row.getString(row.fieldIndex("nrsetupFornrscgstate")) else null
        NrReleaseEvt = if (!row.isNullAt(row.fieldIndex("nrReleaseEvt"))) row.getInt(row.fieldIndex("nrReleaseEvt")) else null
        NrSetupSuccessCnt = if (!row.isNullAt(row.fieldIndex("nrsetupsuccesscnt"))) row.getLong(row.fieldIndex("nrsetupsuccesscnt")) else null
        NrSetupFailureCnt = if (!row.isNullAt(row.fieldIndex("nrsetupfailurecnt"))) row.getLong(row.fieldIndex("nrsetupfailurecnt")) else null
          NrReleaseCnt = if (!row.isNullAt(row.fieldIndex("nrreleasecnt"))) row.getLong(row.fieldIndex("nrreleasecnt")) else null
          NrSetupAttemptCnt = if (!row.isNullAt(row.fieldIndex("nrsetupattemptcnt"))) row.getLong(row.fieldIndex("nrsetupattemptcnt")) else null
        }
        //        phychanBitMaskVal = if(PhychanBitMask_0xB884 != null) getPhychanBitMask(PhychanBitMask_0xB884) else null
        if (!Nrs_ServingPciB975.isEmpty) {
        Nrs_brsrp = if (!row.isNullAt(row.fieldIndex("Serving_BRSRP"))) row.getString(row.fieldIndex("Serving_BRSRP")) else null
        Nrs_brsrq = if (!row.isNullAt(row.fieldIndex("Serving_BRSRQ"))) row.getString(row.fieldIndex("Serving_BRSRQ")) else null
        Nrs_bsnr = if (!row.isNullAt(row.fieldIndex("Serving_BSNR"))) row.getString(row.fieldIndex("Serving_BSNR")) else null
        Nrs_beamcount = if (!row.isNullAt(row.fieldIndex("Serving_nrbeamcount"))) row.getString(row.fieldIndex("Serving_nrbeamcount")) else null
        Nrs_ccindex = if (!row.isNullAt(row.fieldIndex("Serving_ccindex"))) row.getString(row.fieldIndex("Serving_ccindex")) else null
        Nrs_pci = if (!row.isNullAt(row.fieldIndex("Serving_nrpci"))) row.getString(row.fieldIndex("Serving_nrpci")) else null
        NRServingBeamIndexVal = if (!row.isNullAt(row.fieldIndex("NRServing_BeamIndex"))) row.getInt(row.fieldIndex("NRServing_BeamIndex")) else null
        //  NrBeamRsrpLastLogPacket = if (!row.isNullAt(row.fieldIndex("beamRsrspOfPreviousLogPac"))) row.getString(row.fieldIndex("beamRsrspOfPreviousLogPac")) else null
        } else if(Nrs_ServingPciB97F != null) {
          servingBeamKpis0xB97f = getServingBeam0xb97fKpis(row,isLteRrcState)
        }

        val nrs_bsnr_0xb992 = if (!row.isNullAt(row.fieldIndex("NRSBSNR_0xB992"))) row.getString(row.fieldIndex("NRSBSNR_0xB992")) else null
        val nrServingRxBeamSNR0_0xB992 = if (!row.isNullAt(row.fieldIndex("NRServingRxBeamSNR0_0xB992"))) row.getFloat(row.fieldIndex("NRServingRxBeamSNR0_0xB992")) else null
        val nrServingRxBeamSNR1_0xB992 = if (!row.isNullAt(row.fieldIndex("NRServingRxBeamSNR1_0xB992"))) row.getFloat(row.fieldIndex("NRServingRxBeamSNR1_0xB992")) else null

        //"rachsuccessevent","rachfailureevent","rachabortevent","beamIndexChangedEvent","nrServingBeamIndexTransition","numBeamSwitch","bestReportBeamRsrp","totalRachEventCount",
        //      "totalRachSuccessEvents","totalRachFailureEvents","totalRachAbortedEvents","rachSuccessRate","rachLatency"
        var nrBeamMap:mutable.Map[String,Any]  = null
        var nr5GTputKpis:mutable.Map[String,Any]  = null
        if(logRecordJoinedDF.kpiisRachExists) nrBeamMap = extractRachAndBeamKpis(row,commonConfigParams)
        nr5GTputKpis = extractThroughputKpis(row,logRecordJoinedDF)
        val nr5GScgBlerKpis = extract5GScgBlerKpis(row)
        var rrcCon5gDlSessionDataBytes: lang.Long = null
        if(logRecordJoinedDF.kpi5gDlDataBytesExists) {
          rrcCon5gDlSessionDataBytes = if (!row.isNullAt(row.fieldIndex("sessionDlDataBytes"))) row.getLong(row.fieldIndex("sessionDlDataBytes")) else null
        }
        val nr5gMacUlKPIsMap:mutable.Map[String,Any]  = getNr5gMacUlKPIsfrom0xB883(row)

        val EarfcnNcell: String = if (!row.isNullAt(row.fieldIndex("EARFCN_collected"))) row.getString(row.fieldIndex("EARFCN_collected")) else null
        val PciNcell: String = if (!row.isNullAt(row.fieldIndex("PCI_collected"))) row.getString(row.fieldIndex("PCI_collected")) else null
        val RrsrpNcell: String = if (!row.isNullAt(row.fieldIndex("Rsrp_collected"))) row.getString(row.fieldIndex("Rsrp_collected")) else null
        val RSRPRx0_Ncell: String = if (!row.isNullAt(row.fieldIndex("RsrpRx0_collected"))) row.getString(row.fieldIndex("RsrpRx0_collected")) else null
        val RSRPRx1_Ncell: String = if (!row.isNullAt(row.fieldIndex("RsrpRx1_collected"))) row.getString(row.fieldIndex("RsrpRx1_collected")) else null
        val RsrqNcell: String = if (!row.isNullAt(row.fieldIndex("Rsrq_collected"))) row.getString(row.fieldIndex("Rsrq_collected")) else null
        val RSRQRx0_Ncell: String = if (!row.isNullAt(row.fieldIndex("RsrqRx0_collected"))) row.getString(row.fieldIndex("RsrqRx0_collected")) else null
        val RSRQRx1_Ncell: String = if (!row.isNullAt(row.fieldIndex("RsrqRx1_collected"))) row.getString(row.fieldIndex("RsrqRx1_collected")) else null
        val RssiNcell: String = if (!row.isNullAt(row.fieldIndex("Rssi_collected"))) row.getString(row.fieldIndex("Rssi_collected")) else null
        val RSSIRx0_Ncell: String = if (!row.isNullAt(row.fieldIndex("RssiRx0_collected"))) row.getString(row.fieldIndex("RssiRx0_collected")) else null
        val RSSIRx1_Ncell: String = if (!row.isNullAt(row.fieldIndex("RssiRx1_collected"))) row.getString(row.fieldIndex("RssiRx1_collected")) else null

        val VoLTE_Jitter: String = if (!row.isNullAt(row.fieldIndex("vvqJitter"))) row.getString(row.fieldIndex("vvqJitter")) else ""
        val VoLTE_Vvq: String = if (!row.isNullAt(row.fieldIndex("vvq"))) row.getString(row.fieldIndex("vvq")) else ""
        //  val VoLTE_RtpLoss: String = if (!row.isNullAt(row.fieldIndex("rtpLoss"))) row.getString(row.fieldIndex("rtpLoss")) else ""
        val LTE_RRC_State: String = if (!row.isNullAt(row.fieldIndex("lteRrcState"))) row.getString(row.fieldIndex("lteRrcState")) else null

        val firstRecordFlag: Integer = if (!row.isNullAt(row.fieldIndex("rnFR"))) row.getInt(row.fieldIndex("rnFR")) else null
        val lastRecordFlag: Integer = if (!row.isNullAt(row.fieldIndex("rnLR"))) row.getInt(row.fieldIndex("rnLR")) else null
        val nrScgCfgEvt: Integer = if (!row.isNullAt(row.fieldIndex("nrScgCfgEvt"))) row.getInt(row.fieldIndex("nrScgCfgEvt")) else null

        var rtpdlsnPacketLoss: lang.Long = null
        var rtpPacketLoss_3Seconds: lang.Long = null

        if (logRecordJoinedDF.kpiDmatIpExists && commonConfigParams.KPI_RTP_PACKET) {
          rtpdlsnPacketLoss = if (!row.isNullAt(row.fieldIndex("rtpdlsnCountAgg"))) row.getLong(row.fieldIndex("rtpdlsnCountAgg")) else null
          rtpPacketLoss_3Seconds = if (!row.isNullAt(row.fieldIndex("rtpPacketLoss_3Seconds"))) row.getLong(row.fieldIndex("rtpPacketLoss_3Seconds")) else null
        }

        var VoLTETriggerEvtRtpGap: Integer = null
        var VoLTECallEndEvtRtpGap: Integer = null
        var rtpGapEvt: String = null
        var VoLTECallStart: String = null
        var VoLTECallEnd: String = null
        var rtpGapIncomingDuration: Float = 0
        var rtpGapOutgoingDuration: Float = 0
        var VoLTESubtitleLast25Secs: String = null
        var VoLTESeqLast25Secs: String = null
        var NasMsgLast25Secs:String = null

        var VoLTERtpRxDeltaDelayInc: lang.Double = null
        var VoLTERtpRxDeltaDelayOut: lang.Double = null
        var VoLTERtpRxDelayInc: lang.Long = null
        var VoLTERtpRxDelayOut: lang.Long = null
        var volteCallIneffective: Boolean = false
        var rachResult0xb88a:String = null
        var nrbeamFailureLag:Integer = null
        var ipSubtitle:String = null
        var lteConnSetupFailure:Boolean = false
        var NrSetupFailure:Boolean = false

        if(logRecordJoinedDF.kpiisRachExists) {
        rachResult0xb88a = if (!row.isNullAt(row.fieldIndex("rachResult_0xb88a"))) row.getString(row.fieldIndex("rachResult_0xb88a")) else null
        nrbeamFailureLag = if (!row.isNullAt(row.fieldIndex("nrbeamFailureLag"))) row.getInt(row.fieldIndex("nrbeamFailureLag")) else null
        }

        if(logRecordJoinedDF.kpiDmatIpExists) {
          VoLTETriggerEvtRtpGap = if (!row.isNullAt(row.fieldIndex("VoLTETriggerEvtRtpGap"))) row.getInt(row.fieldIndex("VoLTETriggerEvtRtpGap")) else null
          VoLTECallEndEvtRtpGap = if (!row.isNullAt(row.fieldIndex("VoLTECallEndEvtRtpGap"))) row.getInt(row.fieldIndex("VoLTECallEndEvtRtpGap")) else null
          rtpGapEvt = if (!row.isNullAt(row.fieldIndex("rtpGap"))) row.getString(row.fieldIndex("rtpGap")) else null

        }

        var VolteCallIneffecRachCheck: Boolean = false

        if(rachResult0xb88a != null && nrbeamFailureLag != null) {
          if(rachResult0xb88a=="ABORTED") {
            var evtInfo:LteIneffEventInfo = LteIneffEventInfo()
            volteCallIneffective = true
            VolteCallIneffecRachCheck = true
            val evtTimestamp = getTimestampPG(row.getString(row.fieldIndex("dateWithoutMillis"))).getOrElse().asInstanceOf[Timestamp]
            evtInfo = evtInfo.copy(testId = row.getInt(row.fieldIndex("testId")),fileName = Constants.getDlfFromSeq(fileName), evtTimestamp, beamRecoveryFailEvt =1, fileLocation = row.getString(row.fieldIndex("fileLocation")))
            // CommonDaoImpl.insertEventsInfoWhileES(commonConfigParams,evtInfo)
            CommonDaoImpl.updateEventsCountWhileES(commonConfigParams,evtInfo)
          }
        }
        if (lteRrcConSetup != null) {
          if(lteRrcConSetup ==1 && postRrcConSetupCmp == null) {
            var evtInfo:LteIneffEventInfo = LteIneffEventInfo()
            lteConnSetupFailure = true
            val evtTimestamp = getTimestampPG(row.getString(row.fieldIndex("dateWithoutMillis"))).getOrElse().asInstanceOf[Timestamp]
            evtInfo = evtInfo.copy(testId = row.getInt(row.fieldIndex("testId")),fileName = Constants.getDlfFromSeq(fileName), evtTimestamp,  lteRrcCnnSetupFailEvt=1, fileLocation = row.getString(row.fieldIndex("fileLocation")))
            // CommonDaoImpl.insertEventsInfoWhileES(commonConfigParams,evtInfo)
            CommonDaoImpl.updateEventsCountWhileES(commonConfigParams,evtInfo)
          }
        }
        if (nrSetupFailure != null) {
          if(nrSetupFailure=="1") {
            var evtInfo:LteIneffEventInfo = LteIneffEventInfo()
            NrSetupFailure = true
            val evtTimestamp = getTimestampPG(row.getString(row.fieldIndex("dateWithoutMillis"))).getOrElse().asInstanceOf[Timestamp]
            evtInfo = evtInfo.copy(testId = row.getInt(row.fieldIndex("testId")),fileName = Constants.getDlfFromSeq(fileName), evtTimestamp,  nrSetupFailure=1, fileLocation = row.getString(row.fieldIndex("fileLocation")))
            // CommonDaoImpl.insertEventsInfoWhileES(commonConfigParams,evtInfo)
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
          ipSubtitle = if (!row.isNullAt(row.fieldIndex("ipSubtitle"))) row.getString(row.fieldIndex("ipSubtitle")) else null
        }

        var VolteCallIneffec25SecCheck: Boolean = false
        if(VoLTESubtitleLast25Secs != null && VoLTESeqLast25Secs != null) {

          if (checkVolteCallFailure(VoLTESubtitleLast25Secs,VoLTESeqLast25Secs,NasMsgLast25Secs)==1) {
            var evtInfo:LteIneffEventInfo = LteIneffEventInfo()
            volteCallIneffective = true
            VolteCallIneffec25SecCheck = true
            val evtTimestamp = getTimestampPG(row.getString(row.fieldIndex("dateWithoutMillis"))).getOrElse().asInstanceOf[Timestamp]
            evtInfo = evtInfo.copy(testId = row.getInt(row.fieldIndex("testId")), fileName = Constants.getDlfFromSeq(fileName), evtTimestamp, ltecallinefatmpt = 1, fileLocation = row.getString(row.fieldIndex("fileLocation")))
            // CommonDaoImpl.insertEventsInfoWhileES(commonConfigParams, evtInfo)
            CommonDaoImpl.updateEventsCountWhileES(commonConfigParams, evtInfo)
          }
        }

        if (!VolteCallIneffec25SecCheck && VolteCallIneffecRachCheck) {
          var evtInfo: LteIneffEventInfo = LteIneffEventInfo()
          val evtTimestamp = getTimestampPG(row.getString(row.fieldIndex("dateWithoutMillis"))).getOrElse().asInstanceOf[Timestamp]
          evtInfo = evtInfo.copy(testId = row.getInt(row.fieldIndex("testId")), fileName = Constants.getDlfFromSeq(fileName), evtTimestamp, ltecallinefatmpt = 1, fileLocation = row.getString(row.fieldIndex("fileLocation")))
          // CommonDaoImpl.insertEventsInfoWhileES(commonConfigParams,evtInfo)
          CommonDaoImpl.updateEventsCountWhileES(commonConfigParams, evtInfo)
        }
        val sPcellKpiMap:mutable.Map[String,Any]  = get5gsPCellKPIs(row)
        val reTxBlerKPIMap:mutable.Map[String,Any]  = getReTxBlerKPIs(row)
        val NrPCIs0xB887Map:mutable.Map[String,Any]  = getNrCarrierComponentPCIs(row)
        // val CDRX0xB890Map:scala.collection.mutable.Map[String,Any]  = getCdrxKpis(row)
        // val nr0xB8A7Map: scala.collection.mutable.Map[String, Any] = getNr0xB8A7KPIs(row)
        val rm0xF0E4Map: mutable.Map[String, Any] = getRootMetricsTestKpis(row)

        var NrDlScg1SecTput:lang.Double = null
        var NrDlScg1SecPccTput:lang.Double = null
        var NrDlScg1SecScc1Tput:lang.Double = null
        var NrDlScg1SecScc2Tput:lang.Double = null
        var NrDlScg1SecScc3Tput:lang.Double = null
        var NrDlScg1SecScc4Tput:lang.Double = null
        var NrDlScg1SecScc5Tput:lang.Double = null
        var NrDlScg1SecScc6Tput	:lang.Double = null
        var NrDlScg1SecScc7Tput:lang.Double = null

        var carrierIDCount: String = null
        var carrierId0xB887: List[String] = null
        var Nrdlmcs0xB887: List[String] = null
        var carrierIDsFoAvgRb0xB887: String = null
        var numerologyForAvgRb0xB887: String = null
        var pccCarrierIdCount: String = null
        var scc1CarrierIdCount: String = null
        var scc2CarrierIdCount: String = null
        var scc3CarrierIdCount: String = null
        var scc4CarrierIdCount: String = null
        var scc5CarrierIdCount: String = null
        var scc6CarrierIdCount: String = null
        var scc7CarrierIdCount: String = null

        val NRTxBeamIndexPCC: Integer = if (!row.isNullAt(row.fieldIndex("NRTxBeamIndexPCC")) && row.getList(row.fieldIndex("NRTxBeamIndexPCC")).size() > 0) row.getSeq(row.fieldIndex("NRTxBeamIndexPCC")).toList.head else null
        val NRTxBeamIndexSCC1: Integer = if (!row.isNullAt(row.fieldIndex("NRTxBeamIndexSCC1")) && row.getList(row.fieldIndex("NRTxBeamIndexSCC1")).size() > 0) row.getSeq(row.fieldIndex("NRTxBeamIndexSCC1")).toList.head else null

        var cdrxkpislist = if (!row.isNullAt(row.fieldIndex("cdrxkpilist_0xB890")) && row.getList(row.fieldIndex("cdrxkpilist_0xB890")).size() > 0) row.getSeq(row.fieldIndex("cdrxkpilist_0xB890")).toList else null

        carrierId0xB887 = if (!row.isNullAt(row.fieldIndex("CarrierID0xB887")) && row.getList(row.fieldIndex("CarrierID0xB887")).size() > 0) row.getSeq(row.fieldIndex("CarrierID0xB887")).toList else null
        Nrdlmcs0xB887 = if (!row.isNullAt(row.fieldIndex("NRDLMCS0xB887")) && row.getList(row.fieldIndex("NRDLMCS0xB887")).size() > 0) row.getSeq(row.fieldIndex("NRDLMCS0xB887")).toList else null
        val NRMIMOType_0xB887: String = if (!row.isNullAt(row.fieldIndex("NRMIMOType_0xB887")) && row.getList(row.fieldIndex("NRMIMOType_0xB887")).size() > 0) row.getSeq(row.fieldIndex("NRMIMOType_0xB887")).toList.head.toString else null
        //logger.info(s"NRMIMOType_0xB887 = $NRMIMOType_0xB887")

        if (logRecordJoinedDF.kpi5gDlDataBytesExists) {
          carrierIDCount = if (!row.isNullAt(row.fieldIndex("carrierIDCount"))) row.getString(row.fieldIndex("carrierIDCount")) else null
          carrierIDsFoAvgRb0xB887 = if (!row.isNullAt(row.fieldIndex("carrierIDsFoAvgRbCal"))) row.getString(row.fieldIndex("carrierIDsFoAvgRbCal")) else null
          numerologyForAvgRb0xB887 = if (!row.isNullAt(row.fieldIndex("numerologyForAvgRbCal"))) row.getString(row.fieldIndex("numerologyForAvgRbCal")) else null


          if (carrierIDsFoAvgRb0xB887 != null && !carrierIDsFoAvgRb0xB887.isEmpty) {
            var countPerCarrierId = carrierIDsFoAvgRb0xB887.split(",").map(_.trim).toList.groupBy(i => i).mapValues(_.size)
            pccCarrierIdCount = Try(countPerCarrierId.get("0").get.asInstanceOf[Integer].toString).getOrElse("")
            scc1CarrierIdCount = Try(countPerCarrierId.get("1").get.asInstanceOf[Integer].toString).getOrElse("")
            scc2CarrierIdCount = Try(countPerCarrierId.get("2").get.asInstanceOf[Integer].toString).getOrElse("")
            scc3CarrierIdCount = Try(countPerCarrierId.get("3").get.asInstanceOf[Integer].toString).getOrElse("")
            scc4CarrierIdCount = Try(countPerCarrierId.get("4").get.asInstanceOf[Integer].toString).getOrElse("")
            scc5CarrierIdCount = Try(countPerCarrierId.get("5").get.asInstanceOf[Integer].toString).getOrElse("")
            scc6CarrierIdCount = Try(countPerCarrierId.get("6").get.asInstanceOf[Integer].toString).getOrElse("")
            scc7CarrierIdCount = Try(countPerCarrierId.get("7").get.asInstanceOf[Integer].toString).getOrElse("")

          }

        }

        var NrDlScg5MsTput: lang.Double = null
       // var timeDiffFor5MsTput: java.lang.Long = 0
        if(logRecordJoinedDF.kpiNrDlScg5MsTputExists) {
        //  timeDiffFor5MsTput =  if (!row.isNullAt(row.fieldIndex("timeDff"))) row.getLong(row.fieldIndex("timeDff")) else null
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

        var DlL1PccTput:lang.Double = null
        var DlL1Scc1Tput:lang.Double = null
        var DlL1Scc2Tput:lang.Double = null
        var DlL1Scc3Tput:lang.Double = null
        var DlL1Scc4Tput:lang.Double = null
        var DlL1Scc5Tput:lang.Double = null
        var DlL1Scc6Tput:lang.Double = null
        var DlL1Scc7Tput	:lang.Double = null
        var overallDlL1Tput:lang.Double = null

        val tbSizePcellTPUT_0xB173: lang.Long = if (!row.isNullAt(row.fieldIndex("pcellTBSizeSum0xB173"))) row.getLong(row.fieldIndex("pcellTBSizeSum0xB173")) else null
        val tbSizeScc1TPUT_0xB173: lang.Long = if (!row.isNullAt(row.fieldIndex("scc1TBSizeSum0xB173"))) row.getLong(row.fieldIndex("scc1TBSizeSum0xB173")) else null
        val tbSizeScc2TPUT_0xB173: lang.Long = if (!row.isNullAt(row.fieldIndex("scc2TBSizeSum0xB173"))) row.getLong(row.fieldIndex("scc2TBSizeSum0xB173")) else null
        val tbSizeScc3TPUT_0xB173: lang.Long = if (!row.isNullAt(row.fieldIndex("scc3TBSizeSum0xB173"))) row.getLong(row.fieldIndex("scc3TBSizeSum0xB173")) else null
        val tbSizeScc4TPUT_0xB173: lang.Long = if (!row.isNullAt(row.fieldIndex("scc4TBSizeSum0xB173"))) row.getLong(row.fieldIndex("scc4TBSizeSum0xB173")) else null
        val tbSizeScc5TPUT_0xB173: lang.Long = if (!row.isNullAt(row.fieldIndex("scc5TBSizeSum0xB173"))) row.getLong(row.fieldIndex("scc5TBSizeSum0xB173")) else null
        val tbSizeScc6TPUT_0xB173: lang.Long = if (!row.isNullAt(row.fieldIndex("scc6TBSizeSum0xB173"))) row.getLong(row.fieldIndex("scc6TBSizeSum0xB173")) else null
        val tbSizeScc7TPUT_0xB173: lang.Long = if (!row.isNullAt(row.fieldIndex("scc7TBSizeSum0xB173"))) row.getLong(row.fieldIndex("scc7TBSizeSum0xB173")) else null
        val tbSizeOverallPUT_0xB173: lang.Long = if (!row.isNullAt(row.fieldIndex("totalTBSizeSum0xB173"))) row.getLong(row.fieldIndex("totalTBSizeSum0xB173")) else null
        val distServingCellIndex_0xB173: Integer= if (!row.isNullAt(row.fieldIndex("distServingCellIndex_0xB173"))) row.getInt(row.fieldIndex("distServingCellIndex_0xB173")) else null

        val firstfn_0xB173: Integer = if (!row.isNullAt(row.fieldIndex("firstFn_0xB173"))) row.getInt(row.fieldIndex("firstFn_0xB173")) else 0
        val firstSfn_0xB173: Integer = if (!row.isNullAt(row.fieldIndex("firstSfn_0xB173"))) row.getInt(row.fieldIndex("firstSfn_0xB173")) else 0
        val lastfn_0xB173: Integer = if (!row.isNullAt(row.fieldIndex("lastFn_0xB173"))) row.getInt(row.fieldIndex("lastFn_0xB173")) else 0
        val lastSfn_15kHzb173: Integer = if (!row.isNullAt(row.fieldIndex("lastSfn_0xB173"))) row.getInt(row.fieldIndex("lastSfn_0xB173")) else 0

        val Total_elapsed_time = ParseUtils.CalculateMsFromSfnAndSubFrames(firstfn_0xB173, firstSfn_0xB173, lastfn_0xB173, lastSfn_15kHzb173)
        if(tbSizePcellTPUT_0xB173 != null) { DlL1PccTput = (tbSizePcellTPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeScc1TPUT_0xB173 != null) { DlL1Scc1Tput = (tbSizeScc1TPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeScc2TPUT_0xB173 != null) { DlL1Scc2Tput = (tbSizeScc2TPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeScc3TPUT_0xB173 != null) { DlL1Scc3Tput = (tbSizeScc3TPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeScc4TPUT_0xB173 != null) { DlL1Scc4Tput = (tbSizeScc4TPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeScc5TPUT_0xB173 != null) { DlL1Scc5Tput = (tbSizeScc5TPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeScc6TPUT_0xB173 != null) { DlL1Scc6Tput = (tbSizeScc6TPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeScc7TPUT_0xB173 != null) { DlL1Scc7Tput = (tbSizeScc7TPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}
        if(tbSizeOverallPUT_0xB173 != null) { overallDlL1Tput = (tbSizeOverallPUT_0xB173 * 8 * (1000 / Total_elapsed_time)) / 1000000}


        val SRS_Tx_Power: Integer = if (!row.isNullAt(row.fieldIndex("srsTxPower")) && row.getList(row.fieldIndex("srsTxPower")).size() > 0) row.getList(row.fieldIndex("srsTxPower")).get(0) else null

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

        val technology:String = if (!row.isNullAt(row.fieldIndex("technology_logs"))) row.getString(row.fieldIndex("technology_logs")) else null
        var nrScgStateKPIMap:mutable.Map[String,Any] = null
//        if(technology=="5g")  {
//           nrScgStateKPIMap = getNrScgStateKPI(row,fileNameWithExt,NRS_RSRP)
//        }
        var lacId: Int = calcilateLacIdAndPlmnId(Gsm_Lai)._1
        var plmnId: String = calcilateLacIdAndPlmnId(Gsm_Lai)._2

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
            var rlcDlBytesDiff = 0D
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
            if(servingCellIndex != null && logpacket2servingCellIndex != null && logpacket3servingCellIndex != null) {
              var servingCellIndexLists = List(servingCellIndex,logpacket2servingCellIndex,logpacket3servingCellIndex)
              ServingIndexSize_0xB173 = servingCellIndexLists.flatMap(_.zipWithIndex).sortBy(_._2).map(_._1).distinct.size
            }

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
            logger.error("Error: Exception during phy throughput calculation: " + row.get(row.fieldIndex("fileName")).toString, e)

        }

        if (firstRecordFlag != null && firstRecordFlag == 1) {

          val regionsName: StateRegionCounty = CommonDaoImpl.getStateCountyRegionsForXY(exponentialConvertor(mx.toString), exponentialConvertor(my.toString),commonConfigParams)
          val vz_regions: String = if (regionsName.name.nonEmpty) regionsName.name else ""
          var evtInfo: LteIneffEventInfo = LteIneffEventInfo()
          var totalEvts = if (!row.isNullAt(row.fieldIndex("totalEventsCnt"))) row.getInt(row.fieldIndex("totalEventsCnt")) else null
          /* if(totalEvts != null) {
             println("total events count>>>>" + row.getInt(row.fieldIndex("totalEventsCnt")))
           } */
          evtInfo = evtInfo.copy(testId = row.getInt(row.fieldIndex("testId")),fileName = Constants.getDlfFromSeq(fileName), evtsCount = row.getInt(row.fieldIndex("totalEventsCnt")), vzRegions = vz_regions, marketArea = area)
          CommonDaoImpl.updateEventsCountWhileES(commonConfigParams, evtInfo)
        }

        if (nrConnectivityMode != null) {
          var evtInfo:LteIneffEventInfo = LteIneffEventInfo()
          var nrconntivity = CommonDaoImpl.getNrconnecityModeInfo(row.getInt(row.fieldIndex("testId")).toString,commonConfigParams)
          var nrconnecitivtyList:Array[LVFileMetaInfo] = nrconntivity.filter(x => x.nrConnecitivityMode=="SA" || x.nrConnecitivityMode=="NSA")

          if(nrconnecitivtyList.take(1).nonEmpty) {
            var NrConnMode = nrconnecitivtyList.map(row=> row.nrConnecitivityMode).toList
            if(NrConnMode.size>0 && NrConnMode.size < 2)  {
              if(NrConnMode.head != nrConnectivityMode) {
                evtInfo = evtInfo.copy(testId = row.getInt(row.fieldIndex("testId")),fileName = Constants.getDlfFromSeq(fileName), evtsCount = 0, nrconnectivitymode = NrConnMode.head + "," + nrConnectivityMode)
                CommonDaoImpl.updateEventsCountWhileES(commonConfigParams,evtInfo)
              }
            }
          } else  {
            evtInfo = evtInfo.copy(testId = row.getInt(row.fieldIndex("testId")),fileName = Constants.getDlfFromSeq(fileName), evtsCount = 0, nrconnectivitymode = nrConnectivityMode)
            CommonDaoImpl.updateEventsCountWhileES(commonConfigParams,evtInfo)
          }
        }
        var IpSubtitle:String = if (!row.isNullAt(row.fieldIndex("subtitle"))) row.getString(row.fieldIndex("subtitle")) else null
        var HttpHost:String = if (!row.isNullAt(row.fieldIndex("httpHost"))) row.getString(row.fieldIndex("httpHost")) else null
        var HttpMethod:String = if (!row.isNullAt(row.fieldIndex("httpMethod"))) row.getString(row.fieldIndex("httpMethod")) else null
        var HttpContentLength:lang.Float = if (!row.isNullAt(row.fieldIndex("httpContentLength"))) row.getFloat(row.fieldIndex("httpContentLength")) else null
        var timestamp:String = if (!row.isNullAt(row.fieldIndex("dateWithoutMillis"))) row.getString(row.fieldIndex("dateWithoutMillis")) else null

        val mapWithoutMetadata = Map(
          "ObjectId" -> scala.util.Random.nextInt(),
          "TestId" -> row.getInt(row.fieldIndex("testId")),
          "FileName" -> Constants.getDlfFromSeq(fileName)) ++
          ((if (!reportFlag && row.getString(row.fieldIndex("dateWithoutMillis")) != null) Map("TimeStamp" -> getTimestamp(row.getString(row.fieldIndex("dateWithoutMillis"))).getOrElse(),
            "internal_index" -> getIndexName(row.getString(row.fieldIndex("dateWithoutMillis")),
              if (row.fieldIndex("triggerCount") == 1) "DLF" else fileNameWithExt.split('.')(1).toUpperCase,
              Constants.ES_NON_VMAS_ROUTINE,
              if (!row.isNullAt(row.fieldIndex("deviceOS"))) row.getString(row.fieldIndex("deviceOS")) else StringUtils.EMPTY)) else Nil) ++
            (if (fileNameWithExt != null) Map("Type" -> fileNameWithExt.split('.')(1).toUpperCase, "internal_type" -> getTypeName(fileNameWithExt.split('.')(1).toUpperCase)) else Nil) ++
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
            (if (!row.isNullAt(row.fieldIndex("eventsCount")) && fileType != "SIG") Map("EventsCount" -> row.getInt(row.fieldIndex("eventsCount"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("modelName_logs"))) Map("ModelName" -> row.getString(row.fieldIndex("modelName_logs"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("oemName"))) Map("OEMName" -> row.getString(row.fieldIndex("oemName"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("imsStatus"))) Map("IMSStatus" -> row.getString(row.fieldIndex("imsStatus"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("imsFeatureTag"))) Map("IMSFeatureTag" -> row.getString(row.fieldIndex("imsFeatureTag"))) else Nil) ++
            (if ((hrpdState != null) && !hrpdState.isEmpty) Map("EvDOeHRPDState" -> hrpdState) else Nil) ++
            (if (pdnRat != null && !pdnRat.isEmpty) Map("EvDOPdnRat" -> pdnRat) else Nil) ++
            (if (GSM_l2State != null && !GSM_l2State.isEmpty) Map("GSML2State" -> GSM_l2State) else Nil) ++
            (if (GSM_rrState != null && !GSM_rrState.isEmpty) Map("GSMRrState" -> GSM_rrState) else Nil) ++
            (if (lteTransModeUl != null) {
              if (lteTransModeUl >= 0) Map("LTETxModeUL" -> lteTransModeUl.toString) else Nil
            } else Nil) ++
            (if (lteTransModeDl != null) {
              if (lteTransModeDl >= 0) Map("LTETxModeDL" -> lteTransModeDl.toString) else Nil
            } else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("VoLTECallNormalRelEvt"))) Map("LTEVoLTENormalRelease" -> row.getInt(row.fieldIndex("VoLTECallNormalRelEvt"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("VoLTEDropEvt"))) Map("LTEVoLTEDrop" -> row.getInt(row.fieldIndex("VoLTEDropEvt"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("VoLTEAbortEvt"))) Map("LTEVoLTEAbort" -> row.getInt(row.fieldIndex("VoLTEAbortEvt"))) else Nil) ++
            //(if (!row.isNullAt(row.fieldIndex("VoLTETriggerEvt"))) Map("LTEVoLTETrigger" -> row.getInt(row.fieldIndex("VoLTETriggerEvt"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("lteOos"))) Map("LTEOos" -> row.getInt(row.fieldIndex("lteOos"))) else Nil) ++
            (if (row.getString(row.fieldIndex("HoFailureCause")) != null) Map("IntraLteHoFailureCause" -> row.getString(row.fieldIndex("HoFailureCause"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("HoFailure"))) Map("LTEHoFailure" -> row.getInt(row.fieldIndex("HoFailure"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("lteIntraHoFail"))) Map("LTEIntraHoFail" -> row.getInt(row.fieldIndex("lteIntraHoFail"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("lteIntraReselFail"))) Map("LTEIntraReselFail" -> row.getInt(row.fieldIndex("lteIntraReselFail"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("lteReselFromGsmUmtsFail"))) Map("LTEReselFromGsmUmtsFail" -> row.getInt(row.fieldIndex("lteReselFromGsmUmtsFail"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("lteRlf"))) Map("LTERlf" -> row.getInt(row.fieldIndex("lteRlf"))) else Nil) ++
           // (if (!row.isNullAt(row.fieldIndex("nrRlf"))) Map("NRRlf" -> row.getInt(row.fieldIndex("nrRlf"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("NRRlfEvt"))) Map("NRRlf" -> row.getInt(row.fieldIndex("NRRlfEvt"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("NRRrcNewCellIndEvt"))) Map("NRRrcNewCellIndEvt" -> row.getInt(row.fieldIndex("NRRrcNewCellIndEvt"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("NRRrcHoFailEvt"))) Map("NRRrcHoFailEvt" -> row.getInt(row.fieldIndex("NRRrcHoFailEvt"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("NRScgFailureEvt"))) Map("NRScgFailureEvt" -> row.getInt(row.fieldIndex("NRScgFailureEvt"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("NRBeamFailureEvt"))) Map("NRBeamFailureEvt" -> row.getInt(row.fieldIndex("NRBeamFailureEvt"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("NRRlfEvtCause"))) Map("NRRLFCause" -> row.getString(row.fieldIndex("NRRlfEvtCause"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("NRScgFailureEvtCause"))) Map("NRSCGFailureCause" -> row.getString(row.fieldIndex("NRScgFailureEvtCause")).split(" ")(0)) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("lteMobilityFromEutraFail"))) Map("LTEMobilityFromEutraFail" -> row.getInt(row.fieldIndex("lteMobilityFromEutraFail"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("lteSibReadFailure"))) Map("LTESibReadFailure" -> row.getInt(row.fieldIndex("lteSibReadFailure"))) else Nil) ++
            (if (!row.isNullAt(row.fieldIndex("ROAM_STATUS")) && row.getInt(row.fieldIndex("ROAM_STATUS")) >= 0) Map("RoamingStatus" -> row.getInt(row.fieldIndex("ROAM_STATUS")).toString()) else Nil) ++
            //  (if (!WiFi_linkSpeed.isEmpty && WiFi_linkSpeed.indexOf(" ") > 0) Map("WiFilinkSpeed" -> WiFi_linkSpeed.split(" Mbps")(0).toFloat) else Nil) ++
            (if (GSM_cellID != null) Map("GSMCellId" -> GSM_cellID) else Nil) ++
            (if (PuschTxPwr != null && PuschTxPwr < 24) Map("LTEPuschTxPwr" -> PuschTxPwr) else Nil) ++
            (if (femtoStatus != null && fileType != "SIG") Map("LTEFemtoStatus" -> femtoStatus.toString) else Nil) ++
            (if (evdoRxPwr != null) Map("EvDORxPwr" -> evdoRxPwr.toFloat) else Nil) ++
            (if (evdoTxPwr != null) Map("EvDOTxPwr" -> evdoTxPwr.toFloat) else Nil) ++
            (if (timingAdvanceEvt != null) Map("LTEMacCeTa" -> timingAdvanceEvt) else Nil) ++
            (if (firstRecordFlag != null && firstRecordFlag == 1 && fileType != "SIG") Map("firstRecord" -> true) else Nil) ++
            //last gps point to indicate end flag for a log file
            (if (lastRecordFlag != null && lastRecordFlag == 1 && fileType != "SIG") Map("lastRecord" -> true) else Nil) ++
            (if (nrBeamMap != null && nrBeamMap.size > 0) nrBeamMap else Nil) ++
            (if (nr5GTputKpis != null && nr5GTputKpis.size > 0) nr5GTputKpis else Nil) ++
            (if (nr5GScgBlerKpis != null && nr5GScgBlerKpis.size > 0) nr5GScgBlerKpis else Nil) ++
            (if (nr5gMacUlKPIsMap != null && nr5gMacUlKPIsMap.size > 0) nr5gMacUlKPIsMap else Nil) ++
            (if (nrScgStateKPIMap != null && nrScgStateKPIMap.size > 0) nrScgStateKPIMap else Nil) ++
            (if(reTxBlerKPIMap != null && reTxBlerKPIMap.size > 0) reTxBlerKPIMap else Nil) ++
            (if (NrPCIs0xB887Map != null && NrPCIs0xB887Map.size > 0) NrPCIs0xB887Map else Nil) ++
            (if (NRMIMOType_0xB887 != null && NRMIMOType_0xB887.nonEmpty) Map("NRMIMOType" -> NRMIMOType_0xB887) else Nil) ++
            // (if(nr0xB8A7Map != null && nr0xB8A7Map.size > 0) nr0xB8A7Map else Nil) ++
            (if (rm0xF0E4Map != null && rm0xF0E4Map.nonEmpty) rm0xF0E4Map else Nil) ++
            //  (if(CDRX0xB890Map != null && CDRX0xB890Map.size > 0) CDRX0xB890Map else Nil) ++
            (if (rtpdlsnPacketLoss != null) {
              if (rtpdlsnPacketLoss == 0) {
                Map("RtpRxPacketLossCount" -> rtpdlsnPacketLoss.toInt)
              } else if (rtpPacketLoss_3Seconds != null && rtpPacketLoss_3Seconds < Constants.MaxRTP_PacketLossVal) Map("RtpRxPacketLossCount" -> rtpPacketLoss_3Seconds.toInt) else Nil
            } else Nil) ++
            (if (rtpGapEvt != null) {
              if (VoLTETriggerEvtRtpGap == 1 || VoLTECallEndEvtRtpGap == 1) {
                if (rtpGapEvt == "false") Map("RtpRxPacketGap" -> 0) else Nil
              } else if (rtpGapEvt == "true") Map("RtpRxPacketGap" -> 1) else Nil
            } else Nil) ++
            (if (VoLTECallStart != null) Map("VoLTECallStartEvt" -> VoLTECallStart.toInt) else Nil) ++
            (if (VoLTECallEnd != null) Map("VoLTECallEndEvt" -> VoLTECallEnd.toInt) else Nil) ++
            (if (nrScgCfgEvt != null) Map("nrScgCfgEvt" -> nrScgCfgEvt) else Nil) ++
            (if (nrbeamFailureLag != null && rachResult0xb88a != null) {
              if (rachResult0xb88a == "SUCCESS") {
                Map("NRBeamRecoverySuccessEvt" -> 1)
              } else if (rachResult0xb88a == "ABORTED") Map("NRBeamRecoveryFailureEvt" -> 1) else Nil
            } else Nil) ++
            (if (rtpGapIncomingDuration >= 0.5) Map("VoLTEIncomingGapDuration" -> rtpGapIncomingDuration) else Nil) ++
            (if (rtpGapOutgoingDuration >= 0.5) Map("VoLTEOutgoingGapDuration" -> rtpGapOutgoingDuration) else Nil) ++
            (if (rtpGapIncomingDuration >= 0.5) Map("VoLTEIncomingGapType" -> getRtpGapType(rtpGapIncomingDuration)) else Nil) ++
            (if (rtpGapOutgoingDuration >= 0.5) Map("VoLTEOutgoingGapType" -> getRtpGapType(rtpGapOutgoingDuration)) else Nil) ++

            (if (VoLTERtpRxDeltaDelayInc != null) Map("VoLTERtpRxDeltaDelay" -> VoLTERtpRxDeltaDelayInc.toFloat) else Nil) ++
            (if (VoLTERtpRxDeltaDelayOut != null) Map("VoLTERtpRxDeltaDelayOutgoing" -> VoLTERtpRxDeltaDelayOut.toFloat) else Nil) ++
            (if (VoLTERtpRxDelayInc != null) Map("VoLTERtpRxDelay" -> VoLTERtpRxDelayInc.toFloat) else Nil) ++
            (if (VoLTERtpRxDelayOut != null) Map("VoLTERtpRxDelayOutgoing" -> VoLTERtpRxDelayOut.toFloat) else Nil) ++
            (if (ipSubtitle != null) {
              if (ipSubtitle == "INVITE") {
                Map("LTEVoLTETrigger" -> 1)
              } else Nil
            } else Nil) ++ // subtitle is "INVITE" per call id

            (if (VoLTESubtitleLast25Secs != null && VoLTESeqLast25Secs != null) {

              if (volteCallIneffective) {

                Map("LTECallInefAtmpt" -> 1)
              } else Nil
            } else Nil) ++

            (if (sPcellKpiMap != null) sPcellKpiMap else Nil) ++
            (if (servingBeamKpis0xB97f != null) servingBeamKpis0xB97f else Nil) ++
            (if (bandType0xB97F != null && validServingIndex)  Map("NRBandType" -> bandType0xB97F) else Nil) ++
            (if (nrPccArfcn0xB97F != null && validServingIndex) Map("NRPccARFCN" -> nrPccArfcn0xB97F) else Nil) ++
            (if (nrScc1Arfcn0xB97F != null) Map("NRScc1ARFCN" -> nrScc1Arfcn0xB97F) else Nil) ++
            (if (bandType0xB97F != null && validServingIndex) {
              (if(bandType0xB97F.split(" ")(1) == "(FR1)") {
                (if(bandType0xB97F.split(" ")(0) == "C-Band") {
                  var bandtype:String = null
                  if(Scc1BandType != null)  bandtype = "5G_C-Band " + Scc1BandType
                  (if(bandtype != null)  Map("NRConnectivityRat" -> bandtype) else Map("NRConnectivityRat" -> "5G_C-Band"))
                } else if(bandType0xB97F.split(" ")(0) == "DSS") {
                  var bandtype:String = null
                  if(Scc1BandType != null)  bandtype = "5G_NW " + Scc1BandType
                  (if(bandtype != null)  Map("NRConnectivityRat" -> bandtype) else Map("NRConnectivityRat" -> "5G_NW"))
                } else Nil)
              } else if(bandType0xB97F.split(" ")(1) == "(FR2)") {
                Map("NRConnectivityRat" -> "5G_mmW")
              } else Nil)
            } else if (PCI != null) {
              Map("NRConnectivityRat" -> "LTE")
            } else if (fileType != "SIG") {
              Map("NRConnectivityRat" -> "NoService")
            } else Nil) ++

            (if (LTE_RRC_State != null && !LTE_RRC_State.isEmpty) {
              Map("LTERrcState" -> LTE_RRC_State)
            } else if (numofsamples_B193 > 5) {
              Map("LTERrcState" -> "Connected")
            } else if (numofsamples_B193 < 5 && numofsamples_B193 > 0) {
              Map("LTERrcState" -> "Idle Camped")
            } else Nil) ++
            (if(fileName != null && fileType == "DML_DLF")  {
              (if(area.nonEmpty) Map("MarketArea" -> area) else Nil)
            } else Nil) ++

            (if (nrConnectivityType != null) Map("NRConnectivityType" -> nrConnectivityType.head) else Nil) ++
            (if (nrConnectivityMode != null) Map("NRConnectivityMode" -> nrConnectivityMode) else Nil) ++
            (if (vz_regions != null) Map("VzwRegions" -> vz_regions) else Nil) ++
            (if (CountryCode != null) {
              (if (CountryCode == "244") {
                Map("Country" -> "USA")
              } else if (vz_regions != null) {
                Map("Country" -> vz_regions)
              } else Nil)
            } else Nil) ++
            (if (stusps != null) Map("StateCode" -> stusps) else Nil) ++
            (if (countyns != null) Map("County" -> countyns) else Nil) ++

            (if (mx != 0 && my != 0) getLocValues(mx, my, x, y) else Nil) ++
            (if (mx != 0 && my != 0) Map("Outlier" -> 0) else Nil) ++
            //(if (mx != 0 && my != 0 && Constants.CURRENT_ENVIRONMENT=="DEV") getStateCountyRegion(mx, my, spark) else Nil) ++
            (if (ulFreq != null) Map("LTEEarfcnUL" -> ulFreq) else Nil) ++
            //(if (ulFreq != null && caIndex != null) { if(caIndex == "Scc1") Map("LTE"+caIndex+"EarfcnUL" -> ulFreq) else Nil} else Nil) ++
            (if (dlFreq != null) Map("LTEPccEarfcnDL" -> dlFreq) else Nil) ++
            (if (lteeNBId != null) Map("LTEeNBId" -> lteeNBId) else Nil) ++
            (if (LteCellID != 0) Map("LTECellId" -> LteCellID) else Nil) ++
            (if (cellId0xB0C0 != null) Map("CellIdentity" -> cellId0xB0C0) else Nil) ++
            (if (trackingAreaCode != null) Map("LTETaId" -> trackingAreaCode) else Nil) ++
            (if (DLPathLoss0xB16E != 0) Map("LTEPathLossDL" -> DLPathLoss0xB16E) else if (DLPathLoss0xB16F != 0) Map("LTEPathLossDL" -> DLPathLoss0xB16F) else Nil) ++
            (if (CDMAChannel != null) Map("CDMACh" -> CDMAChannel) else Nil) ++
            (if (pilotPN != null) Map("EvDOPilotPN" -> pilotPN) else Nil) ++
            (if (WCDMA_cellID != null) Map("WCDMACellId" -> WCDMA_cellID) else Nil) ++
            (if (WCDMA_RAC_Id != null) Map("WCDMARacId" -> WCDMA_RAC_Id) else Nil) ++
            (if (WCDMA_LAC_Id != null) Map("WCDMALacId" -> WCDMA_LAC_Id) else Nil) ++
            (if (WCDMA_PSC != null) Map("WCDMAPsc" -> WCDMA_PSC) else Nil) ++
            (if (ulFreq != null) Map("WCDMAUlArfcn" -> ulFreq) else Nil) ++
            (if (dlFreq != null) Map("WCDMADLArfcn" -> dlFreq) else Nil) ++
            (if (GSM_MsMaxTxPwr != null) Map("GSMMsMaxTxPwr" -> GSM_MsMaxTxPwr.toFloat) else Nil) ++
            (if (GSM_MsMinRxLev != null) Map("GSMMsMinRxLev" -> GSM_MsMinRxLev.toFloat) else Nil) ++
            (if (GSM_BSIC_BCC != null) Map("BSICBcc" -> GSM_BSIC_BCC) else Nil) ++
            (if (GSM_BSIC_NCC != null) Map("BSICNcc" -> GSM_BSIC_NCC) else Nil) ++
            // Setting inbuilding values
            (if (inbldngAccuracy != null && !inbldngAccuracy.isEmpty && isValidNumeric(inbldngAccuracy, Constants.CONST_DOUBLE)) Map("Accuracy" -> inbldngAccuracy.toDouble) else Nil) ++
            (if (inbldngAltitude != null && !inbldngAltitude.isEmpty && isValidNumeric(inbldngAltitude, Constants.CONST_DOUBLE)) Map("Altitude" -> inbldngAltitude.toDouble) else Nil) ++
            (if (inbldngImageId != 0) Map("InbuildingimageId" -> inbldngImageId) else Nil) ++
            (if (inbldngUserMark != null && !inbldngUserMark.isEmpty) Map("InbuildinguserMark" -> inbldngUserMark) else Nil) ++
            (if (inbldngX != 0) Map("Inbuildingx" -> inbldngX) else Nil) ++
            (if (inbldngY != 0) Map("Inbuildingy" -> inbldngY) else Nil) ++

            //    (if (WiFi_RSSI != null && !WiFi_RSSI.isEmpty && isValidNumeric(WiFi_RSSI, Constants.CONST_FLOAT)) Map("WiFiRssi" -> WiFi_RSSI.toFloat) else Nil) ++
            //(if (timingAdvance != 1) Map("LTEMacRachTA" -> timingAdvance) else Nil) ++
            (if (timingAdvance != null) { if(timingAdvance != 1) Map("LTEMacRachTA" -> timingAdvance) else Nil } else Nil) ++
            (if (PUCCH_Tx_Power0xB16F != -128 && fileType != "SIG") Map("LTEPucchActTxPwr" -> PUCCH_Tx_Power0xB16F) else Nil) ++
            (if (ulBandwidth != null && ulBandwidth.indexOf(" ") > 0) Map("LTEBwUL" -> ulBandwidth.split(" ")(0).toFloat) else Nil) ++
            (if (dlBandwidth != null && dlBandwidth.indexOf(" ") > 0) Map("LTEPccBwDL" -> dlBandwidth.split(" ")(0).toFloat) else Nil) ++
            //if (dlBandwidth != null && dlBandwidth.indexOf(" ") > 0 && caIndex != null) { if(caIndex == "Pcc") Map("LTE"+caIndex+"BwDL" -> dlBandwidth.split(" ")(0).toFloat) else Nil } else Nil) ++
            //(if (dlBandwidth != null && dlBandwidth.indexOf(" ") > 0 && caIndex != null) { if(caIndex != "Pcc" && caIndex != "Scc1") Map("LTE"+caIndex+"BwDL" -> dlBandwidth.split(" ")(0).toInt) else Nil} else Nil) ++
            //(if (dlBandwidth != null && dlBandwidth.indexOf(" ") > 0 && caIndex != null) { if(caIndex == "Scc1") Map("LTE"+caIndex+"BwDl" -> dlBandwidth.split(" ")(0).toInt) else Nil} else Nil) ++
            (if (lacId != 0) Map("GSMLacId" -> lacId) else Nil) ++
            (if (plmnId != null && !plmnId.isEmpty) Map("GsmPlmnId" -> plmnId) else Nil) ++
            (if (mccMnc != null && !mccMnc.contains("151515/1515") && fileType != "SIG") Map("LTEMccMncId" -> mccMnc) else Nil) ++
            (if (CDMA_PilotPN != null) Map("CDMAPilotPN" -> CDMA_PilotPN) else Nil) ++
            (if (upperLayerIndication_r15 != null && upperLayerIndication_r15 != "") Map("upperLayerIndication_r15" -> upperLayerIndication_r15) else Nil) ++
            (if (lteRrcConRestRej != null) Map("LTERrcConRestRej" -> lteRrcConRestRej) else Nil) ++
            (if (lteRrcConRej != null) Map("LTERrcConRej" -> lteRrcConRej) else Nil) ++
            (if (lteRrcConRelCount != null) Map("lteRrcConRelCnt" -> lteRrcConRelCount.toInt) else Nil) ++
            (if (lteRrcConReqCount != null) Map("lteRrcConReqCnt" -> lteRrcConReqCount.toInt) else Nil) ++
            (if (lteRrcConSetupCount != null) Map("lteRrcConSetupCnt" -> lteRrcConSetupCount.toInt) else Nil) ++
            (if (lteRrcConSetupCmpCount != null) Map("lteRrcConSetupCmpCnt" -> lteRrcConSetupCmpCount.toInt) else Nil) ++
            //After LTE RRC ConnectionSetup evt if there isn't RRC ConnectionSetup Complete in 1 sec then declare as Setup Failure
            (if (lteRrcConSetup != null) { if(lteRrcConSetup ==1 && lteConnSetupFailure) Map("lteRrcConSetupFailure" -> 1) else Nil } else Nil) ++
            (if (lteSecModeCmdCount != null) Map("lteSecModeCmdCnt" -> lteSecModeCmdCount.toInt) else Nil) ++
            (if (lteSecModeCmpCount != null) Map("lteSecModeCmpCnt" -> lteSecModeCmpCount.toInt) else Nil) ++
            (if (lteRrcConReconfigCount != null) Map("lteRrcConReconfigCnt" -> lteRrcConReconfigCount.toInt) else Nil) ++
            (if (lteRrcConReconfCmpCount != null) Map("lteRrcConReconfCmpCnt" -> lteRrcConReconfCmpCount.toInt) else Nil) ++
            (if (lteUeCapEnqCount != null) Map("lteUeCapEnqCnt" -> lteUeCapEnqCount.toInt) else Nil) ++
            (if (lteUeCapInfoCount != null) Map("lteUeCapInfoCnt" -> lteUeCapInfoCount.toInt) else Nil) ++
            (if (nrRrcReConfigCount != null) Map("nrRrcReConfigCnt" -> nrRrcReConfigCount.toInt) else Nil) ++
            (if (nrRbConfigCount != null) Map("nrRbConfigCnt" -> nrRbConfigCount.toInt) else Nil) ++
            (if (nrRrcReConfigCmpCount != null) Map("nrRrcReConfigCmpCnt" -> nrRrcReConfigCmpCount.toInt) else Nil) ++
            (if (lteHoCmdCount != null) Map("lteHoCmdCnt" -> lteHoCmdCount.toInt) else Nil) ++
            (if (lteRrcConRestReqCount != null) Map("lteRrcConRestReqCnt" -> lteRrcConRestReqCount.toInt) else Nil) ++
            (if (lteRrcConRestCmpCount != null) Map("LTERrcConRestCmpCnt" -> lteRrcConRestCmpCount.toInt) else Nil) ++
            (if (lteRrcConRestCount != null) Map("lteRrcConRestCnt" -> lteRrcConRestCount.toInt) else Nil) ++
            (if (nrState != null && !nrState.isEmpty) Map("NRSTATE" -> nrState) else Nil) ++
            (if (nrFailureType != null && !nrFailureType.isEmpty) Map("NRFAILURETYPE" -> nrFailureType) else Nil) ++
            (if (nrPrimaryPath != null ) Map("NRPrimaryPath" -> nrPrimaryPath) else Nil) ++
            (if (nrUlDataSplitThreshold != null ) Map("NRUlDataSplitThreshold" -> nrUlDataSplitThreshold) else Nil) ++
            (if (lteServiceRej != null) Map("LTEServiceRej" -> lteServiceRej) else Nil) ++
            (if (lteAttRej != null) Map("LTEAttRej" -> lteAttRej) else Nil) ++
            (if (ltePdnRej != null) Map("LTEPdnRej" -> ltePdnRej) else Nil) ++
            (if (lteAuthRej != null) Map("LTEAuthRej" -> lteAuthRej) else Nil) ++
            (if (lteTaRej != null) Map("LTETaRej" -> lteTaRej) else Nil) ++
            (if (lteActDefContAcptCount != null) Map("lteActDefContAcptCnt" -> lteActDefContAcptCount.toInt) else Nil) ++
            (if (lteDeActContAcptCount != null) Map("lteDeActContAcptCnt" -> lteDeActContAcptCount.toInt) else Nil) ++
            (if (lteEsmInfoResCount != null) Map("lteEsmInfoResCnt" -> lteEsmInfoResCount.toInt) else Nil) ++
            (if (ltePdnConntReqount != null) Map("ltePdnConntReqCnt" -> ltePdnConntReqount) else Nil) ++
            (if (ltePdnDisconReqCount != null) Map("ltePdnDisconReqCnt" -> ltePdnDisconReqCount.toInt) else Nil) ++
            (if (lteActDefBerContReqCount != null) Map("lteActDefBerContReqCnt" -> lteActDefBerContReqCount.toInt) else Nil) ++
            (if (lteDeActBerContReqCount != null) Map("lteDeActBerContReqCnt" -> lteDeActBerContReqCount.toInt) else Nil) ++
            (if (lteEsmInfoReqCount != null) Map("lteEsmInfoReqCnt" -> lteEsmInfoReqCount.toInt) else Nil) ++
            (if (lteAttCmpCount != null) Map("lteAttCmpCnt" -> lteAttCmpCount.toInt) else Nil) ++
            (if (lteAttReqCount != null) Map("lteAttReqCnt" -> lteAttReqCount.toInt) else Nil) ++
            (if (lteDetachReqCount != null) Map("lteDetachReqCnt" -> lteDetachReqCount.toInt) else Nil) ++
            (if (lteSrvReqCount != null) Map("lteSrvReqCnt" -> lteSrvReqCount.toInt) else Nil) ++
            (if (lteAttAcptCount != null) Map("lteAttAcptCnt" -> lteAttAcptCount.toInt) else Nil) ++
            (if (lteEmmInfoCount != null) Map("lteEmmInfoCnt" -> lteEmmInfoCount.toInt) else Nil) ++
            (if (lteTaReqCount != null) Map("lteTaReqCnt" -> lteTaReqCount.toInt) else Nil) ++
            (if (lteTaAcptCount != null) Map("lteTaAcptCnt" -> lteTaAcptCount.toInt) else Nil) ++
            (if (lteEsmModifyEpsReqCount != null) Map("lteBearerModReqCnt" -> lteEsmModifyEpsReqCount.toInt) else Nil) ++
            (if (lteEsmModifyEpsRejCount != null) Map("lteBearerModRejCnt" -> lteEsmModifyEpsRejCount.toInt) else Nil) ++
            (if (lteEsmModifyEpsAccptCount != null) Map("lteBearerModAcptCnt" -> lteEsmModifyEpsAccptCount.toInt) else Nil) ++
            (if (lteActDefContRejCount != null) Map("lteActDefContRejCnt" -> lteActDefContRejCount.toInt) else Nil) ++
            (if (lteActDedContRejCount != null) Map("lteActDedContRejCnt" -> lteActDedContRejCount.toInt) else Nil) ++
            (if (lteActDedBerContReqCount != null) Map("lteActDedBerContReqCnt" -> lteActDedBerContReqCount.toInt) else Nil) ++
            (if (lteActDedContAcptCount != null) Map("lteActDedContAcptCnt" -> lteActDedContAcptCount.toInt) else Nil) ++
            (if (cdmaRfTxPowerVal != null) Map("CDMATxPwr" -> cdmaRfTxPowerVal) else Nil) ++
            (if (CdmaTxStatusVal != null) Map("CDMARfTxStatus" -> CdmaTxStatusVal) else Nil) ++
            (if (NewCellCause != null && fileType != "SIG") Map("LTENewCellCause" -> NewCellCause) else Nil) ++
            (if ((MCC != null && !MCC.isEmpty) && (MNC != null && !MNC.isEmpty)) Map("WCDMAPlmnId" -> (MCC + "/" + MNC)) else Nil) ++
            (if (numOfLayers != null) Map("LTEDlNumOfLayers" -> numOfLayers) else Nil) ++
            (if (PucchGi != null) Map("LTEPucchGi" -> PucchGi) else Nil) ++
            (if (gsmArfcn != null) Map("GSMArfcn" -> gsmArfcn) else Nil) ++
            (if ((numTxAntennas != null && numTxAntennas.indexOf(" ") > 0) && (numRxAntennas != null && numRxAntennas.indexOf(" ") > 0)) Map("LTEMimoType" -> (numTxAntennas.split(" antennas")(0) + "x" + numRxAntennas.split(" antennas")(0))) else Nil) ++

            (if (macUIThroughput > 0) Map("LTEMacTputUl" -> macUIThroughput) else Nil) ++
            (if (macDlThroughput > 0) Map("LTEMacTputDl" -> macDlThroughput) else Nil) ++
            (if (EmmState != null && !EmmState.isEmpty) Map("LTEEmmState" -> EmmState) else Nil) ++
            (if (EmmSubState != null && !EmmSubState.isEmpty) Map("LTEEmmSubState" -> EmmSubState) else Nil) ++
            (if (pUSCHPccTPUT > 0) Map("LTEPccPuschThroughput" -> pUSCHPccTPUT) else Nil) ++ // LTE PHY PCC UL Throughput
            (if (pUSCHScc1TPUT > 0) Map("LTEScc1PuschThroughput" -> pUSCHScc1TPUT) else Nil) ++ // LTE PHY SCC1 UL Throughput
            (if (overallPUSCHTPUT > 0) Map("LTEPuschThroughput" -> overallPUSCHTPUT) else Nil) ++ //LTE PHY Overall UL Throughput
            (if (pdschBLER > 0) Map("LTEPdschBler" -> pdschBLER) else Nil) ++
            (if (numPRB0xB173 != null && (numPRB0xB173.size > 0)) Map("LTEPdschPrb" -> numPRB0xB173.head) else Nil) ++
          //  (if (distServingCellIndex_0xB173 != null && distServingCellIndex_0xB173 > 0) Map("LTENumDlCaCc" -> distServingCellIndex_0xB173) else Nil) ++
            (if (ServingIndexSize_0xB173>0) Map("LTENumDlCaCc" -> ServingIndexSize_0xB173) else Nil) ++ // serving cell index size is determined for first 3 samples in a second
            (if (PowerHeadRoom != null) Map("LTEPH" -> PowerHeadRoom) else Nil) ++
            (if (pucchAdjTxPwr != null) Map("PucchAdjTxPwr" -> pucchAdjTxPwr) else Nil) ++
            (if (pucchTargetMobPwr != null) Map("PucchTargetMobPwr" -> pucchTargetMobPwr) else Nil) ++
            //LTE PCC Throughput DL calculated at ES
            /*           (if (pDSCHpCellTPUT > 0) Map("LTEPccTputDL" -> pDSCHpCellTPUT) else Nil) ++
                       (if (pDSCHsCell1TPUT > 0) Map("LTEScc1TputDl" -> pDSCHsCell1TPUT) else Nil) ++
                       (if (pDSCHsCell2TPUT > 0) Map("LTEScc2TputDl" -> pDSCHsCell2TPUT) else Nil) ++
                       (if (pDSCHsCell3TPUT > 0) Map("LTEScc3TputDl" -> pDSCHsCell3TPUT) else Nil) ++
                       (if (pDSCHsCell4TPUT > 0) Map("LTEScc4TputDl" -> pDSCHsCell4TPUT) else Nil) ++
                       (if (pDSCHsCell5TPUT > 0) Map("LTEScc5TputDl" -> pDSCHsCell5TPUT) else Nil) ++*/
            (if (pDSCHpccModType != null) Map("LTEPccDlMcs" -> pDSCHpccModType) else Nil) ++
            (if (pDSCHscc1ModType != null) Map("LTEScc1DlMcs" -> pDSCHscc1ModType) else Nil) ++
            (if (pDSCHscc2ModType != null) Map("LTEScc2DlMcs" -> pDSCHscc2ModType) else Nil) ++
            (if (pDSCHscc3ModType != null) Map("LTEScc3DlMcs" -> pDSCHscc3ModType) else Nil) ++
            (if (pDSCHscc4ModType != null) Map("LTEScc4DlMcs" -> pDSCHscc4ModType) else Nil) ++
            (if (pDSCHscc5ModType != null) Map("LTEScc5DlMcs" -> pDSCHscc5ModType) else Nil) ++
            (if (dlRlcTrupt > 0) Map("LTERlcTputDl" -> dlRlcTrupt) else Nil) ++
            (if (ulRlcTrupt > 0) Map("LTERlcTputUl" -> ulRlcTrupt) else Nil) ++
            (if (ulPdcpTrupt > 0) Map("LTEPdcpTputUl" -> ulPdcpTrupt) else Nil) ++
            (if (dlPdcpTrupt > 0) Map("LTEPdcpTputDl" -> dlPdcpTrupt) else Nil) ++
            (if (VoLTE_Jitter != null && !VoLTE_Jitter.isEmpty && isValidNumeric(VoLTE_Jitter, Constants.CONST_FLOAT)) Map("VoLTERtpRxJitt" -> VoLTE_Jitter.toFloat) else Nil) ++
            // (if (VoLTE_RtpLoss != null && !VoLTE_RtpLoss.isEmpty && isValidNumeric(VoLTE_RtpLoss, Constants.CONST_FLOAT)) Map("VoLTERtpRxPacketLoss" -> VoLTE_RtpLoss.toFloat) else Nil) ++
            (if (VoLTE_Vvq != null && !VoLTE_Vvq.isEmpty && isValidNumeric(VoLTE_Vvq, Constants.CONST_FLOAT)) Map("VoLTEVolteVVQ" -> VoLTE_Vvq.toFloat) else Nil) ++
            (if (LTENormalServiceCount != null) Map("LTENormalSerCount" -> LTENormalServiceCount) else Nil) ++
            (if (WCDMANormalServiceCount != null) Map("WCDMANormalSerCount" -> WCDMANormalServiceCount) else Nil) ++
            (if (GSMNormalServiceCount != null) Map("GSMNormalSerCount" -> GSMNormalServiceCount) else Nil) ++
            (if (CDMANormalServiceCount != null) Map("CDMANormalSerCount" -> CDMANormalServiceCount) else Nil) ++
            (if (NoServiceCount != null) Map("NoSerCount" -> NoServiceCount) else Nil) ++
            (if (LTE_SSAC_Present != null && fileType != "SIG") Map("LTESsacStatus" -> LTE_SSAC_Present) else Nil) ++
            (if (Qci != null && !Qci.isEmpty && isValidNumeric(Qci, Constants.CONST_NUMBER)) Map("LTEQci" -> Qci.toInt) else Nil) ++
            (if (SRS_Tx_Power != null) Map("LTESrsActualTx" -> SRS_Tx_Power.toFloat) else Nil) ++

            (if (cdma_Sid_0x184E != null) Map("CDMASid" -> cdma_Sid_0x184E) else Nil) ++
            (if (cdma_Nid_x184E != null) Map("CDMANid" -> cdma_Nid_x184E) else Nil) ++
            (if (Cdma_EcIo_0x184E != null) Map("CDMAEcIo" -> Cdma_EcIo_0x184E.toFloat) else Nil) ++
            (if (band_class_0x184E != null) Map("CDMABandClassStr" -> band_class_0x184E) else Nil) ++
            (if (evDo_ECIO_0x184E != null) Map("EvDOEcIo" -> evDo_ECIO_0x184E) else Nil) ++
            (if (evDo_SINR_0x184E != null) Map("EvDOSinr" -> evDo_SINR_0x184E.toFloat) else Nil) ++
            (if (wcdma_Rssi != null) Map("WCDMARssi" -> wcdma_Rssi) else Nil) ++
            (if (wcdma_Rscp != null) Map("WCDMARscp" -> wcdma_Rscp.toFloat) else Nil) ++
            (if (wcdma_Ecio != null) Map("WCDMAEcNo" -> wcdma_Ecio) else Nil) ++
            (if (lteHandoverSuccessCount != null) Map("LTEHoCmpCnt" -> lteHandoverSuccessCount) else Nil) ++
            (if (nrdlbler != null && isValidNumeric(nrdlbler, Constants.CONST_DOUBLE) && nrdlbler != "NaN") Map("NRDLBLER" -> nrdlbler.toFloat) else Nil) ++
            (if (nrresidualbler != null && isValidNumeric(nrresidualbler, Constants.CONST_FLOAT) && nrresidualbler != "NaN") Map("NRRESIDUALBLER" -> nrresidualbler.toFloat) else Nil) ++
            (if (nrdlmodtype != null) Map("NRMODDL" -> nrdlmodtype(0)) else Nil) ++
            (if (nrdlmodtype != null) Map("NRDLMCS" -> getModulationType(nrdlmodtype(0))) else Nil) ++
            (if (nrsdlfrequency_B975 != null) Map("NRSDLFREQUENCY" -> nrsdlfrequency_B975) else if (nrSpCellDlFreq != null) Map("NRSDLFREQUENCY" -> nrSpCellDlFreq) else Nil) ++
            (if (nrband0xB97F != null && validServingIndex) {
              Map("NRBand" -> nrband0xB97F)
            }  else Nil) ++
            (if (nrbandScc10xB97F != null) Map("NRScc1Band" -> nrbandScc10xB97F) else Nil) ++
            (if (nrSpCellDuplexMode != null) Map("NRDuplexMode" -> nrSpCellDuplexMode) else Nil) ++
            (if (nrSpCellDlBw != null) Map("NRSDLBANDWIDTH" -> nrSpCellDlBw) else Nil) ++
            (if (nrSpCellUlBw != null) Map("NRULBANDWIDTH" -> nrSpCellUlBw) else Nil) ++
            (if (nrSpCellUlFreq != null) Map("NRSULFREQUENCY" -> nrSpCellUlFreq) else Nil) ++
            (if (NrPdschTputVal != null) Map("NR5GPdschTput" -> NrPdschTputVal) else Nil) ++
            (if (NRServingBeamIndexVal != null) Map("NRServingBeamIndex" -> NRServingBeamIndexVal) else Nil) ++
            //            (if (PUSCHTxPwr_0xB884 != null) Map("NRPUSCHTxPwr" -> PUSCHTxPwr_0xB884) else Nil) ++
            (if (PUSCHTxPwr_0xB8D2 != null) Map("NRPUSCHTxPwr" -> PUSCHTxPwr_0xB8D2) else Nil) ++
            (if (nrSetupAttempt != null) {
              if (nrSetupAttempt == "1") {
                Map("NrSetupAttempt" -> 1) ++
                  (if (bandType0xB97F != null) {
                    (if (bandType0xB97F.split(" ")(0) == "C-Band") {
                      Map("NrSetupAttemptCband" -> 1)
                    } else if (bandType0xB97F.split(" ")(0) == "DSS") {
                      Map("NrSetupAttemptNW" -> 1)
                    } else Nil)
                  } else Nil)
              } else Nil
            } else Nil) ++
            (if (nrSetupSuccess != null) {
              if (nrSetupSuccess == "1") {
                Map("NrSetupSuccess" -> 1) ++
                  (if (bandType0xB97F != null) {
                    (if (bandType0xB97F.split(" ")(0) == "C-Band") {
                      Map("NrSetupSuccessCband" -> 1)
                    } else if (bandType0xB97F.split(" ")(0) == "DSS") {
                      Map("NrSetupSuccessNW" -> 1)
                    } else Nil)
                  } else Nil)
              } else Nil
            } else Nil) ++
            (if (NrReleaseEvt != null) {
              if (NrReleaseEvt == 1) {
                Map("NrReleaseEvt" -> 1)
              } else Nil
            } else Nil) ++
            (if (NrSetupFailure) {
              (if (bandType0xB97F != null) {
                (if (bandType0xB97F.split(" ")(0) == "C-Band") {
                  Map("NrSetupFailureCband" -> 1)
                } else if (bandType0xB97F.split(" ")(0) == "DSS") {
                  Map("NrSetupFailureNW" -> 1)
                } else Nil)
              } else Nil)
            } else Nil) ++
            (if (NrSetupFailure) Map("NrSetupFailure" -> 1) else Nil) ++
            (if (NrSetupSuccessCnt != null) {
              Map("NrSetupSuccessCnt" -> NrSetupSuccessCnt.toInt)
            } else Nil) ++
            (if (NrSetupFailureCnt != null) {
              Map("NrSetupFailureCnt" -> NrSetupFailureCnt.toInt)
            } else Nil) ++
            (if (NrSetupAttemptCnt != null) {
              Map("NrSetupAttemptCnt" -> NrSetupAttemptCnt.toInt)
            } else Nil) ++
            (if (NrReleaseCnt != null) {
              Map("NrReleaseCnt" -> NrReleaseCnt.toInt)
            } else Nil) ++

            (if (!row.isNullAt(row.fieldIndex("NRRlfEvt"))) {
              Map("NRRlf" -> row.getInt(row.fieldIndex("NRRlfEvt"))) ++
                (if (bandType0xB97F != null) {
                  (if (bandType0xB97F.split(" ")(0) == "C-Band") {
                    Map("NRRlfCband" -> row.getInt(row.fieldIndex("NRRlfEvt")))
                  } else if (bandType0xB97F.split(" ")(0) == "DSS") {
                    Map("NRRlfNW" -> row.getInt(row.fieldIndex("NRRlfEvt")))
                  } else Nil)
                } else Nil)
            } else Nil) ++

            (if (!row.isNullAt(row.fieldIndex("NRBeamFailureEvt"))) {
              Map("NRBeamFailureEvt" -> row.getInt(row.fieldIndex("NRBeamFailureEvt"))) ++
                (if (bandType0xB97F != null) {
                  (if (bandType0xB97F.split(" ")(0) == "C-Band") {
                    Map("NRBeamFailureEvtCband" -> row.getInt(row.fieldIndex("NRBeamFailureEvt")))
                  } else if (bandType0xB97F.split(" ")(0) == "DSS") {
                    Map("NRBeamFailureEvtNW" -> row.getInt(row.fieldIndex("NRBeamFailureEvt")))
                  } else Nil)
                } else Nil)
            } else Nil) ++
            (if(timestamp != null) {
              //added seqid at every 30th to sample documents in ES using match query (DMAT kpi menu API use case)
              val timestampString = timestamp.replace(" ", "T")
              val currentTime = ZonedDateTime.parse(timestampString + 'Z')
              var currentTimeInepoch = currentTime.toEpochSecond
              (if(currentTimeInepoch % 30 ==0) { Map("seqid" -> 1) } else Nil)
            } else Nil) ++
            //DL TPUT KPIS for 5g log file
            (if (NrDlScg5MsTput != null && NrDlScg5MsTput > 0 && isValidNumeric(NrDlScg5MsTput.toString, Constants.CONST_FLOAT)) Map("NrDlScg5MsTput" -> round(NrDlScg5MsTput, 2).toFloat) else Nil) ++
            (if (NrDlScg1SecTput != null && NrDlScg1SecTput > 0 && isValidNumeric(NrDlScg1SecTput.toString, Constants.CONST_FLOAT)) Map("NrDlScg1SecTput" -> round(NrDlScg1SecTput, 2).toFloat) else Nil) ++
            (if (NrDlScg1SecPccTput != null && NrDlScg1SecPccTput > 0 && isValidNumeric(NrDlScg1SecPccTput.toString, Constants.CONST_FLOAT)) Map("NrDlScg1SecPccTput" -> round(NrDlScg1SecPccTput, 2).toFloat) else Nil) ++
            (if (NrDlScg1SecScc1Tput != null && NrDlScg1SecScc1Tput > 0 && isValidNumeric(NrDlScg1SecScc1Tput.toString, Constants.CONST_FLOAT)) Map("NrDlScg1SecScc1Tput" -> round(NrDlScg1SecScc1Tput, 2).toFloat) else Nil) ++
            (if (NrDlScg1SecScc2Tput != null && NrDlScg1SecScc2Tput > 0 && isValidNumeric(NrDlScg1SecScc2Tput.toString, Constants.CONST_FLOAT)) Map("NrDlScg1SecScc2Tput" -> round(NrDlScg1SecScc2Tput, 2).toFloat) else Nil) ++
            (if (NrDlScg1SecScc3Tput != null && NrDlScg1SecScc3Tput > 0 && isValidNumeric(NrDlScg1SecScc3Tput.toString, Constants.CONST_FLOAT)) Map("NrDlScg1SecScc3Tput" -> round(NrDlScg1SecScc3Tput, 2).toFloat) else Nil) ++
            (if (NrDlScg1SecScc4Tput != null && NrDlScg1SecScc4Tput > 0 && isValidNumeric(NrDlScg1SecScc4Tput.toString, Constants.CONST_FLOAT)) Map("NrDlScg1SecScc4Tput" -> round(NrDlScg1SecScc4Tput, 2).toFloat) else Nil) ++
            (if (NrDlScg1SecScc5Tput != null && NrDlScg1SecScc5Tput > 0 && isValidNumeric(NrDlScg1SecScc5Tput.toString, Constants.CONST_FLOAT)) Map("NrDlScg1SecScc5Tput" -> round(NrDlScg1SecScc5Tput, 2).toFloat) else Nil) ++
            (if (NrDlScg1SecScc6Tput != null && NrDlScg1SecScc6Tput > 0 && isValidNumeric(NrDlScg1SecScc6Tput.toString, Constants.CONST_FLOAT)) Map("NrDlScg1SecScc6Tput" -> round(NrDlScg1SecScc6Tput, 2).toFloat) else Nil) ++
            (if (NrDlScg1SecScc7Tput != null && NrDlScg1SecScc7Tput > 0 && isValidNumeric(NrDlScg1SecScc7Tput.toString, Constants.CONST_FLOAT)) Map("NrDlScg1SecScc7Tput" -> round(NrDlScg1SecScc7Tput, 2).toFloat) else Nil) ++

            (if (carrierIDCount != null && !carrierIDCount.isEmpty) Map("NR5GPdschCarrierCount" -> carrierIDCount.split(",").length) else Nil) ++
            //LTE PHYSICAL THROUGHPUT KPIs calculated at hive
            (if (DlL1PccTput != null && DlL1PccTput > 0 && isValidNumeric(DlL1PccTput.toString, Constants.CONST_FLOAT)) Map("LTEPccTputDL" -> round(DlL1PccTput, 2).toFloat) else Nil) ++
            (if (DlL1Scc1Tput != null && DlL1Scc1Tput > 0 && isValidNumeric(DlL1Scc1Tput.toString, Constants.CONST_FLOAT)) Map("LTEScc1TputDl" -> round(DlL1Scc1Tput, 2).toFloat) else Nil) ++
            (if (DlL1Scc2Tput != null && DlL1Scc2Tput > 0 && isValidNumeric(DlL1Scc2Tput.toString, Constants.CONST_FLOAT)) Map("LTEScc2TputDl" -> round(DlL1Scc2Tput, 2).toFloat) else Nil) ++
            (if (DlL1Scc3Tput != null && DlL1Scc3Tput > 0 && isValidNumeric(DlL1Scc3Tput.toString, Constants.CONST_FLOAT)) Map("LTEScc3TputDl" -> round(DlL1Scc3Tput, 2).toFloat) else Nil) ++
            (if (DlL1Scc4Tput != null && DlL1Scc4Tput > 0 && isValidNumeric(DlL1Scc4Tput.toString, Constants.CONST_FLOAT)) Map("LTEScc4TputDl" -> round(DlL1Scc4Tput, 2).toFloat) else Nil) ++
            (if (DlL1Scc5Tput != null && DlL1Scc5Tput > 0 && isValidNumeric(DlL1Scc5Tput.toString, Constants.CONST_FLOAT)) Map("LTEScc5TputDl" -> round(DlL1Scc5Tput, 2).toFloat) else Nil) ++
            (if (DlL1Scc6Tput != null && DlL1Scc6Tput > 0 && isValidNumeric(DlL1Scc6Tput.toString, Constants.CONST_FLOAT)) Map("LTEScc6TputDl" -> round(DlL1Scc6Tput, 2).toFloat) else Nil) ++
            (if (DlL1Scc7Tput != null && DlL1Scc7Tput > 0 && isValidNumeric(DlL1Scc7Tput.toString, Constants.CONST_FLOAT)) Map("LTEScc7TputDl" -> round(DlL1Scc7Tput, 2).toFloat) else Nil) ++
            (if (overallDlL1Tput != null && overallDlL1Tput > 0 && isValidNumeric(overallDlL1Tput.toString, Constants.CONST_FLOAT)) Map("LTEPdschTput" -> round(overallDlL1Tput,2).toFloat) else Nil) ++ // LTE PHY Overall DL Throughput


//            (if (pccCarrierIdCount != null && !pccCarrierIdCount.isEmpty && isValidNumeric(pccCarrierIdCount.toString, Constants.CONST_NUMBER) && numerologyForAvgRb0xB887 != null) Map("NrPccDlRb" -> getAvgRbUtilization(pccCarrierIdCount.toInt, numerologyForAvgRb0xB887).toFloat) else Nil) ++
//            (if (scc1CarrierIdCount != null && !scc1CarrierIdCount.isEmpty && isValidNumeric(scc1CarrierIdCount.toString, Constants.CONST_NUMBER) && numerologyForAvgRb0xB887 != null) Map("NrScc1DlRb" -> getAvgRbUtilization(scc1CarrierIdCount.toInt, numerologyForAvgRb0xB887).toFloat) else Nil) ++
//            (if (scc2CarrierIdCount != null && !scc2CarrierIdCount.isEmpty && isValidNumeric(scc2CarrierIdCount.toString, Constants.CONST_NUMBER) && numerologyForAvgRb0xB887 != null) Map("NrScc2DlRb" -> getAvgRbUtilization(scc2CarrierIdCount.toInt, numerologyForAvgRb0xB887).toFloat) else Nil) ++
//            (if (scc3CarrierIdCount != null && !scc3CarrierIdCount.isEmpty && isValidNumeric(scc3CarrierIdCount.toString, Constants.CONST_NUMBER) && numerologyForAvgRb0xB887 != null) Map("NrScc3DlRb" -> getAvgRbUtilization(scc3CarrierIdCount.toInt, numerologyForAvgRb0xB887).toFloat) else Nil) ++
//            (if (scc4CarrierIdCount != null && !scc4CarrierIdCount.isEmpty && isValidNumeric(scc4CarrierIdCount.toString, Constants.CONST_NUMBER) && numerologyForAvgRb0xB887 != null) Map("NrScc4DlRb" -> getAvgRbUtilization(scc4CarrierIdCount.toInt, numerologyForAvgRb0xB887).toFloat) else Nil) ++
//            (if (scc5CarrierIdCount != null && !scc5CarrierIdCount.isEmpty && isValidNumeric(scc5CarrierIdCount.toString, Constants.CONST_NUMBER) && numerologyForAvgRb0xB887 != null) Map("NrScc5DlRb" -> getAvgRbUtilization(scc5CarrierIdCount.toInt, numerologyForAvgRb0xB887).toFloat) else Nil) ++
//            (if (scc6CarrierIdCount != null && !scc6CarrierIdCount.isEmpty && isValidNumeric(scc6CarrierIdCount.toString, Constants.CONST_NUMBER) && numerologyForAvgRb0xB887 != null) Map("NrScc6DlRb" -> getAvgRbUtilization(scc6CarrierIdCount.toInt, numerologyForAvgRb0xB887).toFloat) else Nil) ++
//            (if (scc7CarrierIdCount != null && !scc7CarrierIdCount.isEmpty && isValidNumeric(scc7CarrierIdCount.toString, Constants.CONST_NUMBER) && numerologyForAvgRb0xB887 != null) Map("NrScc7DlRb" -> getAvgRbUtilization(scc7CarrierIdCount.toInt, numerologyForAvgRb0xB887).toFloat) else Nil) ++

            (if (NRTxBeamIndexPCC != null) Map("NRTxBeamIndexPCC" -> NRTxBeamIndexPCC) else Nil) ++
            (if (NRTxBeamIndexSCC1 != null) Map("NRTxBeamIndexSCC1" -> NRTxBeamIndexSCC1) else Nil) ++
//            (if(PhychanBitMask_0xB884 != null) {
//
//              (if (phychanBitMaskVal.toUpperCase == "PUSCH," && PUSCHDLPathLoss_0xB884 != null) Map("NRDLPATHLOSS" -> PUSCHDLPathLoss_0xB884) else Nil)
//              (if (phychanBitMaskVal.toUpperCase == "PUCCH," && PUCCHDLPathLoss_0xB884 != null) Map("NRDLPATHLOSS" -> PUCCHDLPathLoss_0xB884) else Nil)
//              (if (phychanBitMaskVal.toUpperCase == "PRACH," && PRACPathLoss_0xB884 != null) Map("NRDLPATHLOSS" -> PRACPathLoss_0xB884) else Nil)
//              (if (phychanBitMaskVal.toUpperCase == "SRS," && SRSDLPathLoss_0xB884 != null) Map("NRDLPATHLOSS" -> PRACPathLoss_0xB884) else Nil)
//            } else Nil) ++
            (if (PUSCHDLPathLoss_0xB8D2 != null) Map("NRDLPATHLOSS" -> PUSCHDLPathLoss_0xB8D2) else Nil) ++
            (if (PUCCHDLPathLoss_0xB8D2 != null) Map("NRDLPATHLOSS" -> PUCCHDLPathLoss_0xB8D2) else Nil) ++
            (if (PRACPathLoss_0xB8D2 != null) Map("NRDLPATHLOSS" -> PRACPathLoss_0xB8D2) else Nil) ++
            (if (SRSDLPathLoss_0xB8D2 != null) Map("NRDLPATHLOSS" -> PRACPathLoss_0xB8D2) else Nil) ++
            (if (NRSNR_0xB8D8 != null && NRSNR_0xB8D8.size > 1) Map(s"NRSSCC${NRSNR_0xB8D8(0)}BSNR" -> parseDouble(NRSNR_0xB8D8(1))) else Nil) ++
            (if (NRSBSNR_0xB8DD != null && NRSBSNR_0xB8DD != 0.0) Map("NRSBSNR" -> NRSBSNR_0xB8DD) else Nil) ++
            (if (NRSBSNRRx0_0xB8DD != null && NRSBSNRRx0_0xB8DD != 0.0) Map("NRSBSNRRx0" -> NRSBSNRRx0_0xB8DD) else Nil) ++
            (if (NRSBSNRRx1_0xB8DD != null && NRSBSNRRx1_0xB8DD != 0.0) Map("NRSBSNRRx1" -> NRSBSNRRx1_0xB8DD) else Nil) ++
            (if (NRSBSNRRx2_0xB8DD != null && NRSBSNRRx2_0xB8DD != 0.0) Map("NRSBSNRRx2" -> NRSBSNRRx2_0xB8DD) else Nil) ++
            (if (NRSBSNRRx3_0xB8DD != null && NRSBSNRRx3_0xB8DD != 0.0) Map("NRSBSNRRx3" -> NRSBSNRRx3_0xB8DD) else Nil) ++
            (if (rrcCon5gDlSessionDataBytes != null) Map("NRSessionDlData" -> rrcCon5gDlSessionDataBytes) else Nil) ++
            //(if (SRS_Tx_Power != null && !SRS_Tx_Power.isEmpty && isValidNumeric(VoLTE_Vvq,Constants.CONST_FLOAT)) Map ("LTESrsActualTx" -> SRS_Tx_Power.toFloat) else Nil) ++
            (if (carrierIndex0xB126 != null && !modulation0.isEmpty) {
              if (caIndex0xB126 != null) Map("LTE" + caIndex0xB126 + "DlMod0" -> modulation0) else Nil
            } else Nil) ++
            (if (carrierIndex0xB126 != null && !modulation1.isEmpty) {
              if (caIndex0xB126 != null) Map("LTE" + caIndex0xB126 + "DlMod1" -> modulation1) else Nil
            } else Nil) ++
            (if (carrierIndex0xB126 != null && pmiIndex != null) {
              if (caIndex0xB126 != null) Map("LTE" + caIndex0xB126 + "Pmi" -> pmiIndex) else Nil
            } else Nil) ++
            (if (carrierIndex0xB126 != null && spatialRank != null) {
              if (caIndex0xB126 != null && spatialRank.indexOf(" ") > 0) Map("LTE" + caIndex0xB126 + "SpatialRank" -> spatialRank.split("rank ")(1).toInt) else Nil
            } else Nil) ++
            (if (countLogcodeB126 > 0) Map("LogcodeB126Cnt" -> countLogcodeB126) else Nil) ++
            (if (cqiCw0 != null) Map("LTEPccCqiCw0" -> cqiCw0) else Nil) ++
            (if (cqiCw1 != null) Map("LTEPCccqiCw1" -> cqiCw1) else Nil) ++
            (if (pccRi != null) Map("LTEPccRi" -> pccRi) else Nil) ++
            (if (scc1CqiCw0 != null) Map("LTEScc1CqiCw0" -> scc1CqiCw0) else Nil) ++
            (if (scc1CqiCw1 != null) Map("LTEScc1CqiCw1" -> scc1CqiCw1) else Nil) ++
            (if (scc1Ri != null) Map("LTEScc1Ri" -> scc1Ri) else Nil) ++
            (if (scc2CqiCw0 != null) Map("LTEScc2CqiCw0" -> scc2CqiCw0) else Nil) ++
            (if (scc2CqiCw1 != null) Map("LTEScc2CqiCw1" -> scc2CqiCw1) else Nil) ++
            (if (scc2Ri != null) Map("LTEScc2Ri" -> scc2Ri) else Nil) ++
            (if (scc3CqiCw0 != null) Map("LTEScc3CqiCw0" -> scc3CqiCw0) else Nil) ++
            (if (scc3CqiCw1 != null) Map("LTEScc3CqiCw1" -> scc3CqiCw1) else Nil) ++
            (if (scc3Ri != null) Map("LTEScc3Ri" -> scc3Ri) else Nil) ++
            (if (scc4CqiCw0 != null) Map("LTEScc4CqiCw0" -> scc4CqiCw0) else Nil) ++
            (if (scc4CqiCw1 != null) Map("LTEScc4CqiCw1" -> scc4CqiCw1) else Nil) ++
            (if (scc4Ri != null) Map("LTEScc4Ri" -> scc4Ri) else Nil) ++
            (if (scc5CqiCw0 != null) Map("LTEScc5CqiCw0" -> scc5CqiCw0) else Nil) ++
            (if (scc5CqiCw1 != null) Map("LTEScc5CqiCw1" -> scc5CqiCw1) else Nil) ++
            (if (scc5Ri != null) Map("LTEScc5Ri" -> scc5Ri) else Nil) ++
            (if(IpSubtitle != null) {
              Map("SIPType"-> IpSubtitle) ++
                (if(IpSubtitle.toLowerCase=="invite") { Map("SIPInvite"-> 1) } else Map("SIPInvite"-> 0)) ++
                (if(IpSubtitle.toLowerCase=="bye") { Map("SIPBye"-> 1) } else Map("SIPBye"-> 0)) ++
                (if(IpSubtitle.toLowerCase=="http" && HttpMethod != null) {
                  (if(HttpMethod.toLowerCase.contains("200 ok")) {
                    Map("HTTPMethod"-> "200 OK")
                  } else if(HttpMethod.toLowerCase.contains("get")) {
                    Map("HTTPMethod"-> "GET")
                  } else Nil) ++
                    (if(HttpHost != null) { Map("HTTPHost"-> HttpHost) } else Nil) ++
                    (if(HttpContentLength != null) { Map("SessionLength"-> HttpContentLength.toInt) } else Nil)
                } else Nil)
            } else Nil) ++
            (if (ulcarrierIndex.isEmpty) {
              (if (PuschTxPwr != null && PuschTxPwr < 24) Map("LTEPccPuschTxPwr" -> PuschTxPwr) else Nil) ++
                (if (ModulationUl != null) Map("LTEModUl" -> ModulationUl) else Nil) ++
                (if (PushPrb != null) Map("LTEPuschPrb" -> PushPrb) else Nil)
            } else if (PuschTxPwr != null) {
              ulcarrierIndex match {
                case "PCC" =>
                  (if (PuschTxPwr < 24) Map("LTEPccPuschTxPwr" -> PuschTxPwr) else Nil) ++
                    (if (ModulationUl != null) Map("LTEModUl" -> ModulationUl) else Nil) ++
                    (if (PushPrb != null) Map("LTEPuschPrb" -> PushPrb) else Nil)

                case "SCC1" =>
                  (if (PuschTxPwr < 24) Map("LTEScc1PuschTxPwr" -> PuschTxPwr) else Nil) ++
                    (if (ModulationUl != null) Map("LTEScc1ModUL" -> ModulationUl) else Nil) ++
                    (if (PushPrb != null) Map("LTEScc1PuschPrb" -> PushPrb) else Nil)

                case "SCC2" =>
                  if (PuschTxPwr >= 24) Map("LTEScc2PuschTxPwr" -> PuschTxPwr) else Nil

                case "SCC3" =>
                  if (PuschTxPwr >= 24) Map("LTEScc3PuschTxPwr" -> PuschTxPwr) else Nil

                case "SCC4" =>
                  if (PuschTxPwr >= 24) Map("LTEScc4PuschTxPwr" -> PuschTxPwr) else Nil

                case "SCC5" =>
                  if (PuschTxPwr >= 24) Map("LTEScc5PuschTxPwr" -> PuschTxPwr) else Nil
                case _ =>
                  Nil
              }
            }
            else Nil) ++ (if ((sysMode != null) && (isSysModeFrom0x134F && !isSysModeFrom0x184E)) {
            if (sysMode >= 0) getSysModeKPIs(sysMode, SrvStatus, HdrHybrid) else Nil
          }
          else {
            (if ((sysMode0_0x184E != null) && (isSysModeFrom0x134F || isSysModeFrom0x184E)) {
              if (sysMode0_0x184E >= 0) getSysModeKPIs0x134F(sysMode0_0x184E, sysMode1_0x184E, sysIDx184E, sysModeOperational_0x184E, EmmSubState, SrvStatus0_0x184E, SrvStatus1, HdrHybrid) else Nil
            } else Nil)
          }) ++
            (if (nasMsgType != null && !nasMsgType.isEmpty) nasMsgType.toUpperCase() match {
              case "ACTIVATE DEFAULT EPS BEARER CONTEXT REQUEST" =>
                if (Qci != null && !Qci.isEmpty && isValidNumeric(Qci, Constants.CONST_NUMBER)) Map("LTEQci" -> Qci.toInt) else Nil
              case "ACTIVATE DEDICATED EPS BEARER CONTEXT REQUEST" =>
                if (Qci != null && !Qci.isEmpty && isValidNumeric(Qci, Constants.CONST_NUMBER)) Map("LTEQci" -> Qci.toInt) else Nil
              case "PDN CONNECTIVITY REJECT" | "ESM INFORMATION RESPONSE" =>
                (if (pdnEsmCause != null && !pdnEsmCause.isEmpty) Map("LTEPdnRejCause" -> pdnEsmCause) else Nil) ++
                  (if (PdnType != null && !PdnType.isEmpty) Map("LTEPdnType" -> PdnType) else Nil) ++
                  (if (PdnApnName != null && !PdnApnName.isEmpty) Map("LTEPdnApnName" -> PdnApnName) else Nil)
              case "TRACKING AREA REJECT" | "TRACKING AREA UPDATE REQUEST" =>
                (if (TaType != null && !TaType.isEmpty) Map("LTETaType" -> TaType) else Nil)
              case "AUTHENTICATION REJECT" =>
                (if (EmmAuthRejCause != null && !EmmAuthRejCause.isEmpty) Map("EmmAuthRejCause" -> EmmAuthRejCause) else Nil) ++
                  (if (EmmTaRejCause != null && !EmmTaRejCause.isEmpty) Map("EmmTaRejCause" -> EmmTaRejCause) else Nil)
              case "ATTACH REJECT" =>
                (if (AttRejCause != null && !AttRejCause.isEmpty) Map("LTEAttRejCause" -> AttRejCause) else Nil) ++
                  Map("LTEAttRej" -> 1)
              case "SERVICE REJECT" =>
                (if (ServRejCause != null && !ServRejCause.isEmpty) Map("LTEServRejCause" -> ServRejCause) else Nil)
              case "ATTACH ACCEPT" | "TRACKING AREA UPDATE ACCEPT" | "DETACH ACCEPT" =>
                if (VoPSStatus != null && !VoPSStatus.isEmpty) Map("LTEVoPSStatus" -> VoPSStatus) else Nil

              case "ATTACH REQUEST" | "DEACTIVATE EPS BEARER CONTEXT ACCEPT" | "DEACTIVATE EPS BEARER CONTEXT REQUEST" | "PDN CONNECTIVITY REQUEST" | "DETACH REQUEST" =>
                (if (AttachType != null && !AttachType.isEmpty) Map("LTEAttachType" -> AttachType) else Nil) ++
                  (if (EpsMobileIdType != null && !EpsMobileIdType.isEmpty) Map("LTEEpsMobileIdType" -> EpsMobileIdType) else Nil) ++
                  (if (PdnReqType != null && !PdnReqType.isEmpty) Map("LTEPdnReqType" -> PdnReqType) else Nil) ++
                  (if (Imei != null && !Imei.isEmpty) Map("LTEImei" -> Imei) else Nil) ++
                  (if (Imsi != null && !Imsi.isEmpty) Map("LTEImsi" -> Imsi) else Nil) ++
                  (if (dcnrVal != null && !dcnrVal.isEmpty) Map("dcnr" -> dcnrVal) else Nil) ++
                  (if (n1ModeVal != null && !n1ModeVal.isEmpty) Map("n1Mode" -> n1ModeVal) else Nil)
              case _ =>
                Nil
            } else Nil) ++
            (if(nrSpcellPci_gnbid != null && gnbidInfo!= null) {
              (if (gnbidInfo.contains(nrSpcellPci_gnbid))  {
                var enodebSectorId = gnbidInfo.get(nrSpcellPci_gnbid).get.asInstanceOf[GenericRowWithSchema].getString(0)
                (if(isValidNumeric(enodebSectorId.split("_")(0),Constants.CONST_NUMBER)) Map("gNBId" -> enodebSectorId.split("_")(0).toInt) else Nil)
              } else Nil)
            } else Nil)
            ++ (if (SubPacketSize > 0 || (version_B193 <= 18 && version_B193 > 0)) {

            // var bandIndicator: Integer = null
            val LTEServingCellsKpis = scala.collection.mutable.Map[String, Any]()
            var earfcn: Array[(String, Int)] = null

            if (servingCellIndex_B193 != null && earfcn_b193 != null) {
              earfcn = getServingCellPCI(servingCellIndex_B193, earfcn_b193)

              /*if (earfcn != null) {
                if (earfcn != null) bandIndicator = ParseUtils.getBandIndicator(earfcn(0)._2)
              }*/
            }

            var validRX2: Array[String] = Array("RX2", "RX0_RX2", "RX1_RX2", "RX0_RX1_RX2", "RX2_RX3", "RX0_RX2_RX3", "RX1_RX2_RX3", "RX0_RX1_RX2_RX3")
            var validRX3: Array[String] = Array("RX3", "RX0_RX3", "RX1_RX3", "RX0_RX1_RX3", "RX2_RX3", "RX0_RX2_RX3", "RX1_RX2_RX3", "RX0_RX1_RX2_RX3")

            if (PCI != null) {
              for (i <- 0 to (PCI.size - 1)) {
                //logger.info("0xB193: (Map) PCI(" + i + ").1 = " +PCI(i)._1 + "PCI(" + i + ").2 = " +PCI(i)._2)
                var Index: String = ""
                var validRxIndex: String = ""
                var servingPciDist: lang.Float = null

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
                    //logger.info("0xB193: (Map) i= " + i + "; FTLSNRRx_0 size= " + FTLSNRRx_0.size + "; FTLSNRRx_1 size= " + FTLSNRRx_1.size)
                    FTLSNR = if (FTLSNRRx_0(i)._2 > FTLSNRRx_1(i)._2) FTLSNRRx_0(i)._2 else FTLSNRRx_1(i)._2
                  }

                  if (earfcn != null && earfcn.size > i && earfcn(i)._2 != null) {
                    bandIndicator = ParseUtils.getBandIndicator(earfcn(i)._2)
                  }

                  if (!PCI(i)._1.isEmpty) {
                    if (Index.nonEmpty) {
                      if (Index == "Pcc" && x.nonEmpty && y.nonEmpty && servingPCI != null) {
                        var PciLocation = servingPCI.get(PCI(i)._2)
                        if (PciLocation.isDefined) {
                          servingPciDist = DistanceFrmCellsToLoc(y.toDouble, x.toDouble, PciLocation.get.asInstanceOf[GenericRowWithSchema].getDouble(0), PciLocation.get.asInstanceOf[GenericRowWithSchema].getDouble(1))
                        }
                        // logger.info("servingPCI: " + servingPCI + " PCI val: " + PCI(i)._2 + " validate pci: " + PciLocation.isDefined + " current pci locaton: " +PciLocation + " distance: " + servingPciDist + " cell: " + Index)

                      }

                      LTEServingCellsKpis += (if (PCI(i)._2 >= 0) "LTE" + Index + "Pci" -> PCI(i)._2 else "LTE" + Index + "Pci" -> "",
                        if (PCI(i)._2 >= 0 && Index == "Pcc" && servingPciDist != null) "SCellPciDistance" -> servingPciDist else "SCellPciDistance" -> "",
                        //if (RSRP(i)._2 != null && Index != "Scc1") "LTE" + Index + "Rsrp" -> Try(RSRP(i)._2.toFloat).getOrElse("") else "LTE"+ Index + "Rsrp" -> "",
                        if (earfcn != null && earfcn.size > i && earfcn(i)._2 != null && Index != "Pcc") "LTE" + Index + "EarfcnDl" -> Try(earfcn(i)._2).getOrElse("") else if (earfcn != null && earfcn.size > i && earfcn(i)._2 != null && Index == "Pcc") "LTE" + Index + "EarfcnDL" -> Try(earfcn(i)._2).getOrElse("") else "LTE" + Index + "EarfcnDl" -> "",
                        if (RSRP != null && RSRP.size > i && RSRP(i) != null && RSRP(i)._2 != null && Index == "Scc1") "LTESCC1Rsrp" -> Try(RSRP(i)._2.toFloat).getOrElse("") else if (RSRP != null && RSRP.size > i && RSRP(i) != null && RSRP(i)._2 != null && Index != "Scc1") "LTE" + Index + "Rsrp" -> Try(RSRP(i)._2.toFloat).getOrElse("") else "LTE" + Index + "Rsrp" -> "",
                        if (RSRPRx_0 != null && RSRPRx_0.size > i && isValidNumeric(RSRPRx_0(i)._2, Constants.CONST_FLOAT)) "LTE" + Index + "RsrpRx0" -> RSRPRx_0(i)._2.toFloat else "LTE" + Index + "RsrpRx0" -> "",
                        if (RSRPRx_1 != null) "LTE" + Index + "RsrpRx1" -> Try(RSRPRx_1(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RsrpRx1" -> "",
                        if (RSRQ != null && RSRQ.size > i && RSRQ(i)._2 != null) "LTE" + Index + "Rsrq" -> Try(RSRQ(i)._2.toFloat).getOrElse("") else "LTE" + Index + "Rsrq" -> "",
                        if (RSRQRx_0 != null) "LTE" + Index + "RsrqRx0" -> Try(RSRQRx_0(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RsrqRx0" -> "",
                        if (RSRQRx_1 != null) "LTE" + Index + "RsrqRx1" -> Try(RSRQRx_1(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RsrqRx1" -> "",
                        if (RSSI != null && RSSI.size > i && RSSI(i)._2 != null) "LTE" + Index + "Rssi" -> Try(RSSI(i)._2.toFloat).getOrElse("") else "LTE" + Index + "Rssi" -> "",
                        if (RSSIRx_0 != null) "LTE" + Index + "RssiRx0" -> Try(RSSIRx_0(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RssiRx0" -> "",
                        if (RSSIRx_1 != null) "LTE" + Index + "RssiRx1" -> Try(RSSIRx_1(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RssiRx1" -> "",
                        if (FTLSNR != null) "LTE" + Index + "Sinr" -> Try(FTLSNR.toFloat).getOrElse("") else "LTE" + Index + "Sinr" -> "",
                        if (FTLSNRRx_0 != null && FTLSNRRx_0.size > i) {
                          if (FTLSNRRx_0(i)._2 != null) "LTE" + Index + "SinrRx0" -> Try(FTLSNRRx_0(i)._2.toFloat).getOrElse("") else "LTE" + Index + "SinrRx0" -> ""
                        } else "LTE" + Index + "SinrRx0" -> "",
                        if (FTLSNRRx_1 != null && FTLSNRRx_1.size > i) {
                          if (FTLSNRRx_1(i)._2 != null) "LTE" + Index + "SinrRx1" -> Try(FTLSNRRx_1(i)._2.toFloat).getOrElse("") else "LTE" + Index + "SinrRx1" -> ""
                        } else "LTE" + Index + "SinrRx1" -> "",
                        if (bandIndicator != null && bandIndicator != -1) "LTE" + Index + "BandInd" -> bandIndicator else "LTE" + Index + "BandInd" -> "")
                    }

                    if (horxd_mode != null) {
                      if (horxd_mode(i)._2 != 0 && (Index.nonEmpty && validRxIndex.nonEmpty)) {
                        LTEServingCellsKpis += ("LTE" + validRxIndex + "ValidRx" -> "RX0_RX1_RX2_RX3",
                          if (RSRPRx_2(i)._2 != null) "LTE" + Index + "RsrpRx2" -> Try(RSRPRx_2(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RsrpRx2" -> "",
                          if (RSRPRx_3(i)._2 != null) "LTE" + Index + "RsrpRx3" -> Try(RSRPRx_3(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RsrpRx3" -> "",
                          if (RSRQRx_2(i)._2 != null) "LTE" + Index + "RsrqRx2" -> Try(RSRQRx_2(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RsrqRx2" -> "",
                          if (RSRQRx_3(i)._2 != null) "LTE" + Index + "RsrqRx3" -> Try(RSRQRx_3(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RsrqRx3" -> "",
                          if (RSSIRx_2(i)._2 != null) "LTE" + Index + "RssiRx2" -> Try(RSSIRx_2(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RssiRx2" -> "",
                          if (RSSIRx_3(i)._2 != null) "LTE" + Index + "RssiRx3" -> Try(RSSIRx_3(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RssiRx3" -> "",
                          if (FTLSNRRx_2(i)._2 != null) "LTE" + Index + "SinrRx2" -> Try(FTLSNRRx_2(i)._2.toFloat).getOrElse("") else "LTE" + Index + "SinrRx2" -> "",
                          if (FTLSNRRx_3(i)._2 != null) "LTE" + Index + "SinrRx3" -> Try(FTLSNRRx_3(i)._2.toFloat).getOrElse("") else "LTE" + Index + "SinrRx3" -> ""
                        )
                      } else {
                        LTEServingCellsKpis += ("LTE" + validRxIndex + "ValidRx" -> "RX0_RX1")
                      }
                    }

                    if (ValidRx != null && ValidRx.size > i) {
                      //logger.info("Get validRx value: " + ValidRx(i)._2)
                      LTEServingCellsKpis += (if (ValidRx != null && !ValidRx.isEmpty) "LTE" + validRxIndex + "ValidRx" -> ValidRx(i)._2 else "LTE" + validRxIndex + "ValidRx" -> "")

                      //RX2
                      (if (validRX2.contains(ValidRx(i)._2) && Index.nonEmpty) {
                        LTEServingCellsKpis += (if (RSRPRx_2 != null && RSRPRx_2.size > i && RSRPRx_2(i)._2 != null) "LTE" + Index + "RsrpRx2" -> Try(RSRPRx_2(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RsrpRx2" -> "",
                          if (RSRQRx_2 != null && RSRQRx_2.size > i && RSRQRx_2(i)._2 != null) "LTE" + Index + "RsrqRx2" -> Try(RSRQRx_2(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RsrqRx2" -> "",
                          if (RSSIRx_2 != null && RSSIRx_2.size > i && RSSIRx_2(i)._2 != null) "LTE" + Index + "RssiRx2" -> Try(RSSIRx_2(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RssiRx2" -> "",
                          if (FTLSNRRx_2 != null && FTLSNRRx_2.size > i && FTLSNRRx_2(i)._2 != null) "LTE" + Index + "SinrRx2" -> Try(FTLSNRRx_2(i)._2.toFloat).getOrElse("") else "LTE" + Index + "SinrRx2" -> "")
                      })
                      //RX3
                      (if (validRX3.contains(ValidRx(i)._2) && Index.nonEmpty) {
                        LTEServingCellsKpis += (if (RSRPRx_3 != null && RSRPRx_3.size > i && RSRPRx_3(i)._2 != null) "LTE" + Index + "RsrpRx3" -> Try(RSRPRx_3(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RsrpRx3" -> "",
                          if (RSRQRx_3 != null && RSRQRx_3.size > i && RSRQRx_3(i)._2 != null) "LTE" + Index + "RsrqRx3" -> Try(RSRQRx_3(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RsrqRx3" -> "",
                          if (RSSIRx_3 != null && RSSIRx_3.size > i && RSSIRx_3(i)._2 != null) "LTE" + Index + "RssiRx3" -> Try(RSSIRx_3(i)._2.toFloat).getOrElse("") else "LTE" + Index + "RssiRx3" -> "",
                          if (FTLSNRRx_3 != null && FTLSNRRx_3.size > i && FTLSNRRx_3(i)._2 != null) "LTE" + Index + "SinrRx3" -> Try(FTLSNRRx_3(i)._2.toFloat).getOrElse("") else "LTE" + Index + "SinrRx3" -> "")
                      })
                    } else Nil
                  }
                } catch {
                  case ge: Exception =>
                    logger.error("ES Exception occured while executing getCaModeKPIs for Index : " + Index + " Message : " + ge.getMessage)
                    //println("ES Exception occured while executing getCaModeKPIs : " + ge.getMessage)
                    logger.error("ES Exception occured while executing getCaModeKPIs Cause: " + ge.getCause())
                    logger.error("ES Exception occured while executing getCaModeKPIs StackTrace: " + ge.printStackTrace())
                }
              }
            }
            LTEServingCellsKpis += (if(servingIndexSize_0xb193>0) "LTEnumServCC" ->servingIndexSize_0xb193 else "LTEnumServCC" -> "")
          } else Nil) ++ (if (PciNcell != null && !PciNcell.isEmpty) {
            var Ncells = PciNcell.split(",").map(_.trim).toList.map(_.toInt)
            var Earfcn = EarfcnNcell.split(",").map(_.trim).toList.map(_.toInt)

            val NcellMap = scala.collection.mutable.Map[String, Any]()
            var neighboringPciDist: lang.Float = null
            var neighboringPciEnodebId: String = null
            var neighboringPciBand : Integer = null

            val ncellRsrpRx0InfoList = mutable.ListBuffer.empty[ncellKpiInfo]
            val ncellRsrpRx1InfoList = mutable.ListBuffer.empty[ncellKpiInfo]
            val ncellRsrqRx0InfoList = mutable.ListBuffer.empty[ncellKpiInfo]
            val ncellRsrqRx1InfoList = mutable.ListBuffer.empty[ncellKpiInfo]
            val ncellRssiRx0InfoList = mutable.ListBuffer.empty[ncellKpiInfo]
            val ncellRssiRx1InfoList = mutable.ListBuffer.empty[ncellKpiInfo]

            breakable {
              for (i <- 1 to Ncells.size) {
                if (i > 20) {
                  break
                }

                if (Ncells != null && Ncells.nonEmpty && x.nonEmpty && y.nonEmpty && neighboringPCI != null) {
                  var NcellPciLocation = neighboringPCI.get(PciNcell.split(",")(i - 1).toInt)
                  if (NcellPciLocation.isDefined) {
                    neighboringPciDist = DistanceFrmCellsToLoc(y.toDouble, x.toDouble, NcellPciLocation.get.asInstanceOf[GenericRowWithSchema].getDouble(0), NcellPciLocation.get.asInstanceOf[GenericRowWithSchema].getDouble(1))
                          neighboringPciEnodebId = NcellPciLocation.get.asInstanceOf[GenericRowWithSchema].getString(2)
                          neighboringPciBand = NcellPciLocation.get.asInstanceOf[GenericRowWithSchema].getInt(4)
                  }
                }

                NcellMap += (if (Earfcn != null && Earfcn.nonEmpty) "NCell" + i + "BAND" -> ParseUtils.getBandIndicator(EarfcnNcell.split(",")((Earfcn.size) - 1).toInt) else "NCell" + i + "BAND" -> "",
                  if (Earfcn != null && Earfcn.nonEmpty) "NCell" + i + "EARFCN" -> EarfcnNcell.split(",")((Earfcn.size) - 1).toInt else "NCell" + i + "EARFCN" -> "",
                  if (Ncells != null && Ncells.nonEmpty) "NCell" + i + "PCI" -> PciNcell.split(",")(i - 1).toInt else "NCell" + i + "PCI" -> "",
                  if (Ncells != null && Ncells.nonEmpty && neighboringPciDist != null) "NCell" + i + "PciDistance" -> neighboringPciDist else "NCell" + i + "PciDistance" -> "",
                  if (RrsrpNcell != null && !RrsrpNcell.isEmpty) "NCell" + i + "RSRP" -> RrsrpNcell.split(",")(i - 1).toFloat else "NCell" + i + "RSRP" -> "",
                  //                  if (RSRPRx0_Ncell != null && !RSRPRx0_Ncell.isEmpty) "NCell" + i + "RSRPRx0" -> RSRPRx0_Ncell.split(",")(i - 1).toFloat else "NCell" + i + "RSRPRx0" -> "",
                  //                  if (RSRPRx1_Ncell != null && !RSRPRx1_Ncell.isEmpty) "NCell" + i + "RSRPRx1" -> RSRPRx1_Ncell.split(",")(i - 1).toFloat else "NCell" + i + "RSRPRx1" -> "",
                  if (RsrqNcell != null && !RsrqNcell.isEmpty) "NCell" + i + "RSRQ" -> RsrqNcell.split(",")(i - 1).toFloat else "NCell" + i + "RSRQ" -> "",
                  //                  if (RSRQRx0_Ncell != null && !RSRQRx0_Ncell.isEmpty) "NCell" + i + "RSRQRx0" -> RSRQRx0_Ncell.split(",")(i - 1).toFloat else "NCell" + i + "RSRQRx0" -> "",
                  //                  if (RSRQRx1_Ncell != null && !RSRQRx1_Ncell.isEmpty) "NCell" + i + "RSRQRx1" -> RSRQRx1_Ncell.split(",")(i - 1).toFloat else "NCell" + i + "RSRQRx1" -> "",
                  if (RssiNcell != null && !RssiNcell.isEmpty) "NCell" + i + "RSSI" -> RssiNcell.split(",")(i - 1).toFloat else "NCell" + i + "RSSI" -> "")
                //                  if (RSSIRx0_Ncell != null && !RSSIRx0_Ncell.isEmpty) "NCell" + i + "RSSIRx0" -> RSSIRx0_Ncell.split(",")(i - 1).toFloat else "NCell" + i + "RSSIRx0" -> "",
                //                  if (RSSIRx1_Ncell != null && !RSSIRx1_Ncell.isEmpty) "NCell" + i + "RSSIRx1" -> RSSIRx1_Ncell.split(",")(i - 1).toFloat else "NCell" + i + "RSSIRx1" -> "")
                if (Ncells != null && Ncells.nonEmpty && neighboringPciEnodebId != null) NcellMap += "NCell" + i + "eNBId" -> neighboringPciEnodebId.split("_")(0)
                if (Ncells != null && Ncells.nonEmpty && neighboringPciBand != null) NcellMap += "NCell" + i + "AtolBand" -> neighboringPciBand

                if (RSRPRx0_Ncell != null && !RSRPRx0_Ncell.isEmpty) ncellRsrpRx0InfoList += ncellKpiInfo(i.toString, RSRPRx0_Ncell.split(",")(i - 1).toFloat)
                if (RSRPRx1_Ncell != null && !RSRPRx1_Ncell.isEmpty) ncellRsrpRx1InfoList += ncellKpiInfo(i.toString, RSRPRx1_Ncell.split(",")(i - 1).toFloat)
                if (RSRQRx0_Ncell != null && !RSRQRx0_Ncell.isEmpty) ncellRsrqRx0InfoList += ncellKpiInfo(i.toString, RSRQRx0_Ncell.split(",")(i - 1).toFloat)
                if (RSRQRx1_Ncell != null && !RSRQRx1_Ncell.isEmpty) ncellRsrqRx1InfoList += ncellKpiInfo(i.toString, RSRQRx1_Ncell.split(",")(i - 1).toFloat)
                if (RSSIRx0_Ncell != null && !RSSIRx0_Ncell.isEmpty) ncellRssiRx0InfoList += ncellKpiInfo(i.toString, RSSIRx0_Ncell.split(",")(i - 1).toFloat)
                if (RSSIRx1_Ncell != null && !RSSIRx1_Ncell.isEmpty) ncellRssiRx1InfoList += ncellKpiInfo(i.toString, RSSIRx1_Ncell.split(",")(i - 1).toFloat)

                neighboringPciEnodebId = null
                neighboringPciBand = null
              }
            }

            val timestampString = timestamp.replace(" ", "T")
            val currentTime = ZonedDateTime.parse(timestampString + 'Z')
            var cutoffDate: String = ""
            if (commonConfigParams.ES_NESTEDFIELD_CUTOFFDATE != "") cutoffDate = commonConfigParams.ES_NESTEDFIELD_CUTOFFDATE

            if (cutoffDate.nonEmpty) {
              if (currentTime.isAfter(ZonedDateTime.parse(cutoffDate + "T00:00:00Z"))) {
                NcellMap += ("NcellRSRPRx0" -> ncellRsrpRx0InfoList.map { f => (Map("key" -> f.key) ++ Map("value" -> f.value)) },
                  "NcellRSRPRx1" -> ncellRsrpRx1InfoList.map { f => (Map("key" -> f.key) ++ Map("value" -> f.value)) },
                  "NcellRSRQRx0" -> ncellRsrqRx0InfoList.map { f => (Map("key" -> f.key) ++ Map("value" -> f.value)) },
                  "NcellRSRQRx1" -> ncellRsrqRx1InfoList.map { f => (Map("key" -> f.key) ++ Map("value" -> f.value)) },
                  "NcellRSSIRx0" -> ncellRssiRx0InfoList.map { f => (Map("key" -> f.key) ++ Map("value" -> f.value)) },
                  "NcellRSSIRx1" -> ncellRssiRx1InfoList.map { f => (Map("key" -> f.key) ++ Map("value" -> f.value)) }
                )
              }
            }
            NcellMap
          } else Nil) ++
            /*++ Map(if (FTLSNRRx_0 != null && !FTLSNRRx_0.isEmpty) "LTEScc1SinrRx0" -> RSSIRx1_Ncell.split(",")(0) else "LTEScc1SinrRx0" -> "")*/
            (if (nrs_bsnr_0xb992 != null && nrs_bsnr_0xb992.nonEmpty && nrs_bsnr_0xb992 != "NaN") Map("NRSBSNR" -> nrs_bsnr_0xb992) else Nil) ++

            //            (if(nrServingRxBeamSNR0_0xB992 != null)  Map("NRServingRxBeamSNR0" -> nrServingRxBeamSNR0_0xB992) else Nil) ++
            //            (if(nrServingRxBeamSNR1_0xB992 != null)  Map("NRServingRxBeamSNR1" -> nrServingRxBeamSNR1_0xB992) else Nil) ++

            (if (version_B975 > 0 && NRS_BeamCount != null) {
              val NRServingCellsKpis = scala.collection.mutable.Map[String, Any]()
              if (NRS_BeamCount.size > 0) {
                try {
                  val i: Integer = 0
                  var ccIndex: String = ""
                  if (!NRS_BeamCount(i)._1.isEmpty) {
                    if (NRS_BeamCount(i)._1 != null && isValidNumeric(NRS_BeamCount(i)._1, Constants.CONST_NUMBER)) {
                      //logger.info("nrs_beamcount>>>>>>>>>>>>" + NRS_BeamCount(i)._1.toInt)
                    if (NRS_BeamCount(i)._1.toInt == 0) {
                      ccIndex = "PCC"
                    } else if (NRS_BeamCount(i)._1.toInt == 1) {
                      ccIndex = "SCC1"
                    } else if (NRS_BeamCount(i)._1.toInt == 2) {
                      ccIndex = "SCC2"
                    } else if (NRS_BeamCount(i)._1.toInt == 3) {
                      ccIndex = "SCC3"
                    } else if (NRS_BeamCount(i)._1.toInt == 4) {
                      ccIndex = "SCC4"
                    } else if (NRS_BeamCount(i)._1.toInt == 5) {
                      ccIndex = "SCC5"
                    } else if (NRS_BeamCount(i)._1.toInt == 6) {
                      ccIndex = "SCC6"
                    } else if (NRS_BeamCount(i)._1.toInt == 7) {
                      ccIndex = "SCC7"
                    }
                  }

                  NRServingCellsKpis += (if (NRS_RSRP != null && ccIndex.nonEmpty) (if (NRS_RSRP(i)._2 != null && ccIndex == "PCC") "NRSBRSRP" -> NRS_RSRP(i)._2 else "NRS" + ccIndex + "BRSRP" -> NRS_RSRP(i)._2) else "NRSBRSRP" -> "",
                    if (NRS_RSRQ != null && ccIndex.nonEmpty) (if (NRS_RSRQ(i)._2 != null && isValidNumeric(NRS_RSRQ(i)._2, Constants.CONST_FLOAT) && ccIndex == "PCC") "NRSBRSRQ" -> NRS_RSRQ(i)._2.toFloat else "NRS" + ccIndex + "BRSRQ" -> NRS_RSRQ(i)._2.toFloat) else "NRSBRSRQ" -> "",
                    if (NRS_SNR != null && ccIndex.nonEmpty) (if (NRS_SNR(i)._2 != null && ccIndex == "PCC" && NRS_SNR(i)._2 != "NaN") "NRSBSNR" -> Try(NRS_SNR(i)._2.toFloat).getOrElse("") else if (ccIndex != "PCC" && NRS_SNR(i)._2 != "NaN") "NRS" + ccIndex + "BSNR" -> Try(NRS_SNR(i)._2.toFloat).getOrElse("") else "NRSBSNR" -> "") else "NRSBSNR" -> "",
                    if (NRS_BeamCount != null && ccIndex.nonEmpty) (if (NRS_BeamCount(i)._2 != null && ccIndex == "PCC") "NRBEAMCOUNT" -> Try(NRS_BeamCount(i)._2.toInt).getOrElse("") else "NR" + ccIndex + "BEAMCOUNT" -> Try(NRS_BeamCount(i)._2.toInt).getOrElse("")) else "NRBEAMCOUNT" -> "",
                    if (NRS_PCI != null && ccIndex.nonEmpty) (if (NRS_PCI(i)._2 != null && ccIndex == "PCC") "NRSPHYCELLID" -> Try(NRS_PCI(i)._2.toInt).getOrElse("") else "NRS" + ccIndex + "PHYCELLID" -> Try(NRS_PCI(i)._2.toInt).getOrElse("")) else "NRSPHYCELLID" -> "")

                }
              } catch {
                case ge: Exception =>
                  logger.error("ES Exception Occurred while executing getNrsKPIs for Index : Message : " + ge.getMessage)
                  //println("ES Exception Occurred while executing getNrsKPIs : " + ge.getMessage)
                  logger.error("ES Exception Occurred while executing getNrsKPIs Cause: " + ge.getCause())
                  logger.error("ES Exception Occurred while executing getNrsKPIs StackTrace: " + ge.printStackTrace())
              }
            } else {
              NRServingCellsKpis += (if (nrSpCellPci != null) "NRSPHYCELLID" -> nrSpCellPci else "NRSPHYCELLID" -> "")
            }
            if(NRServingCellsKpis.contains("NRSBRSRP") && LTE_RRC_State != null && !LTE_RRC_State.isEmpty) {
                NRServingCellsKpis +=  (if(NRServingCellsKpis.get("NRSBRSRP").get !="" && LTE_RRC_State=="Connected") "NrSCGStateBRSRP" -> "CONNECTED" else "NrSCGStateBRSRP" -> "DISCONNECTED")
            } else if(NRServingCellsKpis.contains("NRSBRSRP") && isLteRrcState) {
              NRServingCellsKpis +=  (if(NRServingCellsKpis.get("NRSBRSRP").get !="") "NrSCGStateBRSRP" -> "CONNECTED" else "NrSCGStateBRSRP" -> "DISCONNECTED")
            }
            NRServingCellsKpis
          } else Nil) ++
            (if (carrierId0xB887 != null && Nrdlmcs0xB887 != null) {
              var mcsPerCarrierId: Array[(String, String)] = null
              var NrMcgPerCarrierIdKpis = scala.collection.mutable.Map[String, Any]()
              mcsPerCarrierId = getNrCarrierMcs(carrierId0xB887, Nrdlmcs0xB887)

              var indexList: Array[String] = Array("Pcc", "Scc1", "Scc2", "Scc3", "Scc4", "Scc5", "Scc6", "Scc7")

              if (mcsPerCarrierId != null) {
                for (i <- 0 to (mcsPerCarrierId.size - 1)) {
                  NrMcgPerCarrierIdKpis += (if (mcsPerCarrierId(i)._1 != null && mcsPerCarrierId(i)._1.toInt >= 0) "Nr" + indexList(mcsPerCarrierId(i)._1.toInt) + "DlMcs" -> mcsPerCarrierId(i)._2.toInt else "Nr" + indexList(mcsPerCarrierId(i)._1.toInt) + "DlMcs" -> "")
                }
              }


              NrMcgPerCarrierIdKpis

            } else Nil))

       // val MapWithoutMetaData = mapWithoutMetadata  ++ hbflexKpisFromSig(row:Row)

        val mapWithMetadata =  mapWithoutMetadata ++ Map("document_metadata" -> mapWithoutMetadata.keys.toList)

        import org.json4s.jackson.Serialization
        implicit val formats = org.json4s.DefaultFormats

        val jsonStringWithoutMetaData = Serialization.write(mapWithoutMetadata -- Set("internal_type", "internal_index"))
        mapWithMetadata ++ Map("jsonString_WriteToHdfs" -> jsonStringWithoutMetaData)
      })
    })
    mapRDD
  }

  def extractThroughputKpis(row: org.apache.spark.sql.Row,logRecordJoinedDF: LogRecordJoinedDF): scala.collection.mutable.Map[String,Any]  ={
    val indexMap: scala.collection.mutable.Map[String,Any] = scala.collection.mutable.Map()

    //   if(logRecordJoinedDF.kpiisRlcUlInstTputExists) {
    //Cols needed for kpi calculation
    //"nr5grlculinstTput0xb868DiffSeconds","nr5grlculinstTput0xb868DiffTbs"-------------------------->>>>>>> : 0xB868 a/b NR5G RLC UL Stats: 47208 nr5grlculinstTput
    val nr5grlculinstTput0xb868DiffTbs: java.lang.Long = if (!row.isNullAt(row.fieldIndex("nr5grlculinstTput0xb868DiffTbs"))) row.getLong(row.fieldIndex("nr5grlculinstTput0xb868DiffTbs")) else null
    val nr5grlculinstTput0xb868DiffSeconds: java.lang.Double = if (!row.isNullAt(row.fieldIndex("nr5grlculinstTput0xb868DiffSeconds"))) row.getDouble(row.fieldIndex("nr5grlculinstTput0xb868DiffSeconds")) else null

    //nr5grlculinstTput0xb868DiffTbs,nr5grlculinstTput0xb868DiffSeconds

    val nr5gRlcUlInstTput: java.lang.Double = if (nr5grlculinstTput0xb868DiffTbs != null && nr5grlculinstTput0xb868DiffTbs != 0 && nr5grlculinstTput0xb868DiffSeconds != null && nr5grlculinstTput0xb868DiffSeconds != 0.0) {
      BigDecimal(nr5grlculinstTput0xb868DiffTbs.toDouble * 8.0  / (nr5grlculinstTput0xb868DiffSeconds*1000000.0)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    } else {
      null
    }
    if(nr5gRlcUlInstTput!=null && nr5gRlcUlInstTput > 0.0) indexMap.put("NR5gRlcUlInstTput",nr5gRlcUlInstTput)
    //  }

    // if(logRecordJoinedDF.kpiPdcpUlInstTputExists) {
    val nr5gRxBytes0xB860DiffTbs: java.lang.Long = if (!row.isNullAt(row.fieldIndex("nr5gRxBytes0xB860DiffTbs"))) row.getLong(row.fieldIndex("nr5gRxBytes0xB860DiffTbs")) else null
    val nr5gRxBytes0xB860DiffSeconds: java.lang.Double = if (!row.isNullAt(row.fieldIndex("nr5gRxBytes0xB860DiffSeconds"))) row.getDouble(row.fieldIndex("nr5gRxBytes0xB860DiffSeconds")) else null
    val nr5GPdcpUlInstTput: java.lang.Double = if (nr5gRxBytes0xB860DiffTbs != null && nr5gRxBytes0xB860DiffTbs != 0 && nr5gRxBytes0xB860DiffSeconds != null && nr5gRxBytes0xB860DiffSeconds != 0.0) {
      BigDecimal(nr5gRxBytes0xB860DiffTbs.toDouble * 8.0  / (nr5gRxBytes0xB860DiffSeconds*1000000.0)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    } else {
      null
    }
    //NR5GPdcpUlInstTput
    if(nr5GPdcpUlInstTput!=null && nr5GPdcpUlInstTput > 0.0) indexMap.put("NR5GPdcpUlInstTput",nr5GPdcpUlInstTput)
    //  }

    // if(logRecordJoinedDF.kpiPdcpDlExists) {
    //nr5gRlcDataBytes0xB84DDiffSeconds,nr5gRlcDataBytes0xB84DDiff

    //"nr5GPdcpDl0xB842DiffSeconds","nr5gPdcpDl0xB842DataDiff" start
    val nr5gPdcpDl0xB842DataDiff: java.lang.Long = if (!row.isNullAt(row.fieldIndex("nr5gPdcpDl0xB842DataDiff"))) row.getLong(row.fieldIndex("nr5gPdcpDl0xB842DataDiff")) else null
    val nr5GPdcpDl0xB842DiffSeconds: java.lang.Double = if (!row.isNullAt(row.fieldIndex("nr5GPdcpDl0xB842DiffSeconds"))) row.getDouble(row.fieldIndex("nr5GPdcpDl0xB842DiffSeconds")) else null
    val nr5GPdcpDlInstTput: java.lang.Double = if (nr5gPdcpDl0xB842DataDiff != null && nr5gPdcpDl0xB842DataDiff != 0 && nr5GPdcpDl0xB842DiffSeconds != null && nr5GPdcpDl0xB842DiffSeconds != 0.0) {
      BigDecimal((nr5gPdcpDl0xB842DataDiff.toDouble * 8.0) / (nr5GPdcpDl0xB842DiffSeconds * 1000000.0)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    } else {
      null
    }
    if(nr5GPdcpDlInstTput!=null && nr5GPdcpDlInstTput > 0.0) indexMap.put("NR5GPdcpDlInstTput",nr5GPdcpDlInstTput)

    //"nr5GPdcpDl0xB842DiffSeconds","nr5gPdcpDl0xB842DataDiff" end
    //  }
    val evtTimestamp = getTimestampPG(row.getString(row.fieldIndex("dateWithoutMillis"))).getOrElse().asInstanceOf[Timestamp]

    val numMissPDUToUpperLayer0xB842Diff: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numMissPDUToUpperLayer0xB842Diff"))) row.getLong(row.fieldIndex("numMissPDUToUpperLayer0xB842Diff")) else null
    val numDataPDUReceived0xB842Diff: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numDataPDUReceived0xB842Diff"))) row.getLong(row.fieldIndex("numDataPDUReceived0xB842Diff")) else null
    val nrPDCPDLLossRate: java.lang.Double = if (numMissPDUToUpperLayer0xB842Diff != null && numMissPDUToUpperLayer0xB842Diff >= 0 && numDataPDUReceived0xB842Diff != null && (numMissPDUToUpperLayer0xB842Diff + numDataPDUReceived0xB842Diff) > 0) {
      BigDecimal(100D * numMissPDUToUpperLayer0xB842Diff / (numMissPDUToUpperLayer0xB842Diff + numDataPDUReceived0xB842Diff)).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    } else null
    //logger.info(s" evtTimestamp=$evtTimestamp ->   numMissPDUToUpperLayer0xB842Diff=$numMissPDUToUpperLayer0xB842Diff numDataPDUReceived0xB842Diff=$numDataPDUReceived0xB842Diff ->   nrPDCPDLLossRate=$nrPDCPDLLossRate")
    if (nrPDCPDLLossRate != null && nrPDCPDLLossRate > 0.0) indexMap.put("NRPDCPDLLossRate", nrPDCPDLLossRate)

    val totalNumDiscardPackets0xB860Diff: java.lang.Long = if (!row.isNullAt(row.fieldIndex("totalNumDiscardPackets0xB860Diff"))) row.getLong(row.fieldIndex("totalNumDiscardPackets0xB860Diff")) else null
    val numRXPackets0xB860Diff: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numRXPackets0xB860Diff"))) row.getLong(row.fieldIndex("numRXPackets0xB860Diff")) else null
    val nrPDCPULDiscardRate: java.lang.Double = if (totalNumDiscardPackets0xB860Diff != null && totalNumDiscardPackets0xB860Diff >= 0 && numRXPackets0xB860Diff != null && numRXPackets0xB860Diff > 0) {
      BigDecimal(100D * totalNumDiscardPackets0xB860Diff / numRXPackets0xB860Diff).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    } else null
    //logger.info(s" evtTimestamp=$evtTimestamp ->   totalNumDiscardPackets0xB860Diff=$totalNumDiscardPackets0xB860Diff numRXPackets0xB860Diff=$numRXPackets0xB860Diff ->   nrPDCPULDiscardRate=$nrPDCPULDiscardRate")
    if (nrPDCPULDiscardRate != null && nrPDCPULDiscardRate > 0.0) indexMap.put("NRPDCPULDiscardRate", nrPDCPULDiscardRate)

    val totalReTxPDUs0x0xB868Diff: java.lang.Long = if (!row.isNullAt(row.fieldIndex("totalReTxPDUs0x0xB868Diff"))) row.getLong(row.fieldIndex("totalReTxPDUs0x0xB868Diff")) else null
    val totalTxPDUs0x0xB868Diff: java.lang.Long = if (!row.isNullAt(row.fieldIndex("totalTxPDUs0x0xB868Diff"))) row.getLong(row.fieldIndex("totalTxPDUs0x0xB868Diff")) else null
    val nrRLCULBLER: java.lang.Double = if (totalReTxPDUs0x0xB868Diff != null && totalReTxPDUs0x0xB868Diff >= 0 && totalTxPDUs0x0xB868Diff != null && totalTxPDUs0x0xB868Diff > 0) {
      BigDecimal(100D * totalReTxPDUs0x0xB868Diff / totalTxPDUs0x0xB868Diff).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    } else null
    //logger.info(s" evtTimestamp=$evtTimestamp ->   totalReTxPDUs0x0xB868Diff=$totalReTxPDUs0x0xB868Diff totalTxPDUs0x0xB868Diff=$totalTxPDUs0x0xB868Diff ->  nrRLCULBLER=$nrRLCULBLER")
    if (nrRLCULBLER != null && nrRLCULBLER > 0) indexMap.put("NRRLCULBLER", nrRLCULBLER)

    val numReTxPDU0xB84DDiff: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numReTxPDU0xB84DDiff"))) row.getLong(row.fieldIndex("numReTxPDU0xB84DDiff")) else null
    val numMissedUMPDU0xB84DDiff: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numMissedUMPDU0xB84DDiff"))) row.getLong(row.fieldIndex("numMissedUMPDU0xB84DDiff")) else null
    val numDroppedPDU0xB84DDiff: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numDroppedPDU0xB84DDiff"))) row.getLong(row.fieldIndex("numDroppedPDU0xB84DDiff")) else null
    val numDataPDU0xB84DDiff: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numDataPDU0xB84DDiff"))) row.getLong(row.fieldIndex("numDataPDU0xB84DDiff")) else null
    val nrRLCDLBLER: java.lang.Double = if (numDataPDU0xB84DDiff != null && numDataPDU0xB84DDiff > 0 && numReTxPDU0xB84DDiff != null && numMissedUMPDU0xB84DDiff != null && (numReTxPDU0xB84DDiff + numMissedUMPDU0xB84DDiff) >= 0) {
      BigDecimal(100D * (numReTxPDU0xB84DDiff + numMissedUMPDU0xB84DDiff) / numDataPDU0xB84DDiff).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    } else null
    //logger.info(s" evtTimestamp=$evtTimestamp ->   numReTxPDU0xB84DDiff=$numReTxPDU0xB84DDiff numMissedUMPDU0xB84DDiff=$numMissedUMPDU0xB84DDiff numDataPDU0xB84DDiff=$numDataPDU0xB84DDiff ->   nrRLCDLBLER=$nrRLCDLBLER")
    if (nrRLCDLBLER != null && nrRLCDLBLER > 0) indexMap.put("NRRLCDLBLER", nrRLCDLBLER)
    val nrRLCDLDropRate: java.lang.Double = if (numDataPDU0xB84DDiff != null && numDataPDU0xB84DDiff > 0 && numDroppedPDU0xB84DDiff != null && numDroppedPDU0xB84DDiff >= 0) {
      BigDecimal(100D * numDroppedPDU0xB84DDiff / numDataPDU0xB84DDiff).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    } else null
    //logger.info(s" evtTimestamp=$evtTimestamp ->    numDroppedPDU0xB84DDiff=$numDroppedPDU0xB84DDiff numDataPDU0xB84DDiff=$numDataPDU0xB84DDiff ->   nrRLCDLDropRate=$nrRLCDLDropRate")
    if (nrRLCDLDropRate != null && nrRLCDLDropRate > 0) indexMap.put("NRRLCDLDropRate", nrRLCDLDropRate)

    //  if(logRecordJoinedDF.kpiRlcInstTputExists) {
    val nr5gRlcDataBytes0xB84DDiff: java.lang.Long = if (!row.isNullAt(row.fieldIndex("nr5gRlcDataBytes0xB84DDiff"))) row.getLong(row.fieldIndex("nr5gRlcDataBytes0xB84DDiff")) else null
    val nr5gRlcDataBytes0xB84DDiffSeconds: java.lang.Double = if (!row.isNullAt(row.fieldIndex("nr5gRlcDataBytes0xB84DDiffSeconds"))) row.getDouble(row.fieldIndex("nr5gRlcDataBytes0xB84DDiffSeconds")) else null
    val nr5gRlcInstTput: java.lang.Double = if (nr5gRlcDataBytes0xB84DDiff != null && nr5gRlcDataBytes0xB84DDiff != 0 && nr5gRlcDataBytes0xB84DDiffSeconds != null && nr5gRlcDataBytes0xB84DDiffSeconds != 0.0) {
      BigDecimal(nr5gRlcDataBytes0xB84DDiff.toDouble  * 8.0/ (nr5gRlcDataBytes0xB84DDiffSeconds*1000000.0)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    } else {
      null
    }
    //NR5GRlcInstTput
    if(nr5gRlcInstTput!=null && nr5gRlcInstTput > 0.0) indexMap.put("NR5GRlcInstTput",nr5gRlcInstTput)
    //   }

    //NR5gRlcUlInstTput   float
    //NR5GPhyULThroughput float
    //NR5gPhyUlRetransmissionRate float
    //NR5GPdcpUlInstTput float
    //NR5GRlcInstTput float
    //txNewTbSum0xB883  ---------->>>> 0xB883 ::5G PHY UL Throughput 5G PHY UL Throughput

    // if(logRecordJoinedDF.kpiNrPhyULThroughputExists) {
    val nr5GPhyULThroughput: java.lang.Long = if (!row.isNullAt(row.fieldIndex("txNewTbSum0xB883"))) row.getLong(row.fieldIndex("txNewTbSum0xB883")) else null
    val nr5gPhyULThroughputB881 : java.lang.Double = if (!row.isNullAt(row.fieldIndex("nr5gPhyULTputB881"))) row.getDouble(row.fieldIndex("nr5gPhyULTputB881")) else null

    if(nr5gPhyULThroughputB881!= null){
      val nr5gPhyULThroughputB881Mbps=BigDecimal(nr5gPhyULThroughputB881.toDouble * 8.0 / (1000000.0)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
      indexMap.put("NR5GPhyULThroughput",nr5gPhyULThroughputB881Mbps)
    }
    //    else if(nr5GPhyULThroughput!=null) {
    //      val nr5GPhyULThroughputMbps=BigDecimal(nr5GPhyULThroughput.toDouble * 8.0 / (1000000.0)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    //      indexMap.put("NR5GPhyULThroughput",nr5GPhyULThroughputMbps)
    //    }
    //txReTbSum0xB883,txTotalSum0xB883--------------------------------->>>>> ::0xB883 a/b 5G PHY UL Retransmission Rate
    val txReTbSum0xB883: java.lang.Long = if (!row.isNullAt(row.fieldIndex("txReTbSum0xB883"))) row.getLong(row.fieldIndex("txReTbSum0xB883")) else null
    val txTotalSum0xB883: java.lang.Long = if (!row.isNullAt(row.fieldIndex("txTotalSum0xB883"))) row.getLong(row.fieldIndex("txTotalSum0xB883")) else null
    val nr5gPhyULRetransRateB881: java.lang.Double = if (!row.isNullAt(row.fieldIndex("nr5gPhyULRetransRateB881"))) row.getDouble(row.fieldIndex("nr5gPhyULRetransRateB881")) else null
    val nr5gPhyUlRetransmissionRate: java.lang.Double=if(nr5gPhyULRetransRateB881 !=null && nr5gPhyULRetransRateB881>0) {
      BigDecimal(nr5gPhyULRetransRateB881).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    }
//     else if(txReTbSum0xB883!=null && txTotalSum0xB883!=null && txTotalSum0xB883!=0){
//      BigDecimal(txReTbSum0xB883.toDouble  / (txTotalSum0xB883.toDouble*1000000.0)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
//    }
    else{
      null
    }
   if(nr5gPhyUlRetransmissionRate!=null && nr5gPhyUlRetransmissionRate>0) {
      indexMap.put("NR5gPhyUlRetransmissionRate", nr5gPhyUlRetransmissionRate)
    }
   val nr5GPhyULInstTputPCC_0XB883: java.lang.Double = if (!row.isNullAt(row.fieldIndex("nr5GPhyULInstTputPCC_0XB883"))) row.getDouble(row.fieldIndex("nr5GPhyULInstTputPCC_0XB883")) else null
   val nr5GPhyULInstTputSCC1_0XB883: java.lang.Double = if (!row.isNullAt(row.fieldIndex("nr5GPhyULInstTputSCC1_0XB883"))) row.getDouble(row.fieldIndex("nr5GPhyULInstTputSCC1_0XB883")) else null
   val nr5GPhyULInstTput_0XB883: java.lang.Double = if (!row.isNullAt(row.fieldIndex("nr5GPhyULInstTput_0XB883"))) row.getDouble(row.fieldIndex("nr5GPhyULInstTput_0XB883")) else null
   val nr5GPhyULInstRetransRatePCC_0XB883: java.lang.Double = if (!row.isNullAt(row.fieldIndex("nr5GPhyULInstRetransRatePCC_0XB883"))) row.getDouble(row.fieldIndex("nr5GPhyULInstRetransRatePCC_0XB883")) else null
   val nr5GPhyULInstRetransRateSCC1_0XB883: java.lang.Double = if (!row.isNullAt(row.fieldIndex("nr5GPhyULInstRetransRateSCC1_0XB883"))) row.getDouble(row.fieldIndex("nr5GPhyULInstRetransRateSCC1_0XB883")) else null
   val nr5GPhyULInstRetransRate_0XB883: java.lang.Double = if (!row.isNullAt(row.fieldIndex("nr5GPhyULInstRetransRate_0XB883"))) row.getDouble(row.fieldIndex("nr5GPhyULInstRetransRate_0XB883")) else null
   if(nr5GPhyULInstTputPCC_0XB883 != null && nr5GPhyULInstTputPCC_0XB883>0) indexMap.put("NR5GPhyULInstTputPCC",nr5GPhyULInstTputPCC_0XB883)
   if(nr5GPhyULInstTputSCC1_0XB883 != null && nr5GPhyULInstTputSCC1_0XB883>0) indexMap.put("NR5GPhyULInstTputSCC1",nr5GPhyULInstTputSCC1_0XB883)
   if(nr5GPhyULInstTput_0XB883 != null && nr5GPhyULInstTput_0XB883>0) indexMap.put("NR5GPhyULInstTput",nr5GPhyULInstTput_0XB883)
   if(nr5GPhyULInstRetransRatePCC_0XB883 != null && nr5GPhyULInstRetransRatePCC_0XB883>0) indexMap.put("NR5GPhyULInstRetransRatePCC",nr5GPhyULInstRetransRatePCC_0XB883)
   if(nr5GPhyULInstRetransRateSCC1_0XB883 != null && nr5GPhyULInstRetransRateSCC1_0XB883>0) indexMap.put("NR5GPhyULInstRetransRateSCC1",nr5GPhyULInstRetransRateSCC1_0XB883)
   if(nr5GPhyULInstRetransRate_0XB883 != null && nr5GPhyULInstRetransRate_0XB883>0) indexMap.put("NR5GPhyULInstRetransRate",nr5GPhyULInstRetransRate_0XB883)

    val numNEW_TX: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numNEWTXSum0XB883"))) row.getLong(row.fieldIndex("numNEWTXSum0XB883")) else null
    val numRE_TX: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numRETXSum0XB883"))) row.getLong(row.fieldIndex("numRETXSum0XB883")) else null
    val numNEW_TXPCC: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numNEWTXPCCSum0XB883"))) row.getLong(row.fieldIndex("numNEWTXPCCSum0XB883")) else null
    val numNEW_TXSCC1: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numNEWTXSCC1Sum0XB883"))) row.getLong(row.fieldIndex("numNEWTXSCC1Sum0XB883")) else null
    val numRE_TXPCC: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numRETXPCCSum0XB883"))) row.getLong(row.fieldIndex("numRETXPCCSum0XB883")) else null
    val numRE_TXSCC1: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numRETXSCC1Sum0XB883"))) row.getLong(row.fieldIndex("numRETXSCC1Sum0XB883")) else null
    val numRE_TXRV_0: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numRETXRV0Sum0XB883"))) row.getLong(row.fieldIndex("numRETXRV0Sum0XB883")) else 0
    val numRE_TXRV_1: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numRETXRV1Sum0XB883"))) row.getLong(row.fieldIndex("numRETXRV1Sum0XB883")) else 0
    val numRE_TXRV_2: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numRETXRV2Sum0XB883"))) row.getLong(row.fieldIndex("numRETXRV2Sum0XB883")) else 0
    val numRE_TXRV_3: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numRETXRV3Sum0XB883"))) row.getLong(row.fieldIndex("numRETXRV3Sum0XB883")) else 0
    val list = List(numRE_TXRV_0, numRE_TXRV_1, numRE_TXRV_2, numRE_TXRV_3).sorted(Ordering[java.lang.Long].reverse)
    val numReTx_0: java.lang.Long = list.head
    val numReTx_1: java.lang.Long = list(1)
    val numReTx_2: java.lang.Long = list(2)
    val numReTx_3: java.lang.Long = list(3)

    val NRPUSCHWaveformPCC: String = if (!row.isNullAt(row.fieldIndex("NRPUSCHWaveformPCC"))) row.getString(row.fieldIndex("NRPUSCHWaveformPCC")) else null
    val NRPUSCHWaveformSCC1: String = if (!row.isNullAt(row.fieldIndex("NRPUSCHWaveformSCC1"))) row.getString(row.fieldIndex("NRPUSCHWaveformSCC1")) else null

    val nrMACULReTransRate_0XB883: java.lang.Double = if (numRE_TX != null && numNEW_TX != null && (numRE_TX + numNEW_TX) != 0) 100D * numRE_TX / (numRE_TX + numNEW_TX) else null
    //logger.info(s" evtTimestamp=$evtTimestamp ->    numRE_TX=$numRE_TX numNEW_TX=$numNEW_TX ->   nrMACULReTransRate_0XB883=$nrMACULReTransRate_0XB883")
    if (nrMACULReTransRate_0XB883 != null && nrMACULReTransRate_0XB883 > 0) indexMap.put("nrMACULReTransRate", BigDecimal(nrMACULReTransRate_0XB883).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue())
    val nrMACULReTransRatePCC_0XB883: java.lang.Double = if (numRE_TXPCC != null && numNEW_TXPCC != null && (numRE_TXPCC + numNEW_TXPCC) != 0) 100D * numRE_TXPCC / (numRE_TXPCC + numNEW_TXPCC) else null
    //logger.info(s" evtTimestamp=$evtTimestamp ->    numRE_TXPCC=$numRE_TXPCC numNEW_TXPCC=$numNEW_TXPCC ->   nrMACULReTransRatePCC_0XB883=$nrMACULReTransRatePCC_0XB883")
    if (nrMACULReTransRatePCC_0XB883 != null && nrMACULReTransRatePCC_0XB883 > 0) indexMap.put("nrMACULReTransRatePCC", BigDecimal(nrMACULReTransRatePCC_0XB883).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue())
    val nrMACULReTransRateSCC1_0XB883: java.lang.Double = if (numRE_TXSCC1 != null && numNEW_TXSCC1 != null && (numRE_TXSCC1 + numNEW_TXSCC1) != 0) 100D * numRE_TXSCC1 / (numRE_TXSCC1 + numNEW_TXSCC1) else null
    //logger.info(s" evtTimestamp=$evtTimestamp ->    numRE_TXSCC1=$numRE_TXSCC1 numNEW_TXSCC1=$numNEW_TXSCC1 ->   nrMACULReTransRateSCC1_0XB883=$nrMACULReTransRateSCC1_0XB883")
    if (nrMACULReTransRateSCC1_0XB883 != null && nrMACULReTransRateSCC1_0XB883 > 0) indexMap.put("nrMACULReTransRateSCC1", BigDecimal(nrMACULReTransRateSCC1_0XB883).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue())
    val nrMACUL1stReTransRate_0XB883: java.lang.Double = if (numReTx_0 != null && numNEW_TX != null && numNEW_TX != 0) Math.min(100D * numReTx_0 / numNEW_TX, 100) else null
    //logger.info(s" evtTimestamp=$evtTimestamp ->    numReTx_0=$numReTx_0 numNEW_TX=$numNEW_TX ->   nrMACUL1stReTransRate_0XB883=$nrMACUL1stReTransRate_0XB883")
    if (nrMACUL1stReTransRate_0XB883 != null && nrMACUL1stReTransRate_0XB883 > 0) indexMap.put("nrMACUL1stReTransRate", BigDecimal(nrMACUL1stReTransRate_0XB883).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue())
    val nrMACUL2ndReTransRate_0XB883: java.lang.Double = if (numReTx_1 != null && numReTx_0 != null && numReTx_0 != 0) Math.min(100D * numReTx_1 / numReTx_0, 100) else null
    //logger.info(s" evtTimestamp=$evtTimestamp ->    numReTx_1=$numReTx_1 numReTx_0=$numReTx_0 ->   nrMACUL2ndReTransRate_0XB883=$nrMACUL2ndReTransRate_0XB883")
    if (nrMACUL2ndReTransRate_0XB883 != null && nrMACUL2ndReTransRate_0XB883 > 0) indexMap.put("nrMACUL2ndReTransRate", BigDecimal(nrMACUL2ndReTransRate_0XB883).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue())
    val nrMACUL3ndReTransRate_0XB883: java.lang.Double = if (numReTx_2 != null && numReTx_1 != null && numReTx_1 != 0) Math.min(100D * numReTx_2 / numReTx_1, 100) else null
    //logger.info(s" evtTimestamp=$evtTimestamp ->    numReTx_2=$numReTx_2 numReTx_1=$numReTx_1 ->   nrMACUL3ndReTransRate_0XB883=$nrMACUL3ndReTransRate_0XB883")
    if (nrMACUL3ndReTransRate_0XB883 != null && nrMACUL3ndReTransRate_0XB883 > 0) indexMap.put("nrMACUL3rdReTransRate", BigDecimal(nrMACUL3ndReTransRate_0XB883).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue())
    val nrMACUL4ndReTransRate_0XB883: java.lang.Double = if (numReTx_3 != null && numReTx_2 != null && numReTx_2 != 0) Math.min(100D * numReTx_3 / numReTx_2, 100) else null
    //logger.info(s" evtTimestamp=$evtTimestamp ->    numReTx_3=$numReTx_3 numReTx_2=$numReTx_2 ->   nrMACUL4ndReTransRate_0XB883=$nrMACUL4ndReTransRate_0XB883")
    if (nrMACUL4ndReTransRate_0XB883 != null && nrMACUL4ndReTransRate_0XB883 > 0) indexMap.put("nrMACUL4thReTransRate", BigDecimal(nrMACUL4ndReTransRate_0XB883).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue())
    //logger.info(s"indexMap = $indexMap")
    if (NRPUSCHWaveformPCC != null) indexMap.put("NRPUSCHWaveformPCC", NRPUSCHWaveformPCC)
    if (NRPUSCHWaveformSCC1 != null) indexMap.put("NRPUSCHWaveformSCC1", NRPUSCHWaveformSCC1)

    indexMap
  }

  def extract5GScgBlerKpis(row: org.apache.spark.sql.Row): scala.collection.mutable.Map[String, Any] = {
    val indexMap: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()
    //carrieridcsv_0xb887,crcstatuscsv_0xb887
    val carrieridcsv_0xb887: java.lang.String = if (!row.isNullAt(row.fieldIndex("carrieridcsv_0xb887"))) row.getString(row.fieldIndex("carrieridcsv_0xb887")) else null
    val crcstatuscsv_0xb887: java.lang.String = if (!row.isNullAt(row.fieldIndex("crcstatuscsv_0xb887"))) row.getString(row.fieldIndex("crcstatuscsv_0xb887")) else null

    //NR5gDlScgBlerPcc float,NR5gDlScgBlerScc1 float,NR5gDlScgBlerScc2 float,NR5gDlScgBlerScc3 float,NR5gDlScgBlerScc4 float,NR5gDlScgBlerScc5 float,NR5gDlScgBlerScc6 float,NR5gDlScgBlerScc7 float
    if (carrieridcsv_0xb887 != null && crcstatuscsv_0xb887 != null) {

      val carrierIds = carrieridcsv_0xb887.split(",").toList
      val crcStatus = crcstatuscsv_0xb887.split(",").toList
      if (carrierIds.size > 0 && crcStatus.size > 0) {
        val carrierCrcStatusTups = carrierIds.zip(crcStatus)
        val carCrcMap = carrierCrcStatusTups.groupBy(tup => tup._1)

        carCrcMap.foreach((carStateTup: (String, List[(String, String)])) => {
          val carrierId = carStateTup._1

          val crcStatusList = carStateTup._2.map(tup => tup._2)

          val numCountCarrierFails=crcStatusList.count(_.equalsIgnoreCase("FAIL"))
          if (numCountCarrierFails > 0) {
            val totalCarrierStatusCounts = crcStatusList.size
            val nr5GDlScgBler = BigDecimal((numCountCarrierFails.toDouble * 100.0) / totalCarrierStatusCounts.toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
            if (Constants.CARRIER_ID_TYPE_MAP.contains(carrierId)) {
              val kpiName = "NR5gDlScgBler" + Constants.CARRIER_ID_TYPE_MAP(carrierId)
              indexMap.put(kpiName, nr5GDlScgBler)
            }

          }
        })
      }
    }
    indexMap
  }

  def extract5GBeamKpis(row: org.apache.spark.sql.Row): scala.collection.mutable.Map[String, Any] = {
    val nrServingBeamIndex = if (!row.isNullAt(row.fieldIndex("Serving_NRServingBeamIndex"))) row.getString(row.fieldIndex("Serving_NRServingBeamIndex")) else null


    val nrServingBeamIndEvents: (java.lang.Boolean, String, java.lang.Integer) = if (nrServingBeamIndex != null) {
      val nrServingBeamIndexList: List[String] = nrServingBeamIndex.trim.replaceAll(",+", ",").split(",").map(_.trim).toList
      val filteredNrServingBeamIndexList = nrServingBeamIndexList.filter(x => x != null && !x.equals(""))
      val distinctIndexes = compress(filteredNrServingBeamIndexList)
      val beamIndexChangedEvent: java.lang.Boolean = if (distinctIndexes.size > 1) true else null
      //"56->45->36"
      val nrServingBeamIndexTransition: String = if (distinctIndexes.size > 1) {
        distinctIndexes.mkString("->")
      } else null
      val numBeamSwitch: java.lang.Integer = if (distinctIndexes.size > 1) distinctIndexes.size else null
      (beamIndexChangedEvent, nrServingBeamIndexTransition, numBeamSwitch)
    } else null

    val rsrp10xB891: java.lang.Long = if (!row.isNullAt(row.fieldIndex("rsrp1_0xB891"))) row.getString(row.fieldIndex("rsrp1_0xB891")).toLong else null
    val bestReportBeamRsrp: java.lang.Integer = if (rsrp10xB891 != null) rsrp10xB891.toInt - 156 else null
    val beamIndexChangedEvent: java.lang.Boolean = if (nrServingBeamIndEvents != null) nrServingBeamIndEvents._1 else null
    val nrServingBeamIndexTransition: String = if (nrServingBeamIndEvents != null) nrServingBeamIndEvents._2 else null
    val numBeamSwitch: java.lang.Integer = if (nrServingBeamIndEvents != null) nrServingBeamIndEvents._3 else null
    val indexMap: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()
    if (beamIndexChangedEvent != null) indexMap.put("NRBeamSwitchEvent0xB975", beamIndexChangedEvent.toString)
    if (nrServingBeamIndexTransition != null) indexMap.put("NRBeamTransition0xB975", nrServingBeamIndexTransition.toString)
    if (numBeamSwitch != null) indexMap.put("NRBeamSwitchCount", numBeamSwitch)
    if (bestReportBeamRsrp != null) indexMap.put("bestReportBeamRsrp", bestReportBeamRsrp)

    indexMap
  }

  def extractRachAndBeamKpis(row: org.apache.spark.sql.Row, commonConfigParams:CommonConfigParameters): scala.collection.mutable.Map[String,Any] = {
    // val rachResult = if (!row.isNullAt(row.fieldIndex("rachResult_b88a"))) row.getString(row.fieldIndex("rachResult_b88a")) else null
    val rachResult = if (!row.isNullAt(row.fieldIndex("rachResult_0xb88a"))) row.getString(row.fieldIndex("rachResult_0xb88a")) else null
    var successEvent: java.lang.Integer = null
    var failureEvent: java.lang.Integer = null
    var abortedEvent: java.lang.Integer = null
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
    if (rachResult != null && !rachResult.trim().equals("")) {
      if (rachResult.contains("SUCCESS")) {
        successEvent = 1
      }
      if (rachResult.contains("FAILURE")) {
        failureEvent = 1
      }
      if (rachResult.contains("ABORTED")) {
        abortedEvent = 1
        //        var evtInfo:EventInfofromEs = EventInfofromEs()
        //        val evtTimestamp = getTimestampPG(row.getString(row.fieldIndex("dateWithoutMillis"))).getOrElse().asInstanceOf[Timestamp]
        //        evtInfo = evtInfo.copy(testId = row.getInt(row.fieldIndex("testId")),fileName = Constants.getDlfFromSeq(row.get(row.fieldIndex("fileName")).toString), evtTimestamp, nrRachAbortEvt =1, fileLocation = row.getString(row.fieldIndex("fileLocation")))
        //       // CommonDaoImpl.insertEventsInfoWhileES(commonConfigParams,evtInfo)
        //        CommonDaoImpl.updateEventsCountWhileES(commonConfigParams,evtInfo)
      }
    }

    val rachLatency: lang.Long = if (!row.isNullAt(row.fieldIndex("avgRachLatencyperSecond"))) row.getDouble(row.fieldIndex("avgRachLatencyperSecond")).toLong else null


    //rachLatency,rachResult_0xb88a,Serving_NRServingBeamIndex,rsrp10xB891,rachResult_b88a
    val nrServingBeamIndex = if (!row.isNullAt(row.fieldIndex("Serving_NRServingBeamIndex"))) row.getString(row.fieldIndex("Serving_NRServingBeamIndex")) else null


    val nrServingBeamIndEvents: (java.lang.Integer, String, java.lang.Integer) = if (nrServingBeamIndex != null) {
      val nrServingBeamIndexList: List[String] = nrServingBeamIndex.trim.replaceAll(",+", ",").split(",").map(_.trim).toList
      val filteredNrServingBeamIndexList = nrServingBeamIndexList.filter(x => x != null && !x.equals(""))
      val distinctIndexes = compress(filteredNrServingBeamIndexList)
      val beamIndexChangedEvent: java.lang.Integer = if (distinctIndexes.size > 1) 1 else null
      //"56->45->36"
      val nrServingBeamIndexTransition: String = if (distinctIndexes.size > 1) {
        distinctIndexes.mkString("->")
      } else null
      val numBeamSwitch: java.lang.Integer = if (distinctIndexes.size > 1) distinctIndexes.size else null
      (beamIndexChangedEvent, nrServingBeamIndexTransition, numBeamSwitch)
    } else null

    val rsrp10xB891: java.lang.Long = if (!row.isNullAt(row.fieldIndex("rsrp1_0xB891"))) row.getString(row.fieldIndex("rsrp1_0xB891")).toLong else null
    val bestReportBeamRsrp: java.lang.Integer = if (rsrp10xB891 != null) rsrp10xB891.toInt - 156 else null

    //rachResult_b88a,Serving_NRServingBeamIndex
    val rachResultOneSec = if (!row.isNullAt(row.fieldIndex("rachResult_b88a"))) row.getString(row.fieldIndex("rachResult_b88a")) else null
    //if (rachResultOneSec != null) logger.info("rachResultOneSec >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + rachResultOneSec)
    val rachResultCounts = if (rachResultOneSec != null && rachResultOneSec.trim.length != 0) {
      val rachResultList: List[String] = rachResultOneSec.trim.replaceAll(",+", ",").split(",").map(_.trim).toList
      val filteredRachResultList = rachResultList.filter(rachRes => {
        rachRes.toUpperCase().contains("SUCCESS") || rachRes.toUpperCase().contains("ABORTED") || rachRes.toUpperCase().contains("FAIL")
      })
      val rachEventCounts = filteredRachResultList.map((rach: String) =>
        if (rach.equalsIgnoreCase("SUCCESS") || rach.equalsIgnoreCase("ABORTED"))
          {
            rach.toUpperCase
          } else{
              "FAILURE"
        }
      ).groupBy(str => str).map(tup => (tup._1, tup._2.size)).withDefaultValue(0)

      rachEventCounts
    } else {
      null
    }

    //totalRachSuccessEvents int,totalRachFailureEvents int,totalRachAbortedEvents int ,rachSuccessRate double,rachTotalEvents int,rachLatency long
    var totalRachSuccessEvents: java.lang.Integer = null
    var totalRachFailureEvents: java.lang.Integer = null
    var totalRachAbortedEvents: java.lang.Integer = null
    var rachSuccessRate: java.lang.Double = null
    var rachTotalEvents: java.lang.Long = null
    var totalEventCount: java.lang.Integer = null

    if (rachResultCounts != null) {


      totalEventCount = rachResultCounts.map(tup => tup._2).sum
      totalRachSuccessEvents = new java.lang.Integer(rachResultCounts.getOrElse("SUCCESS", 0))
      totalRachFailureEvents = new java.lang.Integer(rachResultCounts.getOrElse("FAILURE", 0))
      totalRachAbortedEvents = new java.lang.Integer(rachResultCounts.getOrElse("ABORTED", 0))
      rachSuccessRate = if (totalEventCount > 0) (BigDecimal(rachResultCounts.get("SUCCESS").getOrElse(0).toDouble * 100 / totalEventCount).setScale(2, BigDecimal.RoundingMode.HALF_UP)).toDouble else null
    }

    //(successEvent,failureEvent,abortedEvent)
    /* ,nrServingBeamIndEvents._1,nrServingBeamIndEvents._2,nrServingBeamIndEvents._3,
    bestReportBeamRsrp,totalEventCount,totalRachSuccessEvents,totalRachFailureEvents,totalRachAbortedEvents,rachSuccessRate,rachLatency)*/
    val beamIndexChangedEvent: java.lang.Integer = if (nrServingBeamIndEvents != null) nrServingBeamIndEvents._1 else null
    val nrServingBeamIndexTransition: String = if (nrServingBeamIndEvents != null) nrServingBeamIndEvents._2 else null
    val numBeamSwitch: java.lang.Integer = if (nrServingBeamIndEvents != null) nrServingBeamIndEvents._3 else null

    val indexMap: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()
    if (beamIndexChangedEvent != null) indexMap.put("NRBeamSwitchEvt", beamIndexChangedEvent)
    if (nrServingBeamIndexTransition != null) indexMap.put("NRBeamTransition0xB975", nrServingBeamIndexTransition.toString)
    if (numBeamSwitch != null) indexMap.put("NRBeamSwitchCount", numBeamSwitch)
    if (bestReportBeamRsrp != null) indexMap.put("bestReportBeamRsrp", bestReportBeamRsrp)
    if (successEvent != null) indexMap.put("NRRachSuccessEvt", successEvent)
    if (failureEvent != null) indexMap.put("NRRachFailureEvt", failureEvent)
    if (abortedEvent != null) indexMap.put("NRRachAbortEvt", abortedEvent)
    if (rachResultCounts != null) indexMap.put("rachTotalEvents", totalEventCount)
    if (rachResultCounts != null) indexMap.put("totalRachSuccessEvents", totalRachSuccessEvents)
    if (rachResultCounts != null) indexMap.put("totalRachAbortedEvents", totalRachAbortedEvents)
    if (rachResultCounts != null && rachSuccessRate != null) indexMap.put("rachSuccessRate", rachSuccessRate)
    if (rachLatency != null) indexMap.put("rachLatency", rachLatency.toInt)

    //NRBeamSwitchEvent0xB975 ----String (true or false), NRBeamTransition0xB975, NRBeamSwitchCount
    indexMap
  }

  def get5gsPCellKPIs(row: org.apache.spark.sql.Row): scala.collection.mutable.Map[String, Any] = {
    val indexMap: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()

    val spCellBandDL: Integer = if (!row.isNullAt(row.fieldIndex("spCellBandDL_deriv"))) row.getInt(row.fieldIndex("spCellBandDL_deriv")) else null
    val spCellNRARFCNDL: Integer = if (!row.isNullAt(row.fieldIndex("spCellNRARFCNDL_deriv"))) row.getInt(row.fieldIndex("spCellNRARFCNDL_deriv")) else null
    val spCellSubCarSpaceDL: Integer = if (!row.isNullAt(row.fieldIndex("spCellSubCarSpaceDL_deriv"))) row.getString(row.fieldIndex("spCellSubCarSpaceDL_deriv")).toLowerCase().split("khz")(1).toInt else null
    val spCellNumPRBDL: Integer = if (!row.isNullAt(row.fieldIndex("spCellNumPRBDL_deriv"))) row.getInt(row.fieldIndex("spCellNumPRBDL_deriv")) else null
    val spCellBandwidthDL: Integer = if (!row.isNullAt(row.fieldIndex("spCellBandwidthDL_deriv"))) row.getString(row.fieldIndex("spCellBandwidthDL_deriv")).split("MHz")(0).toInt else null
    val spCellDuplexMode: String = if (!row.isNullAt(row.fieldIndex("spCellDuplexMode_deriv"))) row.getString(row.fieldIndex("spCellDuplexMode_deriv")) else null
    val spCellTypeDL: String = if (!row.isNullAt(row.fieldIndex("spCellTypeDL_deriv"))) row.getString(row.fieldIndex("spCellTypeDL_deriv")) else null


    val spCellBandUL: Integer = if (!row.isNullAt(row.fieldIndex("spCellBandUL_deriv"))) row.getInt(row.fieldIndex("spCellBandUL_deriv")) else null
    val spCellNRARFCNUL: Integer = if (!row.isNullAt(row.fieldIndex("spCellNRARFCNUL_deriv"))) row.getInt(row.fieldIndex("spCellNRARFCNUL_deriv")) else null
    val spCellSubCarSpaceUL: Integer = if (!row.isNullAt(row.fieldIndex("spCellSubCarSpaceUL_deriv"))) row.getString(row.fieldIndex("spCellSubCarSpaceUL_deriv")).toLowerCase().split("khz")(1).toInt else null
    val spCellNumPRBUL: Integer = if (!row.isNullAt(row.fieldIndex("spCellNumPRBUL_deriv"))) row.getInt(row.fieldIndex("spCellNumPRBUL_deriv")) else null
    val spCellBandwidthUL: Integer = if (!row.isNullAt(row.fieldIndex("spCellBandwidthUL_deriv"))) row.getString(row.fieldIndex("spCellBandwidthUL_deriv")).split("MHz")(0).toInt else null
    val spCellTypeUL: String = if (!row.isNullAt(row.fieldIndex("spCellTypeUL_deriv"))) row.getString(row.fieldIndex("spCellTypeUL_deriv")) else null

    if (spCellBandDL != null) indexMap.put("spCellBandDL", spCellBandDL.toInt)
    if (spCellNRARFCNDL != null) indexMap.put("spCellNRARFCNDL", spCellNRARFCNDL.toInt)
    if (spCellSubCarSpaceDL != null) indexMap.put("spCellSubcarrierSpaceDL", spCellSubCarSpaceDL.toInt)
    if (spCellNumPRBDL != null) indexMap.put("spCellNumPRBDL", spCellNumPRBDL.toInt)
    if (spCellBandwidthDL != null) indexMap.put("spCellBandwidthDL", spCellBandwidthDL.toInt)
    if (spCellTypeDL != null) indexMap.put("sPCellCellType", spCellTypeDL.toString)

    if (spCellDuplexMode != null) indexMap.put("spCellDuplexMode", spCellDuplexMode.toString)

  if(spCellBandUL!=null && spCellDuplexMode != "TDD") { indexMap.put("spCellBandUL",spCellBandUL.toInt) } else if(spCellBandDL!=null && spCellDuplexMode == "TDD") { indexMap.put("spCellBandUL",spCellBandDL.toInt) }
  if(spCellNRARFCNUL!=null && spCellDuplexMode != "TDD") { indexMap.put("spCellNRARFCNUL",spCellNRARFCNUL.toInt) } else if(spCellNRARFCNDL!=null && spCellDuplexMode == "TDD") { indexMap.put("spCellNRARFCNUL",spCellNRARFCNDL.toInt) }
  if(spCellSubCarSpaceUL!=null && spCellDuplexMode != "TDD") { indexMap.put("spCellSubcarrierSpaceUL",spCellSubCarSpaceUL.toInt) } else if(spCellSubCarSpaceDL!=null && spCellDuplexMode == "TDD") indexMap.put("spCellSubcarrierSpaceUL",spCellSubCarSpaceDL.toInt)
  if(spCellNumPRBUL!=null && spCellDuplexMode != "TDD") { indexMap.put("spCellNumPRBUL",spCellNumPRBUL.toInt) } else if (spCellNumPRBDL!=null && spCellDuplexMode == "TDD") indexMap.put("spCellNumPRBUL",spCellNumPRBDL.toInt)
  if(spCellBandwidthUL!=null && spCellDuplexMode != "TDD") { indexMap.put("spCellBandwidthUL",spCellBandwidthUL.toInt) } else if(spCellBandwidthDL!=null && spCellDuplexMode == "TDD") indexMap.put("spCellBandwidthUL",spCellBandwidthDL.toInt)
  if(spCellTypeUL!=null && spCellTypeUL != "TDD") { indexMap.put("spCellTypeUL",spCellTypeUL.toString) } else if(spCellTypeDL!=null && spCellDuplexMode == "TDD") indexMap.put("spCellTypeUL",spCellTypeDL.toString)
  indexMap
    }

  def getServingBeam0xb97fKpis(row: org.apache.spark.sql.Row, rrcstate:Boolean): scala.collection.mutable.Map[String,Any] = {

    val indexMap: scala.collection.mutable.Map[String,Any] = scala.collection.mutable.Map()
    val Nrs_brsrp_B97F: List[String] = if (!row.isNullAt(row.fieldIndex("nrsbrsrp_0xb97f"))) row.getSeq(row.fieldIndex("nrsbrsrp_0xb97f")).toList else null
    val Nrs_ccindex_B97F: List[Integer] = if (!row.isNullAt(row.fieldIndex("ccindex_0xb97f"))) row.getSeq(row.fieldIndex("ccindex_0xb97f")).toList else null
    val Nrs_brsrq_B97F: List[String] = if (!row.isNullAt(row.fieldIndex("nrsbrsq_0xb97f"))) row.getSeq(row.fieldIndex("nrsbrsq_0xb97f")).toList else null
    val Nrs_beamcount_B97F: List[Integer] = if (!row.isNullAt(row.fieldIndex("nrbeamcount_0xb97f"))) row.getSeq(row.fieldIndex("nrbeamcount_0xb97f")).toList else null
    val Nrs_pci_B97F: List[Integer] = if (!row.isNullAt(row.fieldIndex("nrpci_0xb97f"))) row.getSeq(row.fieldIndex("nrpci_0xb97f")).toList else null
    val  Nrs_servingbeamindex_B97F: Integer = if (!row.isNullAt(row.fieldIndex("nrservingbeamindex_0xb97f"))) row.getInt(row.fieldIndex("nrservingbeamindex_0xb97f")) else null
    val Nrs_servingpci_B97F: List[Integer] = if (!row.isNullAt(row.fieldIndex("nrservingcellpci_0xb97f"))) row.getSeq(row.fieldIndex("nrservingcellpci_0xb97f")).toList else null
    val LTE_RRC_State: String = if (!row.isNullAt(row.fieldIndex("lteRrcState"))) row.getString(row.fieldIndex("lteRrcState")) else null

    //ONP-356 5G SCC RSRP/RSRQ and Serving RX Beam
    //logger.info("Adding 0xB97F KPIs to map")
    /*
        val nrServingPCCPCI_0xB97F: List[Int] = if (!row.isNullAt(row.fieldIndex("NRServingPCCPCI_0xB97F"))) row.getSeq(row.fieldIndex("NRServingPCCPCI_0xB97F")).toList else null
        val nrServingSCC1PCI_0xB97F: List[Int] = if (!row.isNullAt(row.fieldIndex("NRServingSCC1PCI_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC1PCI_0xB97F")).toList else null
        val nrServingSCC2PCI_0xB97F: List[Int] = if (!row.isNullAt(row.fieldIndex("NRServingSCC2PCI_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC2PCI_0xB97F")).toList else null
        val nrServingSCC3PCI_0xB97F: List[Int] = if (!row.isNullAt(row.fieldIndex("NRServingSCC3PCI_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC3PCI_0xB97F")).toList else null
        val nrServingSCC4PCI_0xB97F: List[Int] = if (!row.isNullAt(row.fieldIndex("NRServingSCC4PCI_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC4PCI_0xB97F")).toList else null
        val nrServingSCC5PCI_0xB97F: List[Int] = if (!row.isNullAt(row.fieldIndex("NRServingSCC5PCI_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC5PCI_0xB97F")).toList else null
        val nrServingPCCRxBeamId0_0xB97F: Integer = if (!row.isNullAt(row.fieldIndex("NRServingPCCRxBeamId0_0xB97F"))) row.getInt(row.fieldIndex("NRServingPCCRxBeamId0_0xB97F")) else null
        val nrServingSCC1RxBeamId0_0xB97F: Integer = if (!row.isNullAt(row.fieldIndex("NRServingSCC1RxBeamId0_0xB97F"))) row.getInt(row.fieldIndex("NRServingSCC1RxBeamId0_0xB97F")) else null
        val nrServingSCC2RxBeamId0_0xB97F: Integer = if (!row.isNullAt(row.fieldIndex("NRServingSCC2RxBeamId0_0xB97F"))) row.getInt(row.fieldIndex("NRServingSCC2RxBeamId0_0xB97F")) else null
        val nrServingSCC3RxBeamId0_0xB97F: Integer = if (!row.isNullAt(row.fieldIndex("NRServingSCC3RxBeamId0_0xB97F"))) row.getInt(row.fieldIndex("NRServingSCC3RxBeamId0_0xB97F")) else null
        val nrServingSCC4RxBeamId0_0xB97F: Integer = if (!row.isNullAt(row.fieldIndex("NRServingSCC4RxBeamId0_0xB97F"))) row.getInt(row.fieldIndex("NRServingSCC4RxBeamId0_0xB97F")) else null
        val nrServingSCC5RxBeamId0_0xB97F: Integer = if (!row.isNullAt(row.fieldIndex("NRServingSCC5RxBeamId0_0xB97F"))) row.getInt(row.fieldIndex("NRServingSCC5RxBeamId0_0xB97F")) else null
        val nrServingPCCRxBeamId1_0xB97F: Integer = if (!row.isNullAt(row.fieldIndex("NRServingPCCRxBeamId1_0xB97F"))) row.getInt(row.fieldIndex("NRServingPCCRxBeamId1_0xB97F")) else null
        val nrServingSCC1RxBeamId1_0xB97F: Integer = if (!row.isNullAt(row.fieldIndex("NRServingSCC1RxBeamId1_0xB97F"))) row.getInt(row.fieldIndex("NRServingSCC1RxBeamId1_0xB97F")) else null
        val nrServingSCC2RxBeamId1_0xB97F: Integer = if (!row.isNullAt(row.fieldIndex("NRServingSCC2RxBeamId1_0xB97F"))) row.getInt(row.fieldIndex("NRServingSCC2RxBeamId1_0xB97F")) else null
        val nrServingSCC3RxBeamId1_0xB97F: Integer = if (!row.isNullAt(row.fieldIndex("NRServingSCC3RxBeamId1_0xB97F"))) row.getInt(row.fieldIndex("NRServingSCC3RxBeamId1_0xB97F")) else null
        val nrServingSCC4RxBeamId1_0xB97F: Integer = if (!row.isNullAt(row.fieldIndex("NRServingSCC4RxBeamId1_0xB97F"))) row.getInt(row.fieldIndex("NRServingSCC4RxBeamId1_0xB97F")) else null
        val nrServingSCC5RxBeamId1_0xB97F: Integer = if (!row.isNullAt(row.fieldIndex("NRServingSCC5RxBeamId1_0xB97F"))) row.getInt(row.fieldIndex("NRServingSCC5RxBeamId1_0xB97F")) else null
        val nrServingPCCRxBeamRSRP0_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingPCCRxBeamRSRP0_0xB97F"))) row.getSeq(row.fieldIndex("NRServingPCCRxBeamRSRP0_0xB97F")).toList else null
        val nrServingSCC1RxBeamRSRP0_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC1RxBeamRSRP0_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC1RxBeamRSRP0_0xB97F")).toList else null
        val nrServingSCC2RxBeamRSRP0_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC2RxBeamRSRP0_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC2RxBeamRSRP0_0xB97F")).toList else null
        val nrServingSCC3RxBeamRSRP0_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC3RxBeamRSRP0_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC3RxBeamRSRP0_0xB97F")).toList else null
        val nrServingSCC4RxBeamRSRP0_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC4RxBeamRSRP0_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC4RxBeamRSRP0_0xB97F")).toList else null
        val nrServingSCC5RxBeamRSRP0_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC5RxBeamRSRP0_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC5RxBeamRSRP0_0xB97F")).toList else null
        val nrServingPCCRxBeamRSRP1_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingPCCRxBeamRSRP1_0xB97F"))) row.getSeq(row.fieldIndex("NRServingPCCRxBeamRSRP1_0xB97F")).toList else null
        val nrServingSCC1RxBeamRSRP1_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC1RxBeamRSRP1_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC1RxBeamRSRP1_0xB97F")).toList else null
        val nrServingSCC2RxBeamRSRP1_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC2RxBeamRSRP1_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC2RxBeamRSRP1_0xB97F")).toList else null
        val nrServingSCC3RxBeamRSRP1_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC3RxBeamRSRP1_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC3RxBeamRSRP1_0xB97F")).toList else null
        val nrServingSCC4RxBeamRSRP1_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC4RxBeamRSRP1_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC4RxBeamRSRP1_0xB97F")).toList else null
        val nrServingSCC5RxBeamRSRP1_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC5RxBeamRSRP1_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC5RxBeamRSRP1_0xB97F")).toList else null
        val nrServingPCCRSRP_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingPCCRSRP_0xB97F"))) row.getSeq(row.fieldIndex("NRServingPCCRSRP_0xB97F")).toList else null
        val nrServingSCC1RSRP_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC1RSRP_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC1RSRP_0xB97F")).toList else null
        val nrServingSCC2RSRP_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC2RSRP_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC2RSRP_0xB97F")).toList else null
        val nrServingSCC3RSRP_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC3RSRP_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC3RSRP_0xB97F")).toList else null
        val nrServingSCC4RSRP_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC4RSRP_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC4RSRP_0xB97F")).toList else null
        val nrServingSCC5RSRP_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC5RSRP_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC5RSRP_0xB97F")).toList else null
        val nrServingPCCRSRQ_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingPCCRSRQ_0xB97F"))) row.getSeq(row.fieldIndex("NRServingPCCRSRQ_0xB97F")).toList else null
        val nrServingSCC1RSRQ_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC1RSRQ_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC1RSRQ_0xB97F")).toList else null
        val nrServingSCC2RSRQ_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC2RSRQ_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC2RSRQ_0xB97F")).toList else null
        val nrServingSCC3RSRQ_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC3RSRQ_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC3RSRQ_0xB97F")).toList else null
        val nrServingSCC4RSRQ_0xB97F: List[Float] = if (!row.isNullAt(row.fieldIndex("NRServingSCC4RSRQ_0xB97F"))) row.getSeq(row.fieldIndex("NRServingSCC4RSRQ_0xB97F")).toList else null
        if (nrServingSCC5RSRQ_0xB97F != null && nrServingSCC5RSRQ_0xB97F.nonEmpty) indexMap.put("NRServingSCC5RSRQ", nrServingSCC5RSRQ_0xB97F.head)*/

    //    logger.info("nrServingPCCPCI_0xB97F" + nrServingPCCPCI_0xB97F)
    //    logger.info("nrServingPCCRxBeamId0_0xB97F" + nrServingPCCRxBeamId0_0xB97F)
    //    logger.info("nrServingPCCRxBeamId1_0xB97F" + nrServingPCCRxBeamId1_0xB97F)
    //    logger.info("nrServingPCCRxBeamRSRP0_0xB97F" + nrServingPCCRxBeamRSRP0_0xB97F)
    //    logger.info("nrServingPCCRxBeamRSRP1_0xB97F" + nrServingPCCRxBeamRSRP1_0xB97F)
    //    logger.info("nrServingPCCRSRP_0xB97F" + nrServingPCCRSRP_0xB97F)
    //    logger.info("nrServingPCCRSRQ_0xB97F" + nrServingPCCRSRQ_0xB97F)
    /* if (nrServingPCCPCI_0xB97F != null && nrServingPCCPCI_0xB97F.nonEmpty) indexMap.put("NRServingPCCPCI", nrServingPCCPCI_0xB97F.head)
     if (nrServingSCC1PCI_0xB97F != null && nrServingSCC1PCI_0xB97F.nonEmpty) indexMap.put("NRServingSCC1PCI", nrServingSCC1PCI_0xB97F.head)
     if (nrServingSCC2PCI_0xB97F != null && nrServingSCC2PCI_0xB97F.nonEmpty) indexMap.put("NRServingSCC2PCI", nrServingSCC2PCI_0xB97F.head)
     if (nrServingSCC3PCI_0xB97F != null && nrServingSCC3PCI_0xB97F.nonEmpty) indexMap.put("NRServingSCC3PCI", nrServingSCC3PCI_0xB97F.head)
     if (nrServingSCC4PCI_0xB97F != null && nrServingSCC4PCI_0xB97F.nonEmpty) indexMap.put("NRServingSCC4PCI", nrServingSCC4PCI_0xB97F.head)
     if (nrServingSCC5PCI_0xB97F != null && nrServingSCC5PCI_0xB97F.nonEmpty) indexMap.put("NRServingSCC5PCI", nrServingSCC5PCI_0xB97F.head)
     if (nrServingPCCRxBeamId0_0xB97F != null) indexMap.put("NRServingPCCRxBeamId0", nrServingPCCRxBeamId0_0xB97F)
     if (nrServingSCC1RxBeamId0_0xB97F != null) indexMap.put("NRServingSCC1RxBeamId0", nrServingSCC1RxBeamId0_0xB97F)
     if (nrServingSCC2RxBeamId0_0xB97F != null) indexMap.put("NRServingSCC2RxBeamId0", nrServingSCC2RxBeamId0_0xB97F)
     if (nrServingSCC3RxBeamId0_0xB97F != null) indexMap.put("NRServingSCC3RxBeamId0", nrServingSCC3RxBeamId0_0xB97F)
     if (nrServingSCC4RxBeamId0_0xB97F != null) indexMap.put("NRServingSCC4RxBeamId0", nrServingSCC4RxBeamId0_0xB97F)
     if (nrServingSCC5RxBeamId0_0xB97F != null) indexMap.put("NRServingSCC5RxBeamId0", nrServingSCC5RxBeamId0_0xB97F)
     if (nrServingPCCRxBeamId1_0xB97F != null) indexMap.put("NRServingPCCRxBeamId1", nrServingPCCRxBeamId1_0xB97F)
     if (nrServingSCC1RxBeamId1_0xB97F != null) indexMap.put("NRServingSCC1RxBeamId1", nrServingSCC1RxBeamId1_0xB97F)
     if (nrServingSCC2RxBeamId1_0xB97F != null) indexMap.put("NRServingSCC2RxBeamId1", nrServingSCC2RxBeamId1_0xB97F)
     if (nrServingSCC3RxBeamId1_0xB97F != null) indexMap.put("NRServingSCC3RxBeamId1", nrServingSCC3RxBeamId1_0xB97F)
     if (nrServingSCC4RxBeamId1_0xB97F != null) indexMap.put("NRServingSCC4RxBeamId1", nrServingSCC4RxBeamId1_0xB97F)
     if (nrServingSCC5RxBeamId1_0xB97F != null) indexMap.put("NRServingSCC5RxBeamId1", nrServingSCC5RxBeamId1_0xB97F)
     if (nrServingPCCRxBeamRSRP0_0xB97F != null && nrServingPCCRxBeamRSRP0_0xB97F.nonEmpty) indexMap.put("NRServingPCCRxBeamRSRP0", nrServingPCCRxBeamRSRP0_0xB97F.head)
     if (nrServingSCC1RxBeamRSRP0_0xB97F != null && nrServingSCC1RxBeamRSRP0_0xB97F.nonEmpty) indexMap.put("NRServingSCC1RxBeamRSRP0", nrServingSCC1RxBeamRSRP0_0xB97F.head)
     if (nrServingSCC2RxBeamRSRP0_0xB97F != null && nrServingSCC2RxBeamRSRP0_0xB97F.nonEmpty) indexMap.put("NRServingSCC2RxBeamRSRP0", nrServingSCC2RxBeamRSRP0_0xB97F.head)
     if (nrServingSCC3RxBeamRSRP0_0xB97F != null && nrServingSCC3RxBeamRSRP0_0xB97F.nonEmpty) indexMap.put("NRServingSCC3RxBeamRSRP0", nrServingSCC3RxBeamRSRP0_0xB97F.head)
     if (nrServingSCC4RxBeamRSRP0_0xB97F != null && nrServingSCC4RxBeamRSRP0_0xB97F.nonEmpty) indexMap.put("NRServingSCC4RxBeamRSRP0", nrServingSCC4RxBeamRSRP0_0xB97F.head)
     if (nrServingSCC5RxBeamRSRP0_0xB97F != null && nrServingSCC5RxBeamRSRP0_0xB97F.nonEmpty) indexMap.put("NRServingSCC5RxBeamRSRP0", nrServingSCC5RxBeamRSRP0_0xB97F.head)
     if (nrServingPCCRxBeamRSRP1_0xB97F != null && nrServingPCCRxBeamRSRP1_0xB97F.nonEmpty) indexMap.put("NRServingPCCRxBeamRSRP1", nrServingPCCRxBeamRSRP1_0xB97F.head)
     if (nrServingSCC1RxBeamRSRP1_0xB97F != null && nrServingSCC1RxBeamRSRP1_0xB97F.nonEmpty) indexMap.put("NRServingSCC1RxBeamRSRP1", nrServingSCC1RxBeamRSRP1_0xB97F.head)
     if (nrServingSCC2RxBeamRSRP1_0xB97F != null && nrServingSCC2RxBeamRSRP1_0xB97F.nonEmpty) indexMap.put("NRServingSCC2RxBeamRSRP1", nrServingSCC2RxBeamRSRP1_0xB97F.head)
     if (nrServingSCC3RxBeamRSRP1_0xB97F != null && nrServingSCC3RxBeamRSRP1_0xB97F.nonEmpty) indexMap.put("NRServingSCC3RxBeamRSRP1", nrServingSCC3RxBeamRSRP1_0xB97F.head)
     if (nrServingSCC4RxBeamRSRP1_0xB97F != null && nrServingSCC4RxBeamRSRP1_0xB97F.nonEmpty) indexMap.put("NRServingSCC4RxBeamRSRP1", nrServingSCC4RxBeamRSRP1_0xB97F.head)
     if (nrServingSCC5RxBeamRSRP1_0xB97F != null && nrServingSCC5RxBeamRSRP1_0xB97F.nonEmpty) indexMap.put("NRServingSCC5RxBeamRSRP1", nrServingSCC5RxBeamRSRP1_0xB97F.head)
     if (nrServingPCCRSRP_0xB97F != null && nrServingPCCRSRP_0xB97F.nonEmpty) indexMap.put("NRServingPCCRSRP", nrServingPCCRSRP_0xB97F.head)
     if (nrServingSCC1RSRP_0xB97F != null && nrServingSCC1RSRP_0xB97F.nonEmpty) indexMap.put("NRServingSCC1RSRP", nrServingSCC1RSRP_0xB97F.head)
     if (nrServingSCC2RSRP_0xB97F != null && nrServingSCC2RSRP_0xB97F.nonEmpty) indexMap.put("NRServingSCC2RSRP", nrServingSCC2RSRP_0xB97F.head)
     if (nrServingSCC3RSRP_0xB97F != null && nrServingSCC3RSRP_0xB97F.nonEmpty) indexMap.put("NRServingSCC3RSRP", nrServingSCC3RSRP_0xB97F.head)
     if (nrServingSCC4RSRP_0xB97F != null && nrServingSCC4RSRP_0xB97F.nonEmpty) indexMap.put("NRServingSCC4RSRP", nrServingSCC4RSRP_0xB97F.head)
     if (nrServingSCC5RSRP_0xB97F != null && nrServingSCC5RSRP_0xB97F.nonEmpty) indexMap.put("NRServingSCC5RSRP", nrServingSCC5RSRP_0xB97F.head)
     if (nrServingPCCRSRQ_0xB97F != null && nrServingPCCRSRQ_0xB97F.nonEmpty) indexMap.put("NRServingPCCRSRQ", nrServingPCCRSRQ_0xB97F.head)
     if (nrServingSCC1RSRQ_0xB97F != null && nrServingSCC1RSRQ_0xB97F.nonEmpty) indexMap.put("NRServingSCC1RSRQ", nrServingSCC1RSRQ_0xB97F.head)
     if (nrServingSCC2RSRQ_0xB97F != null && nrServingSCC2RSRQ_0xB97F.nonEmpty) indexMap.put("NRServingSCC2RSRQ", nrServingSCC2RSRQ_0xB97F.head)
     if (nrServingSCC3RSRQ_0xB97F != null && nrServingSCC3RSRQ_0xB97F.nonEmpty) indexMap.put("NRServingSCC3RSRQ", nrServingSCC3RSRQ_0xB97F.head)
     if (nrServingSCC4RSRQ_0xB97F != null && nrServingSCC4RSRQ_0xB97F.nonEmpty) indexMap.put("NRServingSCC4RSRQ", nrServingSCC4RSRQ_0xB97F.head)
     if (nrServingSCC5RSRQ_0xB97F != null && nrServingSCC5RSRQ_0xB97F.nonEmpty) indexMap.put("NRServingSCC5RSRQ", nrServingSCC5RSRQ_0xB97F.head)*/

    /* val nrsetup_NrScgState = if (!row.isNullAt(row.fieldIndex("nrsetupFornrscgstate"))) row.getString(row.fieldIndex("nrsetupFornrscgstate")) else null
   val NrBeamRsrp_LastLogPacket: List[String]  = if (!row.isNullAt(row.fieldIndex("beamRsrspOfB97f_PreviousLogPac"))) row.getSeq(row.fieldIndex("beamRsrspOfB97f_PreviousLogPac")).toList else null
    val  NrBeamIndex_LastLogPacket: List[Integer] = if (!row.isNullAt(row.fieldIndex("beamIndexOfB97f_PreviousLogPac"))) row.getSeq(row.fieldIndex("beamIndexOfB97f_PreviousLogPac")).toList else null
    val nrSetup_Success = if (!row.isNullAt(row.fieldIndex("nrsetupsuccess"))) row.getString(row.fieldIndex("nrsetupsuccess")) else null
    val nrSetup_SuccessLastLog = if (!row.isNullAt(row.fieldIndex("setupsuccess_PreviousLogPac"))) row.getString(row.fieldIndex("setupsuccess_PreviousLogPac")) else null
   */
    var servingbeamindex: Integer = null
    if(Nrs_servingbeamindex_B97F != null) { servingbeamindex = Nrs_servingbeamindex_B97F }
    if(Nrs_beamcount_B97F != null && Nrs_servingbeamindex_B97F != null && servingbeamindex != null) {
      var counter = 0
      var totalBeamCount = 0
      for (i <- 0 to Nrs_beamcount_B97F.size - 1) {
        var beamCount = Nrs_beamcount_B97F(i).toInt
        var servingcellIndex = Nrs_ccindex_B97F(0)
        var sevingcellpci = Nrs_servingpci_B97F(0)
        if (i < 1 && servingcellIndex == 0) {
          var pci = Nrs_pci_B97F.take(beamCount)
          val bsrspMap = pci.zip(Nrs_brsrp_B97F.take(beamCount)).toMap
          val bsrsqMap = pci.zip(Nrs_brsrq_B97F.take(beamCount)).toMap
          if (servingbeamindex.toInt != 255) {
            if (bsrspMap != null) {
              if (bsrspMap.contains(sevingcellpci)) {
                if (isValidNumeric(bsrspMap(sevingcellpci), "FLOAT")) indexMap.put("NRSBRSRP", bsrspMap(sevingcellpci).toFloat)
              } else if (bsrspMap.size > 0) indexMap.put("NRSBRSRP", bsrspMap.head._2.toFloat)
            }
            if (bsrsqMap != null) {
              if (bsrsqMap.contains(sevingcellpci)) {
                if (isValidNumeric(bsrsqMap(sevingcellpci), "FLOAT")) indexMap.put("NRSBRSRQ", bsrsqMap(sevingcellpci).toFloat)
              } else if (bsrsqMap.size > 0) indexMap.put("NRSBRSRQ", bsrsqMap.head._2.toFloat)
            }
            if (beamCount != null) indexMap.put("NRBEAMCOUNT", beamCount)
            if (sevingcellpci != null) indexMap.put("NRSPHYCELLID", sevingcellpci)
            if (servingbeamindex != null) indexMap.put("NRServingBeamIndex", servingbeamindex.toInt)
          }
        } else if (i >= 1 && i == servingcellIndex) {
          var pci = Nrs_pci_B97F.slice(totalBeamCount, Nrs_beamcount_B97F(i).toInt + totalBeamCount)
          val bsrspMap = pci.zip(Nrs_brsrp_B97F.slice(totalBeamCount, Nrs_beamcount_B97F(i).toInt + totalBeamCount)).toMap
          val bsrsqMap = pci.zip(Nrs_brsrq_B97F.slice(totalBeamCount, Nrs_beamcount_B97F(i).toInt + totalBeamCount)).toMap
          if (servingbeamindex.toInt != 255) {
            if (bsrspMap != null) {
              if (bsrspMap.contains(sevingcellpci)) {
                if (isValidNumeric(bsrspMap(sevingcellpci), "FLOAT")) indexMap.put("NRSBRSRP", bsrspMap(sevingcellpci).toFloat)
              } else if (bsrspMap.size > 0) indexMap.put("NRSBRSRP", bsrspMap.head._2.toFloat)
            }
            if (bsrsqMap != null) {
              if (bsrsqMap.contains(sevingcellpci)) {
                if (isValidNumeric(bsrsqMap(sevingcellpci), "FLOAT")) indexMap.put("NRSBRSRQ", bsrsqMap(sevingcellpci).toFloat)
              } else if (bsrsqMap.size > 0) indexMap.put("NRSBRSRQ", bsrsqMap.head._2.toFloat)
            }
            if (beamCount != null) indexMap.put("NRBEAMCOUNT", beamCount)
            if (sevingcellpci != null) indexMap.put("NRSPHYCELLID", sevingcellpci)
            if (servingbeamindex != null) indexMap.put("NRServingBeamIndex", servingbeamindex.toInt)
          }

        }
        totalBeamCount += beamCount
      }
      /* var beamindex_lastlog: Int = 0
       if(NrBeamIndex_LastLogPacket != null) { if(NrBeamIndex_LastLogPacket.size>0) beamindex_lastlog = NrBeamIndex_LastLogPacket(0) }
   */
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
      if (indexMap.contains("NRSBRSRP")) {
        if (LTE_RRC_State != null && !LTE_RRC_State.isEmpty) {
          if (LTE_RRC_State == "Connected") indexMap.put("NrSCGStateBRSRP", "CONNECTED")
        } else if (rrcstate) indexMap.put("NrSCGStateBRSRP", "CONNECTED")
      } else {
        indexMap.put("NrSCGStateBRSRP", "DISCONNECTED")
      }
    }
    indexMap
  }


  def getNr5gMacUlKPIsfrom0xB883(row: org.apache.spark.sql.Row): scala.collection.mutable.Map[String, Any] = {
    val indexMap: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()

    val numerology: List[String] = if (!row.isNullAt(row.fieldIndex("numerology0xB883"))) row.getSeq(row.fieldIndex("numerology0xB883")).toList else null
    val carrierid: List[Int] = if (!row.isNullAt(row.fieldIndex("carrierId0xB883"))) row.getSeq(row.fieldIndex("carrierId0xB883")).toList else null
    val harq: List[Int] = if (!row.isNullAt(row.fieldIndex("harq0xB883"))) row.getSeq(row.fieldIndex("harq0xB883")).toList else null
    val mcs: List[Int] = if (!row.isNullAt(row.fieldIndex("mcs0xB883"))) row.getSeq(row.fieldIndex("mcs0xB883")).toList else null
    val numrbs: List[Int] = if (!row.isNullAt(row.fieldIndex("numrbs0xB883"))) row.getSeq(row.fieldIndex("numrbs0xB883")).toList else null
    val tbsize: List[Long] = if (!row.isNullAt(row.fieldIndex("tbsize0xB883"))) row.getSeq(row.fieldIndex("tbsize0xB883")).toList else null
    val txmode: List[String] = if (!row.isNullAt(row.fieldIndex("txmode0xB883"))) row.getSeq(row.fieldIndex("txmode0xB883")).toList else null
    val txtype: List[String] = if (!row.isNullAt(row.fieldIndex("txtype0xB883"))) row.getSeq(row.fieldIndex("txtype0xB883")).toList else null
    val bwpidx: List[Int] = if (!row.isNullAt(row.fieldIndex("bwpidx0xB883"))) row.getSeq(row.fieldIndex("bwpidx0xB883")).toList else null

    if (carrierid != null) {
      for (i <- 0 to carrierid.size - 1) {
        var numerologyMap = (carrierid.zip(numerology).toMap).toArray
        var harqMap = (carrierid.zip(harq).toMap).toArray
        var mcsMap = (carrierid.zip(mcs).toMap).toArray
        var numRbsMap = (carrierid.zip(numrbs).toMap).toArray
        var tbsizeMap = (carrierid.zip(tbsize).toMap).toArray
        var txmodeMap = (carrierid.zip(txmode).toMap).toArray
        var txtypeMap = (carrierid.zip(txtype).toMap).toArray
        var bwpidxMap = (carrierid.zip(bwpidx).toMap).toArray
        if (numerologyMap.size > 0) {
          for (i <- 0 to numerologyMap.size - 1) {
            var index: String = null
            if (numerologyMap(i)._1 == 0) {
              index = "Pcc"
            } else if (numerologyMap(i)._1 == 1) {
              index = "Scc1"
            }
            if(numerologyMap(i) != null && numerologyMap.size > i && index != null) indexMap.put("Pusch" + index + "Numerology", numerologyMap(i)._2.toLowerCase().split("khz")(0).toInt)
            if (numerologyMap != null && numerologyMap.size > i && index != null) {
              if (index != "Scc1") {
                indexMap.put("Pusch" + index + "NumOfULCC", numerologyMap.size)
              }
            }
            if (harqMap.size > i && index != null) {
              if (harqMap(i) != null) {
                indexMap.put("Pusch" + index + "HarqId", harqMap(i)._2)
              }
            }
            if (mcsMap.size > i && index != null) {
              if (mcsMap(i) != null) {
                indexMap.put("Pusch" + index + "Mcs", mcsMap(i)._2)
              }
            }
            if (numRbsMap.size > i && index != null) {
              if (numRbsMap(i) != null) {
                indexMap.put("Pusch" + index + "NumRbs", numRbsMap(i)._2)
              }
            }
            if (tbsizeMap.size > i && index != null) {
              if (tbsizeMap(i) != null) {
                indexMap.put("Pusch" + index + "Tbsize", tbsizeMap(i)._2)
              }
            }
            if (txmodeMap.size > i && index != null) {
              if (txmodeMap(i) != null) {
                indexMap.put("Pusch" + index + "TxMode", txmodeMap(i)._2)
              }
            }
            if (txtypeMap.size > i && index != null) {
              if (txtypeMap(i) != null) {
                indexMap.put("Pusch" + index + "TxType", txtypeMap(i)._2)
              }
            }
            if (bwpidxMap.size > i && index != null) {
              if (bwpidxMap(i) != null) {
                indexMap.put("Pusch" + index + "BwpIdex", bwpidxMap(i)._2)
              }
            }
          }
        }
      }
    }
    indexMap
  }

  def getNrScgStateKPI(row: org.apache.spark.sql.Row, filename: String, nrsBrsrp: Array[(String, String)]): scala.collection.mutable.Map[String, Any] = {
    val indexMap: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()
    val nrSetupSuccess: String = if (!row.isNullAt(row.fieldIndex("Nrsetupsuccess"))) row.getString(row.fieldIndex("Nrsetupsuccess")) else null
    //  val nrsetupForNrScgState:String = if (!row.isNullAt(row.fieldIndex("nrsetupFornrscgstate"))) row.getString(row.fieldIndex("nrsetupFornrscgstate")) else null
    val Nrs_brsrp0x97f: String = if (!row.isNullAt(row.fieldIndex("Serving_BRSRP_0xB97F"))) row.getString(row.fieldIndex("Serving_BRSRP_0xB97F")) else null
    val nrRelease: Integer = if (!row.isNullAt(row.fieldIndex("nrReleaseEvt"))) row.getInt(row.fieldIndex("nrReleaseEvt")) else null
    //  val nrReleaseForNrScgState:Integer = if (!row.isNullAt(row.fieldIndex("nrReleaseFornrscgstate"))) row.getInt(row.fieldIndex("nrReleaseFornrscgstate")) else null
    val firstRecordFlag: Integer = if (!row.isNullAt(row.fieldIndex("rnFR"))) row.getInt(row.fieldIndex("rnFR")) else null
    // val NrState: List[String] = if (!row.isNullAt(row.fieldIndex("nrstate"))) row.getSeq(row.fieldIndex("nrstate")).toList else null
    var brsrpSamples0xb97f: Int = 0
    var brsrpSamples0xb975: Int = 0
    if (Nrs_brsrp0x97f != null) brsrpSamples0xb97f = Nrs_brsrp0x97f.replaceAll(",+", ",").split(",").map(_.trim).toList.size
    if (nrsBrsrp != null) brsrpSamples0xb975 = nrsBrsrp.size
    if (filename != null && !filename.contains("PART0")) {
      if (firstRecordFlag != null) {
        if (firstRecordFlag == 1 && (brsrpSamples0xb97f >= 2 || brsrpSamples0xb975 >= 2)) indexMap.put("NrSCGstate", "CONNECTED")
      }
      /*else if(nrSetupSuccess != null && nrRelease != null && NrState != null) {
        var nrsetupstate:String = null
        if(NrState.size>0) nrsetupstate = NrState.last
        if(nrsetupstate=="success") indexMap.put("NrSCGstate", "CONNECTED") else if(nrsetupstate=="release") indexMap.put("NrSCGstate", "DISCONNECTED")
      } */
      else {
        indexMap.put("NrSCGstate", "DISCONNECTED")
      }
    } else {
      indexMap.put("NrSCGstate", "DISCONNECTED")
    }
    indexMap
  }

  def getReTxBlerKPIs(row: org.apache.spark.sql.Row) = {
    val indexMap: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()

    val numReTxSumCnt0: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numReTxCnt0Sum0xB887"))) row.getLong(row.fieldIndex("numReTxCnt0Sum0xB887")) else null
    val numReTxSumCnt1: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numReTxCnt1Sum0xB887"))) row.getLong(row.fieldIndex("numReTxCnt1Sum0xB887")) else null
    val numReTxSumCnt2: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numReTxCnt2Sum0xB887"))) row.getLong(row.fieldIndex("numReTxCnt2Sum0xB887")) else null
    val numReTxSumCnt3: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numReTxCnt3Sum0xB887"))) row.getLong(row.fieldIndex("numReTxCnt3Sum0xB887")) else null
    val numReTxSumCnt4: java.lang.Long = if (!row.isNullAt(row.fieldIndex("numReTxCnt4Sum0xB887"))) row.getLong(row.fieldIndex("numReTxCnt4Sum0xB887")) else null
    val CRCFailSumcnt0: java.lang.Long = if (!row.isNullAt(row.fieldIndex("CRCFailcnt0Sum0xB887"))) row.getLong(row.fieldIndex("CRCFailcnt0Sum0xB887")) else null
    val CRCFailSumcnt1: java.lang.Long = if (!row.isNullAt(row.fieldIndex("CRCFailcnt1Sum0xB887"))) row.getLong(row.fieldIndex("CRCFailcnt1Sum0xB887")) else null
    val CRCFailSumcnt2: java.lang.Long = if (!row.isNullAt(row.fieldIndex("CRCFailcnt2Sum0xB887"))) row.getLong(row.fieldIndex("CRCFailcnt2Sum0xB887")) else null
    val CRCFailSumcnt3: java.lang.Long = if (!row.isNullAt(row.fieldIndex("CRCFailcnt3Sum0xB887"))) row.getLong(row.fieldIndex("CRCFailcnt3Sum0xB887")) else null
    val CRCFailSumcnt4: java.lang.Long = if (!row.isNullAt(row.fieldIndex("CRCFailcnt4Sum0xB887"))) row.getLong(row.fieldIndex("CRCFailcnt4Sum0xB887")) else null


    if (numReTxSumCnt0 != null && CRCFailSumcnt0 != null) {
      if (numReTxSumCnt0 > 0) indexMap.put("ReTxblerBler0", BigDecimal((CRCFailSumcnt0.toDouble * 100.0) / numReTxSumCnt0.toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue())
    }
    if (numReTxSumCnt1 != null && CRCFailSumcnt1 != null) {
      if (numReTxSumCnt1 > 0) indexMap.put("ReTxblerBler1", BigDecimal((CRCFailSumcnt1.toDouble * 100.0) / numReTxSumCnt1.toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue())
    }
    if (numReTxSumCnt2 != null && CRCFailSumcnt2 != null) {
      if (numReTxSumCnt2 > 0) indexMap.put("ReTxblerBler2", BigDecimal((CRCFailSumcnt2.toDouble * 100.0) / numReTxSumCnt2.toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue())
    }
    if (numReTxSumCnt3 != null && CRCFailSumcnt3 != null) {
      if (numReTxSumCnt3 > 0) indexMap.put("ReTxblerBler3", BigDecimal((CRCFailSumcnt3.toDouble * 100.0) / numReTxSumCnt3.toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue())
    }
    if (numReTxSumCnt4 != null && CRCFailSumcnt4 != null) {
      if (numReTxSumCnt4 > 0) indexMap.put("ReTxblerBler4", BigDecimal((CRCFailSumcnt4.toDouble * 100.0) / numReTxSumCnt4.toDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue())
    }

    indexMap
  }

  def getNrCarrierComponentPCIs(row: org.apache.spark.sql.Row) = {
    val indexMap: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()

    var PCI_0xB887: Array[(String, String)] = null
    val carrierid0xB887: String = if (!row.isNullAt(row.fieldIndex("carrieridcsv_0xb887"))) row.getString(row.fieldIndex("carrieridcsv_0xb887")) else null
    val pci0xB887: String = if (!row.isNullAt(row.fieldIndex("cellid_0xb887"))) row.getString(row.fieldIndex("cellid_0xb887")) else null

    if (carrierid0xB887 != null && pci0xB887 != null) {
      PCI_0xB887 = getServingCellCaKPIs(carrierid0xB887, pci0xB887)
    }
    if (PCI_0xB887 != null) {
      for (i <- 0 to (PCI_0xB887.size - 1)) {
        var Index: String = ""
        try {
          if (PCI_0xB887(i)._1 == "0") Index = "PCC"
          if (PCI_0xB887(i)._1 == "1") Index = "SCC1"
          if (PCI_0xB887(i)._1 == "2") Index = "SCC2"
          if (PCI_0xB887(i)._1 == "3") Index = "SCC3"
          if (PCI_0xB887(i)._1 == "4") Index = "SCC4"
          if (PCI_0xB887(i)._1 == "5") Index = "SCC5"
          if (PCI_0xB887(i)._1 == "6") Index = "SCC6"
          if (PCI_0xB887(i)._1 == "7") Index = "SCC7"

          if (!Index.isEmpty && PCI_0xB887.size > i) {
            if (isValidNumeric(PCI_0xB887(i)._2, "Number") && Index != "PCC") indexMap.put("NRS" + Index + "PHYCELLID", PCI_0xB887(i)._2.toInt)
          }
        }
        catch {
          case e: Exception =>
            logger.error("[CreateMapRdd] Exception occured while Calculating PCC to SCC7 : " + e.getMessage)
        }
      }
    }
    indexMap
  }

  def getCdrxKpis(row: org.apache.spark.sql.Row) = {
    val indexMap: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()

    val slotCurrentList: List[String] = if (!row.isNullAt(row.fieldIndex("slotCurrent0xb890"))) row.getSeq(row.fieldIndex("slotCurrent0xb890")).toList else null
    val slotPrevList: List[String] = if (!row.isNullAt(row.fieldIndex("slotPrev0xb890"))) row.getSeq(row.fieldIndex("slotPrev0xb890")).toList else null
    val sfnCurrentList: List[String] = if (!row.isNullAt(row.fieldIndex("sfnCurrent0xb890"))) row.getSeq(row.fieldIndex("sfnCurrent0xb890")).toList else null
    val sfnPrevList: List[String] = if (!row.isNullAt(row.fieldIndex("sfnPrev0xb890"))) row.getSeq(row.fieldIndex("sfnPrev0xb890")).toList else null

    val slot: List[String] = if (!row.isNullAt(row.fieldIndex("slot_0xB890"))) row.getSeq(row.fieldIndex("slot_0xB890")).toList else null
    val frame: List[String] = if (!row.isNullAt(row.fieldIndex("sfn0xB890"))) row.getSeq(row.fieldIndex("sfn0xB890")).toList else null
    val numerology: List[String] = if (!row.isNullAt(row.fieldIndex("numerology0xB890"))) row.getSeq(row.fieldIndex("numerology0xB890")).toList else null

    val cdrxOndurationTimer: String = if (!row.isNullAt(row.fieldIndex("cdrxOndurationTimer_0xB821"))) row.getString(row.fieldIndex("cdrxOndurationTimer_0xB821")) else null
    val cdrxInactivityTimer: String = if (!row.isNullAt(row.fieldIndex("cdrxInactivityTimer_0xB821"))) row.getString(row.fieldIndex("cdrxInactivityTimer_0xB821")) else null
    val cdrxLongDrxCyle: String = if (!row.isNullAt(row.fieldIndex("cdrxLongDrxCycle_0xB821"))) row.getString(row.fieldIndex("cdrxLongDrxCycle_0xB821")) else null
    val cdrxShortDrxCyle: String = if (!row.isNullAt(row.fieldIndex("cdrxShortDrxCycle_0xB821"))) row.getString(row.fieldIndex("cdrxShortDrxCycle_0xB821")) else null

    var totailInactiveDuration: java.lang.Double = null
    var totalDuration: java.lang.Double = null

    if (slotCurrentList != null && slotPrevList != null && numerology != null) {
      for (i <- 0 to slotCurrentList.size - 1) {
        totailInactiveDuration += Constants.CalculateMsFromSfnAndSubFrames5gNr(numerology.head, sfnPrevList(i).toInt, slotPrevList(i).toInt, sfnCurrentList(i).toInt, slotCurrentList(i).toInt)
      }
    }

    if (slot != null && frame != null && numerology != null) {
      for (i <- 0 to slot.size - 1) {
        totalDuration += Constants.CalculateMsFromSfnAndSubFrames5gNr(numerology.head, frame.head.toInt, slot.head.toInt, frame.last.toInt, slot.last.toInt)
      }
    }

    if (totailInactiveDuration != null && totalDuration != null) indexMap.put("CDRXInactivePercentage", (totailInactiveDuration / totalDuration) * 100)
    if (cdrxOndurationTimer != null) if (cdrxOndurationTimer.contains("ms")) {
      indexMap.put("CDRXOndurationTimer", cdrxOndurationTimer.split("ms")(1))
    } else if (cdrxOndurationTimer.contains("spare")) {
      indexMap.put("CDRXOndurationTimer", 999)
    }
    if (cdrxInactivityTimer != null) if (cdrxInactivityTimer.contains("ms")) {
      indexMap.put("CDRXInactivityTimer", cdrxInactivityTimer.split("ms")(1))
    } else if (cdrxInactivityTimer.contains("spare")) {
      indexMap.put("CDRXInactivityTimer", 999)
    }
    if (cdrxLongDrxCyle != null) if (cdrxLongDrxCyle.contains("ms")) {
      indexMap.put("CDRXLongDrxCyle", cdrxLongDrxCyle.split("ms")(1))
    } else if (cdrxLongDrxCyle.contains("spare")) {
      indexMap.put("CDRXLongDrxCyle", 999)
    }
    if (cdrxShortDrxCyle != null) if (cdrxShortDrxCyle.contains("ms")) {
      indexMap.put("CDRXShortDrxCyle", cdrxShortDrxCyle.split("ms")(1))
    } else if (cdrxShortDrxCyle.contains("spare")) {
      indexMap.put("CDRXShortDrxCyle", 999)
    }
  }

  def getNr0xB8A7KPIs(row: Row): mutable.Map[String, Any] = {
    val indexMap: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()

    val nRRankIndexPCC: Integer = if (!row.isNullAt(row.fieldIndex("NRRankIndexPCC"))) row.getInt(row.fieldIndex("NRRankIndexPCC")) else null
    val nRRankIndexSCC1: Integer = if (!row.isNullAt(row.fieldIndex("NRRankIndexSCC1"))) row.getInt(row.fieldIndex("NRRankIndexSCC1")) else null
    val nRRankIndexSCC2: Integer = if (!row.isNullAt(row.fieldIndex("NRRankIndexSCC2"))) row.getInt(row.fieldIndex("NRRankIndexSCC2")) else null
    val nRRankIndexSCC3: Integer = if (!row.isNullAt(row.fieldIndex("NRRankIndexSCC3"))) row.getInt(row.fieldIndex("NRRankIndexSCC3")) else null
    val nRRankIndexSCC4: Integer = if (!row.isNullAt(row.fieldIndex("NRRankIndexSCC4"))) row.getInt(row.fieldIndex("NRRankIndexSCC4")) else null
    val nRRankIndexSCC5: Integer = if (!row.isNullAt(row.fieldIndex("NRRankIndexSCC5"))) row.getInt(row.fieldIndex("NRRankIndexSCC5")) else null
    val nRRankIndexSCC6: Integer = if (!row.isNullAt(row.fieldIndex("NRRankIndexSCC6"))) row.getInt(row.fieldIndex("NRRankIndexSCC6")) else null
    val nRRankIndexSCC7: Integer = if (!row.isNullAt(row.fieldIndex("NRRankIndexSCC7"))) row.getInt(row.fieldIndex("NRRankIndexSCC7")) else null
    val nRCQIWBPCC: Integer = if (!row.isNullAt(row.fieldIndex("NRCQIWBPCC"))) row.getInt(row.fieldIndex("NRCQIWBPCC")) else null
    val nRCQIWBSCC1: Integer = if (!row.isNullAt(row.fieldIndex("NRCQIWBSCC1"))) row.getInt(row.fieldIndex("NRCQIWBSCC1")) else null
    val nRCQIWBSCC2: Integer = if (!row.isNullAt(row.fieldIndex("NRCQIWBSCC2"))) row.getInt(row.fieldIndex("NRCQIWBSCC2")) else null
    val nRCQIWBSCC3: Integer = if (!row.isNullAt(row.fieldIndex("NRCQIWBSCC3"))) row.getInt(row.fieldIndex("NRCQIWBSCC3")) else null
    val nRCQIWBSCC4: Integer = if (!row.isNullAt(row.fieldIndex("NRCQIWBSCC4"))) row.getInt(row.fieldIndex("NRCQIWBSCC4")) else null
    val nRCQIWBSCC5: Integer = if (!row.isNullAt(row.fieldIndex("NRCQIWBSCC5"))) row.getInt(row.fieldIndex("NRCQIWBSCC5")) else null
    val nRCQIWBSCC6: Integer = if (!row.isNullAt(row.fieldIndex("NRCQIWBSCC6"))) row.getInt(row.fieldIndex("NRCQIWBSCC6")) else null
    val nRCQIWBSCC7: Integer = if (!row.isNullAt(row.fieldIndex("NRCQIWBSCC7"))) row.getInt(row.fieldIndex("NRCQIWBSCC7")) else null

    if (nRRankIndexPCC != null) indexMap.put("NRRankIndexPCC", nRRankIndexPCC)
    if (nRRankIndexSCC1 != null) indexMap.put("NRRankIndexSCC1", nRRankIndexSCC1)
    if (nRRankIndexSCC2 != null) indexMap.put("NRRankIndexSCC2", nRRankIndexSCC2)
    if (nRRankIndexSCC3 != null) indexMap.put("NRRankIndexSCC3", nRRankIndexSCC3)
    if (nRRankIndexSCC4 != null) indexMap.put("NRRankIndexSCC4", nRRankIndexSCC4)
    if (nRRankIndexSCC5 != null) indexMap.put("NRRankIndexSCC5", nRRankIndexSCC5)
    if (nRRankIndexSCC6 != null) indexMap.put("NRRankIndexSCC6", nRRankIndexSCC6)
    if (nRRankIndexSCC7 != null) indexMap.put("NRRankIndexSCC7", nRRankIndexSCC7)
    if (nRCQIWBPCC != null) indexMap.put("NRCQIWBPCC", nRCQIWBPCC)
    if (nRCQIWBSCC1 != null) indexMap.put("NRCQIWBSCC1", nRCQIWBSCC1)
    if (nRCQIWBSCC2 != null) indexMap.put("NRCQIWBSCC2", nRCQIWBSCC2)
    if (nRCQIWBSCC3 != null) indexMap.put("NRCQIWBSCC3", nRCQIWBSCC3)
    if (nRCQIWBSCC4 != null) indexMap.put("NRCQIWBSCC4", nRCQIWBSCC4)
    if (nRCQIWBSCC5 != null) indexMap.put("NRCQIWBSCC5", nRCQIWBSCC5)
    if (nRCQIWBSCC6 != null) indexMap.put("NRCQIWBSCC6", nRCQIWBSCC6)
    if (nRCQIWBSCC7 != null) indexMap.put("NRCQIWBSCC7", nRCQIWBSCC7)

    indexMap
  }

  def getRootMetricsTestKpis(row: Row): mutable.Map[String, Any] = {
    val indexMap: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()
    val testCategory: Array[String] = Array("M2MReceive", "Idle", "UDPECHO", "TRANSMIT", "M2MCall", "SMS", "DNS", "LDR", "LDRS", "UPLOAD", "DOWNLOAD")
    val rmTestName: String = if (!row.isNullAt(row.fieldIndex("rmTestName"))) row.getString(row.fieldIndex("rmTestName")) else null
    val rmTestState: String = if (!row.isNullAt(row.fieldIndex("rmTestState"))) row.getString(row.fieldIndex("rmTestState")) else null
    val testCycleId: String = if (!row.isNullAt(row.fieldIndex("testCycleId"))) row.getString(row.fieldIndex("testCycleId")) else null
    val deviceId: String = if (!row.isNullAt(row.fieldIndex("deviceId"))) row.getString(row.fieldIndex("deviceId")) else null

    if (rmTestName != null && testCategory.contains(rmTestName)) {
      if (rmTestState != null) indexMap.put("RmTestState", rmTestState)
      if (testCycleId != null) indexMap.put("rmTestCycleId", testCycleId.toLong)
      if (deviceId != null) indexMap.put("rmDeviceId", deviceId.toLong)
      if (rmTestName != null) indexMap.put("RmTestName", rmTestName)

      if (rmTestState == "start") indexMap.put(rmTestName + "Start", 1)
      if (rmTestState == "end") indexMap.put(rmTestName + "End", 1)

      val testInfoStart: String = if (!row.isNullAt(row.fieldIndex(rmTestName + "_Start"))) row.getString(row.fieldIndex(rmTestName + "_Start")) else null
      val testInfoEnd: String = if (!row.isNullAt(row.fieldIndex(rmTestName + "_End"))) row.getString(row.fieldIndex(rmTestName + "_End")) else null

      if (testInfoStart != null && testInfoEnd != null) {
        var Duration:Float = 0F
        val deviceStartTime = ZonedDateTime.parse(testInfoStart.split("_")(1) + 'Z')
        val deviceEndTime = ZonedDateTime.parse(testInfoEnd.split("_")(1) + 'Z')

        if (deviceStartTime != null) {
          Duration = ((deviceEndTime.toInstant.toEpochMilli - deviceStartTime.toInstant.toEpochMilli) / 1000f).toFloat
        }

        if (Duration != 0) indexMap.put(rmTestName + "Duration", roundingToDecimal(Duration.toString).toFloat)

      }
    }

    indexMap
  }

  def getTypeName(fileType: String, indexPrefix: String = ""): String = indexPrefix + "_doc"

  def getIndexName(timeStamp: String, Type: String, defaultIndex: String, osName: String): String = {
    var indexName: String = ""
    try {
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val parsedDate = dateFormat.parse(timeStamp)
      val cal: Calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
      cal.setTimeInMillis(parsedDate.getTime)
      var index: String = defaultIndex
      if (StringUtils.containsIgnoreCase(osName, Constants.CONST_IOS_NAME)) {
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
