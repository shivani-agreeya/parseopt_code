package com.verizon.oneparser.parselogs

import com.dmat.qc.parser.ParseUtils
import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.ProcessLogCodes
import com.verizon.oneparser.common.Constants
import com.verizon.oneparser.schema._
import org.apache.spark.sql.Row

import scala.collection.immutable.List
import scala.util.Try

object LogRecordParserB193 extends ProcessLogCodes with LazyLogging {

  def parseLogRecordB193(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordB193Obj:LogRecordB193 = LogRecordB193()

    def parse22 = {
      val bean45459 = process45459_22(x, exceptionUseArray)
      //logger.info("0xB193_22 json >>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45459));
      val cinr0Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.CINRRX0)))).flatten.flatten.flatten
      val cinr1Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.CINRRX1)))).flatten.flatten.flatten
      val cinr2Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.CINRRX2)))).flatten.flatten.flatten
      val cinr3Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.CINRRX3)))).flatten.flatten.flatten
      val PhysicalCellIDVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.PhysicalCellID)))).flatten.flatten.flatten
      val ServingCellIndex_B193Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.ServingCellIndex)))).flatten.flatten.flatten
      val IsServingCellVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.IsServingCell)))).flatten.flatten.flatten
      val CurrentSFNVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.CurrentSFN)))).flatten.flatten.flatten
      val CurrentSubframeNumberVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.CurrentSubframeNumber)))).flatten.flatten.flatten
      val IsRestrictedVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.IsRestricted)))).flatten.flatten.flatten
      val CellTiming_0Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.CellTiming0)))).flatten.flatten.flatten
      val CellTiming_1Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.CellTiming1)))).flatten.flatten.flatten
      val CellTimingSFN_0Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.CellTimingSFN0)))).flatten.flatten.flatten
      val CellTimingSFN_1Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.CellTimingSFN1)))).flatten.flatten.flatten
      val InstRSRPRx_0Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstRSRPRx0)))).flatten.flatten.flatten
      val InstRSRPRx_1Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstRSRPRx1)))).flatten.flatten.flatten
      val InstRSRPRx_2Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstRSRPRx2)))).flatten.flatten.flatten
      val InstRSRPRx_3Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstRSRPRx3)))).flatten.flatten.flatten
      val InstMeasuredRSRPVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstMeasuredRSRP)))).flatten.flatten.flatten
      val InstRSRQRx_0Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstRSRQRx0)))).flatten.flatten.flatten
      val InstRSRQRx_1Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstRSRQRx1)))).flatten.flatten.flatten
      val InstRSRQRx_2Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstRSRQRx2)))).flatten.flatten.flatten
      val InstRSRQRx_3Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstRSRQRx3)))).flatten.flatten.flatten
      val InstRSRQVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstRSRQ)))).flatten.flatten.flatten
      val InstRSSIRx_0Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstRSSIRx0)))).flatten.flatten.flatten
      val InstRSSIRx_1Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstRSSIRx1)))).flatten.flatten.flatten
      val InstRSSIRx_2Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstRSSIRx2)))).flatten.flatten.flatten
      val InstRSSIRx_3Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstRSSIRx3)))).flatten.flatten.flatten
      val InstRSSIVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.InstRSSI)))).flatten.flatten.flatten
      val ResidualFrequencyErrorVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.ResidualFrequencyError)))).flatten.flatten.flatten
      val FTLSNRRx_0Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.FTLSNRRx0)))).flatten.flatten.flatten
      val FTLSNRRx_1Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.FTLSNRRx1)))).flatten.flatten.flatten
      val FTLSNRRx_2Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.FTLSNRRx2)))).flatten.flatten.flatten
      val FTLSNRRx_3Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.FTLSNRRx3)))).flatten.flatten.flatten
      val ProjectedSirVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.ProjectedSir)))).flatten.flatten.flatten
      val PostIcRsrqVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.sr_15967_inst_list.asScala.toList.map(_.PostIcRsrq)))).flatten.flatten.flatten
      val NumberofSubPacketsVal: Integer = Integer.parseInt(bean45459.NumberofSubPackets)
      val SubPacketIDVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.SubPacketID)
      val SubPacketSizeVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.SubPacketSize)).flatten
      val EARFCNVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
      val NumofCellVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.NumofCell))).flatten.flatten
      val horxd_modeVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15305_inst_list.asScala.toList.map(_.horxdmode))).flatten.flatten
      logRecordB193Obj = logRecordB193Obj.copy(
        cinr0 = Try(List(cinr0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr1 = Try(List(cinr1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr2 = Try(List(cinr2Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr3 = Try(List(cinr3Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        PhysicalCellID_B193 = Try(List(PhysicalCellIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ServingCellIndex_B193 = Try(List(ServingCellIndex_B193Val(0))).getOrElse(null),
        IsServingCell = Try(List(IsServingCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSFN = Try(List(CurrentSFNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSubframeNumber = Try(List(CurrentSubframeNumberVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTiming_0 = Try(List(CellTiming_0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        CellTiming_1 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_0 = Try(List(CellTimingSFN_0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        CellTimingSFN_1 = Try(List(CellTimingSFN_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        InstRSRPRx_0 = Try(List(InstRSRPRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_1 = Try(List(InstRSRPRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_2 = Try(List(InstRSRPRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_3 = Try(List(InstRSRPRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstMeasuredRSRP = Try(List(InstMeasuredRSRPVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_0 = Try(List(InstRSRQRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_1 = Try(List(InstRSRQRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_2 = Try(List(InstRSRQRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_3 = Try(List(InstRSRQRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQ = Try(List(InstRSRQVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_0 = Try(List(InstRSSIRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_1 = Try(List(InstRSSIRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_2 = Try(List(InstRSSIRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_3 = Try(List(InstRSSIRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ResidualFrequencyError = Try(List(ResidualFrequencyErrorVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        FTLSNRRx_0 = Try(List(FTLSNRRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_1 = Try(List(FTLSNRRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_2 = Try(List(FTLSNRRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_3 = Try(List(FTLSNRRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ProjectedSir = Try(List(ProjectedSirVal(0).toLong.asInstanceOf[Long])).getOrElse(null),
        PostIcRsrq = Try(List(PostIcRsrqVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        NumberofSubPackets = NumberofSubPacketsVal,
        SubPacketID = Try(List(SubPacketIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        SubPacketSize = Try(List(SubPacketSizeVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        EARFCN_B193 = Try(List(EARFCNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        NumofCell = Try(List(NumofCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        horxd_mode = Try(List(horxd_modeVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        InstRSSI = Try(List(InstRSSIVal(0).toDouble.asInstanceOf[Double])).getOrElse(null)
      )
      logRecord = logRecord.copy(logRecordName = "B193", logCode = "45459",
        hexLogCode = "0xB193",
        version = 22, logRecordB193 = logRecordB193Obj)
    }

    def parse24 = {
      val bean45459 = process45459_24(x, exceptionUseArray)
      //logger.info("0xB193_22 json >>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45459));
      //            val cinr0Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.CINRRX0)))).flatten.flatten.flatten
      //            val cinr1Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.CINRRX1)))).flatten.flatten.flatten
      //            val cinr2Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.CINRRX2)))).flatten.flatten.flatten
      //            val cinr3Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.CINRRX3)))).flatten.flatten.flatten
      val PhysicalCellIDVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.PhysicalCellID)))).flatten.flatten.flatten
      val ServingCellIndex_B193Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.ServingCellIndex)))).flatten.flatten.flatten
      val IsServingCellVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.IsServingCell)))).flatten.flatten.flatten
      val CurrentSFNVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.CurrentSFN)))).flatten.flatten.flatten
      val CurrentSubframeNumberVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.CurrentSubframeNumber)))).flatten.flatten.flatten
      val IsRestrictedVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.IsRestricted)))).flatten.flatten.flatten
      val CellTiming_0Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.CellTiming0)))).flatten.flatten.flatten
      val CellTiming_1Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.CellTiming1)))).flatten.flatten.flatten
      val CellTimingSFN_0Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.CellTimingSFN0)))).flatten.flatten.flatten
      val CellTimingSFN_1Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.CellTimingSFN1)))).flatten.flatten.flatten
      val InstRSRPRx_0Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstRSRPRx0)))).flatten.flatten.flatten
      val InstRSRPRx_1Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstRSRPRx1)))).flatten.flatten.flatten
      //            val InstRSRPRx_2Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstRSRPRx2)))).flatten.flatten.flatten
      //            val InstRSRPRx_3Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstRSRPRx3)))).flatten.flatten.flatten
      val InstMeasuredRSRPVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstMeasuredRSRP)))).flatten.flatten.flatten
      val InstRSRQRx_0Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstRSRQRx0)))).flatten.flatten.flatten
      val InstRSRQRx_1Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstRSRQRx1)))).flatten.flatten.flatten
      //            val InstRSRQRx_2Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstRSRQRx2)))).flatten.flatten.flatten
      //            val InstRSRQRx_3Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstRSRQRx3)))).flatten.flatten.flatten
      val InstRSRQVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstRSRQ)))).flatten.flatten.flatten
      val InstRSSIRx_0Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstRSSIRx0)))).flatten.flatten.flatten
      val InstRSSIRx_1Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstRSSIRx1)))).flatten.flatten.flatten
      //            val InstRSSIRx_2Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstRSSIRx2)))).flatten.flatten.flatten
      //            val InstRSSIRx_3Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstRSSIRx3)))).flatten.flatten.flatten
      val InstRSSIVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.InstRSSI)))).flatten.flatten.flatten
      val ResidualFrequencyErrorVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.ResidualFrequencyError)))).flatten.flatten.flatten
      val FTLSNRRx_0Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.FTLSNRRx0)))).flatten.flatten.flatten
      val FTLSNRRx_1Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.FTLSNRRx1)))).flatten.flatten.flatten
      //            val FTLSNRRx_2Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.FTLSNRRx2)))).flatten.flatten.flatten
      //            val FTLSNRRx_3Val = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.FTLSNRRx3)))).flatten.flatten.flatten
      val ProjectedSirVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.ProjectedSir)))).flatten.flatten.flatten
      val PostIcRsrqVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.sr_15881_inst_list.asScala.toList.map(_.PostIcRsrq)))).flatten.flatten.flatten
      val NumberofSubPacketsVal: Integer = Integer.parseInt(bean45459.NumberofSubPackets)
      val SubPacketIDVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.SubPacketID)
      val SubPacketSizeVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.SubPacketSize)).flatten
      val EARFCNVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
      val NumofCellVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.NumofCell))).flatten.flatten
      //            val horxd_modeVal = bean45459.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15306_inst_list.asScala.toList.map(_.horxdmode))).flatten.flatten
      logRecordB193Obj = logRecordB193Obj.copy(
        //              cinr0 = Try(List(cinr0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        //              cinr1 = Try(List(cinr1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        //              cinr2 = Try(List(cinr2Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        //              cinr3 = Try(List(cinr3Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        PhysicalCellID_B193 = Try(List(PhysicalCellIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ServingCellIndex_B193 = Try(List(ServingCellIndex_B193Val(0))).getOrElse(null),
        IsServingCell = Try(List(IsServingCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSFN = Try(List(CurrentSFNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSubframeNumber = Try(List(CurrentSubframeNumberVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTiming_0 = Try(List(CellTiming_0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        CellTiming_1 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_0 = Try(List(CellTimingSFN_0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        CellTimingSFN_1 = Try(List(CellTimingSFN_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        InstRSRPRx_0 = Try(List(InstRSRPRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_1 = Try(List(InstRSRPRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        //              InstRSRPRx_2 = Try(List(InstRSRPRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        //              InstRSRPRx_3 = Try(List(InstRSRPRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstMeasuredRSRP = Try(List(InstMeasuredRSRPVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_0 = Try(List(InstRSRQRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_1 = Try(List(InstRSRQRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        //              InstRSRQRx_2 = Try(List(InstRSRQRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        //              InstRSRQRx_3 = Try(List(InstRSRQRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQ = Try(List(InstRSRQVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_0 = Try(List(InstRSSIRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_1 = Try(List(InstRSSIRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        //              InstRSSIRx_2 = Try(List(InstRSSIRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        //              InstRSSIRx_3 = Try(List(InstRSSIRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ResidualFrequencyError = Try(List(ResidualFrequencyErrorVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        FTLSNRRx_0 = Try(List(FTLSNRRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_1 = Try(List(FTLSNRRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        //              FTLSNRRx_2 = Try(List(FTLSNRRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        //              FTLSNRRx_3 = Try(List(FTLSNRRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ProjectedSir = Try(List(ProjectedSirVal(0).toLong.asInstanceOf[Long])).getOrElse(null),
        PostIcRsrq = Try(List(PostIcRsrqVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        NumberofSubPackets = NumberofSubPacketsVal,
        SubPacketID = Try(List(SubPacketIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        SubPacketSize = Try(List(SubPacketSizeVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        EARFCN_B193 = Try(List(EARFCNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        NumofCell = Try(List(NumofCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        //              horxd_mode = Try(List(horxd_modeVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        InstRSSI = Try(List(InstRSSIVal(0).toDouble.asInstanceOf[Double])).getOrElse(null)
      )
      logRecord = logRecord.copy(logRecordName = "B193", logCode = "45459",
        hexLogCode = "0xB193",
        version = 24, logRecordB193 = logRecordB193Obj)
    }

    def parse35 = {
      val bean45459_35 = process45459_35(x, exceptionUseArray)
      //logger.info("0xB193_35 json >>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45459_35))
      val cinr0Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.CINRRX0)))).flatten.flatten.flatten
      val cinr1Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.CINRRX1)))).flatten.flatten.flatten
      val cinr2Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.CINRRX2)))).flatten.flatten.flatten
      val cinr3Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.CINRRX3)))).flatten.flatten.flatten
      val PhysicalCellIDVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.PhysicalCellID)))).flatten.flatten.flatten
      val ServingCellIndex_B193Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.ServingCellIndex)))).flatten.flatten.flatten
      val IsServingCellVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.IsServingCell)))).flatten.flatten.flatten
      val CurrentSFNVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.CurrentSFN)))).flatten.flatten.flatten
      val CurrentSubframeNumberVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.CurrentSubframeNumber)))).flatten.flatten.flatten
      val IsRestrictedVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.IsRestricted)))).flatten.flatten.flatten
      val CellTiming_0Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.CellTiming0)))).flatten.flatten.flatten
      val CellTiming_1Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.CellTiming1)))).flatten.flatten.flatten
      val CellTimingSFN_0Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.CellTimingSFN0)))).flatten.flatten.flatten
      val CellTimingSFN_1Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.CellTimingSFN1)))).flatten.flatten.flatten
      val InstRSRPRx_0Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstRSRPRx0)))).flatten.flatten.flatten
      val InstRSRPRx_1Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstRSRPRx1)))).flatten.flatten.flatten
      val InstRSRPRx_2Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstRSRPRx2)))).flatten.flatten.flatten
      val InstRSRPRx_3Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstRSRPRx3)))).flatten.flatten.flatten
      val InstMeasuredRSRPVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstMeasuredRSRP)))).flatten.flatten.flatten
      val InstRSRQRx_0Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstRSRQRx0)))).flatten.flatten.flatten
      val InstRSRQRx_1Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstRSRQRx1)))).flatten.flatten.flatten
      val InstRSRQRx_2Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstRSRQRx2)))).flatten.flatten.flatten
      val InstRSRQRx_3Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstRSRQRx3)))).flatten.flatten.flatten
      val InstRSRQVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstRSRQ)))).flatten.flatten.flatten
      val InstRSSIRx_0Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstRSSIRx0)))).flatten.flatten.flatten
      val InstRSSIRx_1Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstRSSIRx1)))).flatten.flatten.flatten
      val InstRSSIRx_2Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstRSSIRx2)))).flatten.flatten.flatten
      val InstRSSIRx_3Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstRSSIRx3)))).flatten.flatten.flatten
      val InstRSSIVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.InstRSSI)))).flatten.flatten.flatten
      val ResidualFrequencyErrorVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.ResidualFrequencyError)))).flatten.flatten.flatten
      val FTLSNRRx_0Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.FTLSNRRx0)))).flatten.flatten.flatten
      val FTLSNRRx_1Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.FTLSNRRx1)))).flatten.flatten.flatten
      val FTLSNRRx_2Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.FTLSNRRx2)))).flatten.flatten.flatten
      val FTLSNRRx_3Val = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.FTLSNRRx3)))).flatten.flatten.flatten
      val ProjectedSirVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.ProjectedSir)))).flatten.flatten.flatten
      val PostIcRsrqVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.sr_15315_inst_list.asScala.toList.map(_.PostIcRsrq)))).flatten.flatten.flatten
      val NumberofSubPacketsVal = bean45459_35.NumberofSubPackets
      val SubPacketIDVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.SubPacketID)
      val SubPacketSizeVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.SubPacketSize)).flatten
      val EARFCNVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
      val NumofCellVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.NumofCell))).flatten.flatten
      val ValidRxVal = bean45459_35.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15310_inst_list.asScala.toList.map(_.ValidRx))).flatten.flatten

      //          logger.info("0xB193 InstRSSIRx_0Val "+ InstRSSIRx_0Val)
      //          logger.info("0xB193 InstRSSIRx_1Val "+ InstRSSIRx_1Val)
      //          logger.info("0xB193 InstRSSIRx_2Val " + InstRSSIRx_2Val)
      //          logger.info("0xB193 InstRSSIRx_3Val " + InstRSSIRx_3Val)
      //          logger.info("0xB193 InstRSSIVal " + InstRSSIVal)

      logRecordB193Obj = logRecordB193Obj.copy(
        cinr0 = Try(List(cinr0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr1 = Try(List(cinr1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr2 = Try(List(cinr2Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr3 = Try(List(cinr3Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        PhysicalCellID_B193 = Try(List(PhysicalCellIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ServingCellIndex_B193 = Try(List(ServingCellIndex_B193Val(0))).getOrElse(null),
        IsServingCell = Try(List(IsServingCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSFN = Try(List(CurrentSFNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSubframeNumber = Try(List(CurrentSubframeNumberVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        IsRestricted = Try(List(IsRestrictedVal.asInstanceOf[Integer])).getOrElse(null),
        CellTiming_0 = Try(List(CellTiming_0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        CellTiming_1 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_0 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_1 = Try(List(CellTimingSFN_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        InstRSRPRx_0 = Try(List(InstRSRPRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_1 = Try(List(InstRSRPRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_2 = Try(List(InstRSRPRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_3 = Try(List(InstRSRPRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstMeasuredRSRP = Try(List(InstMeasuredRSRPVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_0 = Try(List(InstRSRQRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_1 = Try(List(InstRSRQRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_2 = Try(List(InstRSRQRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_3 = Try(List(InstRSRQRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQ = Try(List(InstRSRQVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_0 = Try(List(InstRSSIRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_1 = Try(List(InstRSSIRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_2 = Try(List(InstRSSIRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_3 = Try(List(InstRSSIRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ResidualFrequencyError = Try(List(ResidualFrequencyErrorVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        FTLSNRRx_0 = Try(List(FTLSNRRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_1 = Try(List(FTLSNRRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_2 = Try(List(FTLSNRRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_3 = Try(List(FTLSNRRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ProjectedSir = Try(List(ProjectedSirVal(0).toLong.asInstanceOf[Long])).getOrElse(null),
        PostIcRsrq = Try(List(PostIcRsrqVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        NumberofSubPackets = Try(NumberofSubPacketsVal.toInt.asInstanceOf[Integer]).getOrElse(null),
        SubPacketID = Try(List(SubPacketIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        SubPacketSize = Try(List(SubPacketSizeVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        EARFCN_B193 = Try(List(EARFCNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        NumofCell = Try(List(NumofCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ValidRx = Try(List(ValidRxVal(0))).getOrElse(null),
        InstRSSI = Try(List(InstRSSIVal(0).toDouble.asInstanceOf[Double])).getOrElse(null)
      )
      logRecord = logRecord.copy(logRecordName = "B193", logCode = "45459",
        hexLogCode = "0xB193", version = 35, logRecordB193 = logRecordB193Obj)
    }

    def parse18 = {
      val bean45459_18 = process45459_18(x, exceptionUseArray)
      //logger.info("0xB193_18 json >>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45459_18))
      val EARFCNVal = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
      val PhysicalCellIDVal = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.PhysicalCellID))).flatten.flatten
      val ServingCellIndexVal = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.ServingCellIndex))).flatten.flatten
      val IsServingCellVal = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.IsServingCell))).flatten.flatten
      val CurrentSFNVal = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.CurrentSFN))).flatten.flatten
      val CurrentSubframeNumberVal = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.CurrentSubframeNumber))).flatten.flatten
      val IsRestrictedVal = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.IsRestricted))).flatten.flatten
      val CellTiming_0Val = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.CellTiming0))).flatten.flatten
      val CellTiming_1Val = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.CellTiming1))).flatten.flatten
      val CellTimingSFN_0Val = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.CellTimingSFN0))).flatten.flatten
      val CellTimingSFN_1Val = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.CellTimingSFN1))).flatten.flatten
      val InstRSRPRx_0Val = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.InstRSRPRx0))).flatten.flatten
      val InstRSRPRx_1Val = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.InstRSRPRx1))).flatten.flatten
      val InstMeasuredRSRPVal = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.InstMeasuredRSRP))).flatten.flatten
      val InstRSRQRx_0Val = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.InstRSRQRx0))).flatten.flatten
      val InstRSRQRx_1Val = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.InstRSRQRx1))).flatten.flatten
      val InstRSRQVal = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.InstRSRQ))).flatten.flatten
      val InstRSSIRx_0Val = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.InstRSSIRx0))).flatten.flatten
      val InstRSSIRx_1Val = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.InstRSSIRx1))).flatten.flatten
      val InstRSSIVal = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.InstRSSI))).flatten.flatten
      val ResidualFrequencyErrorVal = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.ResidualFrequencyError))).flatten.flatten
      val SINRRx_0Val = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.SINRRx0))).flatten.flatten
      val SINRRx_1Val = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.SINRRx1))).flatten.flatten
      val ProjectedSirVal = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.ProjectedSir))).flatten.flatten
      val PostIcRsrqVal = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_15301_inst_list.asScala.toList.map(_.PostIcRsrq))).flatten.flatten
      val SubPacketSizeVal = bean45459_18.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.SubPacketSize)).flatten
      logRecordB193Obj = logRecordB193Obj.copy(
        PhysicalCellID_B193 = Try(List(PhysicalCellIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ServingCellIndex_B193 = Try(List(ServingCellIndexVal(0))).getOrElse(null),
        IsServingCell = Try(List(IsServingCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSFN = Try(List(CurrentSFNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSubframeNumber = Try(List(CurrentSubframeNumberVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTiming_0 = Try(List(CellTiming_0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        CellTiming_1 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_0 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_1 = Try(List(CellTimingSFN_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        InstRSRPRx_0 = Try(List(InstRSRPRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_1 = Try(List(InstRSRPRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstMeasuredRSRP = Try(List(InstMeasuredRSRPVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_0 = Try(List(InstRSRQRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_1 = Try(List(InstRSRQRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQ = Try(List(InstRSRQVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_0 = Try(List(InstRSSIRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_1 = Try(List(InstRSSIRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ResidualFrequencyError = Try(List(ResidualFrequencyErrorVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        FTLSNRRx_0 = Try(List(SINRRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_1 = Try(List(SINRRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        SubPacketSize = Try(List(SubPacketSizeVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ProjectedSir = Try(List(ProjectedSirVal(0).toLong.asInstanceOf[Long])).getOrElse(null),
        PostIcRsrq = Try(List(PostIcRsrqVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        EARFCN_B193 = Try(List(EARFCNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        InstRSSI = Try(List(InstRSSIVal(0).toDouble.asInstanceOf[Double])).getOrElse(null)
      )
      logRecord = logRecord.copy(logRecordName = "B193", logCode = "45459",
        hexLogCode = "0xB193",
        version = 18, logRecordB193 = logRecordB193Obj)
    }

    def parse36 = {
      val bean45459_36 = process45459_36(x, exceptionUseArray)
      //logger.info("0xB193_36 json >>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45459_36))
      val EARFCNVal = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
      val PhysicalCellIDVal = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.PhysicalCellID)))).flatten.flatten.flatten
      val ServingCellIndexVal = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.ServingCellIndex)))).flatten.flatten.flatten
      val IsServingCellVal = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.IsServingCell)))).flatten.flatten.flatten
      val CurrentSFNVal = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.CurrentSFN)))).flatten.flatten.flatten
      val CurrentSubframeNumberVal = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.CurrentSubframeNumber)))).flatten.flatten.flatten
      val IsRestrictedVal = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.IsRestricted)))).flatten.flatten.flatten
      val CellTiming_0Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.CellTiming0)))).flatten.flatten.flatten
      val CellTiming_1Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.CellTiming1)))).flatten.flatten.flatten
      val CellTimingSFN_0Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.CellTimingSFN0)))).flatten.flatten.flatten
      val CellTimingSFN_1Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.CellTimingSFN1)))).flatten.flatten.flatten
      val InstRSRPRx_0Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.InstRSRPRx0)))).flatten.flatten.flatten
      val InstRSRPRx_1Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.InstRSRPRx1)))).flatten.flatten.flatten
      val InstMeasuredRSRPVal = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.InstMeasuredRSRP)))).flatten.flatten.flatten
      val InstRSRQRx_0Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.InstRSRQRx0)))).flatten.flatten.flatten
      val InstRSRQRx_1Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.InstRSRQRx1)))).flatten.flatten.flatten
      val InstRSRQVal = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.InstRSRQ)))).flatten.flatten.flatten
      val InstRSSIRx_0Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.InstRSSIRx0)))).flatten.flatten.flatten
      val InstRSSIRx_1Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.InstRSSIRx1)))).flatten.flatten.flatten
      val InstRSSIVal = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.InstRSSI)))).flatten.flatten.flatten
      val ResidualFrequencyErrorVal = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.ResidualFrequencyError)))).flatten.flatten.flatten
      val cinr0Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.CINRRX0)))).flatten.flatten.flatten
      val cinr1Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.CINRRX1)))).flatten.flatten.flatten
      val cinr2Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.CINRRX2)))).flatten.flatten.flatten
      val cinr3Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.CINRRX3)))).flatten.flatten.flatten
      val ProjectedSirVal = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.ProjectedSir)))).flatten.flatten.flatten
      val PostIcRsrqVal = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.PostIcRsrq)))).flatten.flatten.flatten
      val SubPacketSizeVal = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.SubPacketSize)).flatten
      val FTLSNRRx_0Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.FTLSNRRx0)))).flatten.flatten.flatten
      val FTLSNRRx_1Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.FTLSNRRx1)))).flatten.flatten.flatten
      val FTLSNRRx_2Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.FTLSNRRx2)))).flatten.flatten.flatten
      val FTLSNRRx_3Val = bean45459_36.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_36173_inst_list.asScala.toList.map(_.sr_36176_inst_list.asScala.toList.map(_.FTLSNRRx3)))).flatten.flatten.flatten

      logRecordB193Obj = logRecordB193Obj.copy(
        PhysicalCellID_B193 = Try(List(PhysicalCellIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ServingCellIndex_B193 = Try(List(ServingCellIndexVal(0))).getOrElse(null),
        IsServingCell = Try(List(IsServingCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSFN = Try(List(CurrentSFNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSubframeNumber = Try(List(CurrentSubframeNumberVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        IsRestricted = Try(List(IsRestrictedVal.asInstanceOf[Integer])).getOrElse(null),
        CellTiming_0 = Try(List(CellTiming_0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        CellTiming_1 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_0 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_1 = Try(List(CellTimingSFN_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        InstRSRPRx_0 = Try(List(InstRSRPRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_1 = Try(List(InstRSRPRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstMeasuredRSRP = Try(List(InstMeasuredRSRPVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_0 = Try(List(InstRSRQRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_1 = Try(List(InstRSRQRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQ = Try(List(InstRSRQVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_0 = Try(List(InstRSSIRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_1 = Try(List(InstRSSIRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ResidualFrequencyError = Try(List(ResidualFrequencyErrorVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr0 = Try(List(cinr0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr1 = Try(List(cinr1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr2 = Try(List(cinr2Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr3 = Try(List(cinr3Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        FTLSNRRx_0 = Try(List(FTLSNRRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_1 = Try(List(FTLSNRRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_2 = Try(List(FTLSNRRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_3 = Try(List(FTLSNRRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        SubPacketSize = Try(List(SubPacketSizeVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ProjectedSir = Try(List(ProjectedSirVal(0).toLong.asInstanceOf[Long])).getOrElse(null),
        PostIcRsrq = Try(List(PostIcRsrqVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        EARFCN_B193 = Try(List(EARFCNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        InstRSSI = Try(List(InstRSSIVal(0).toDouble.asInstanceOf[Double])).getOrElse(null)
      )
      logRecord = logRecord.copy(logRecordName = "B193", logCode = "45459",
        hexLogCode = "0xB193",
        version = 36, logRecordB193 = logRecordB193Obj)
    }

    def parse39 = {
      val bean45459_39 = process45459_39(x, exceptionUseArray)
      val cinr0Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.CINRRX0)))).flatten.flatten.flatten
      val cinr1Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.CINRRX1)))).flatten.flatten.flatten
      val cinr2Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.CINRRX2)))).flatten.flatten.flatten
      val cinr3Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.CINRRX3)))).flatten.flatten.flatten
      val PhysicalCellIDVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.PhysicalCellID)))).flatten.flatten.flatten
      val ServingCellIndex_B193Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.ServingCellIndex)))).flatten.flatten.flatten
      val IsServingCellVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.IsServingCell)))).flatten.flatten.flatten
      val CurrentSFNVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.CurrentSFN)))).flatten.flatten.flatten
      val CurrentSubframeNumberVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.CurrentSubframeNumber)))).flatten.flatten.flatten
      val IsRestrictedVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.IsRestricted)))).flatten.flatten.flatten
      val CellTiming_0Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.CellTiming0)))).flatten.flatten.flatten
      val CellTiming_1Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.CellTiming1)))).flatten.flatten.flatten
      val CellTimingSFN_0Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.CellTimingSFN0)))).flatten.flatten.flatten
      val CellTimingSFN_1Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.CellTimingSFN1)))).flatten.flatten.flatten
      val InstRSRPRx_0Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstRSRPRx0)))).flatten.flatten.flatten
      val InstRSRPRx_1Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstRSRPRx1)))).flatten.flatten.flatten
      val InstRSRPRx_2Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstRSRPRx2)))).flatten.flatten.flatten
      val InstRSRPRx_3Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstRSRPRx3)))).flatten.flatten.flatten
      val InstMeasuredRSRPVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstMeasuredRSRP)))).flatten.flatten.flatten
      val InstRSRQRx_0Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstRSRQRx0)))).flatten.flatten.flatten
      val InstRSRQRx_1Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstRSRQRx1)))).flatten.flatten.flatten
      val InstRSRQRx_2Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstRSRQRx2)))).flatten.flatten.flatten
      val InstRSRQRx_3Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstRSRQRx3)))).flatten.flatten.flatten
      val InstRSRQVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstRSRQ)))).flatten.flatten.flatten
      val InstRSSIRx_0Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstRSSIRx0)))).flatten.flatten.flatten
      val InstRSSIRx_1Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstRSSIRx1)))).flatten.flatten.flatten
      val InstRSSIRx_2Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstRSSIRx2)))).flatten.flatten.flatten
      val InstRSSIRx_3Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstRSSIRx3)))).flatten.flatten.flatten
      val InstRSSIVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.InstRSSI)))).flatten.flatten.flatten
      val ResidualFrequencyErrorVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.ResidualFrequencyError)))).flatten.flatten.flatten
      val FTLSNRRx_0Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.FTLSNRRx0)))).flatten.flatten.flatten
      val FTLSNRRx_1Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.FTLSNRRx1)))).flatten.flatten.flatten
      val FTLSNRRx_2Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.FTLSNRRx2)))).flatten.flatten.flatten
      val FTLSNRRx_3Val = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.FTLSNRRx3)))).flatten.flatten.flatten
      val ProjectedSirVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.ProjectedSir)))).flatten.flatten.flatten
      val PostIcRsrqVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.sr_103649_inst_list.asScala.toList.map(_.PostIcRsrq)))).flatten.flatten.flatten
      val NumberofSubPacketsVal = bean45459_39.NumberofSubPackets
      val SubPacketIDVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.SubPacketID)
      val SubPacketSizeVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.SubPacketSize)).flatten
      val EARFCNVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
      val NumofCellVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.NumofCell))).flatten.flatten
      val ValidRxVal = bean45459_39.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_103611_inst_list.asScala.toList.map(_.ValidRx))).flatten.flatten
      logRecordB193Obj = logRecordB193Obj.copy(
        cinr0 = Try(List(cinr0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr1 = Try(List(cinr1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr2 = Try(List(cinr2Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr3 = Try(List(cinr3Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        PhysicalCellID_B193 = Try(List(PhysicalCellIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ServingCellIndex_B193 = Try(List(ServingCellIndex_B193Val(0))).getOrElse(null),
        IsServingCell = Try(List(IsServingCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSFN = Try(List(CurrentSFNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSubframeNumber = Try(List(CurrentSubframeNumberVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        IsRestricted = Try(List(IsRestrictedVal.asInstanceOf[Integer])).getOrElse(null),
        CellTiming_0 = Try(List(CellTiming_0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        CellTiming_1 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_0 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_1 = Try(List(CellTimingSFN_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        InstRSRPRx_0 = Try(List(InstRSRPRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_1 = Try(List(InstRSRPRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_2 = Try(List(InstRSRPRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_3 = Try(List(InstRSRPRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstMeasuredRSRP = Try(List(InstMeasuredRSRPVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_0 = Try(List(InstRSRQRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_1 = Try(List(InstRSRQRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_2 = Try(List(InstRSRQRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_3 = Try(List(InstRSRQRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQ = Try(List(InstRSRQVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_0 = Try(List(InstRSSIRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_1 = Try(List(InstRSSIRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_2 = Try(List(InstRSSIRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_3 = Try(List(InstRSSIRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ResidualFrequencyError = Try(List(ResidualFrequencyErrorVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        FTLSNRRx_0 = Try(List(FTLSNRRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_1 = Try(List(FTLSNRRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_2 = Try(List(FTLSNRRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_3 = Try(List(FTLSNRRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ProjectedSir = Try(List(ProjectedSirVal(0).toLong.asInstanceOf[Long])).getOrElse(null),
        PostIcRsrq = Try(List(PostIcRsrqVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        NumberofSubPackets = Try(NumberofSubPacketsVal.toInt.asInstanceOf[Integer]).getOrElse(null),
        SubPacketID = Try(List(SubPacketIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        SubPacketSize = Try(List(SubPacketSizeVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        EARFCN_B193 = Try(List(EARFCNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        NumofCell = Try(List(NumofCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ValidRx = Try(List(ValidRxVal(0))).getOrElse(null),
        InstRSSI = Try(List(InstRSSIVal(0).toDouble.asInstanceOf[Double])).getOrElse(null)
      )
      logRecord = logRecord.copy(logRecordName = "B193", logCode = "45459",
        hexLogCode = "0xB193", version = 39, logRecordB193 = logRecordB193Obj)
    }

    def parse40 = {
      val bean45459_40 = process45459_40(x, exceptionUseArray)
      //logger.info("0xB193_40 json >>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45459_40))
      val cinr0Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.CINRRX0)))).flatten.flatten.flatten
      val cinr1Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.CINRRX1)))).flatten.flatten.flatten
      val cinr2Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.CINRRX2)))).flatten.flatten.flatten
      val cinr3Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.CINRRX3)))).flatten.flatten.flatten
      val PhysicalCellIDVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.PhysicalCellID)))).flatten.flatten.flatten
      val ServingCellIndex_B193Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.ServingCellIndex)))).flatten.flatten.flatten
      val IsServingCellVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.IsServingCell)))).flatten.flatten.flatten
      val CurrentSFNVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.CurrentSFN)))).flatten.flatten.flatten
      val CurrentSubframeNumberVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.CurrentSubframeNumber)))).flatten.flatten.flatten
      val IsRestrictedVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.IsRestricted)))).flatten.flatten.flatten
      val CellTiming_0Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.CellTiming0)))).flatten.flatten.flatten
      val CellTiming_1Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.CellTiming1)))).flatten.flatten.flatten
      val CellTimingSFN_0Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.CellTimingSFN0)))).flatten.flatten.flatten
      val CellTimingSFN_1Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.CellTimingSFN1)))).flatten.flatten.flatten
      val InstRSRPRx_0Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstRSRPRx0)))).flatten.flatten.flatten
      val InstRSRPRx_1Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstRSRPRx1)))).flatten.flatten.flatten
      val InstRSRPRx_2Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstRSRPRx2)))).flatten.flatten.flatten
      val InstRSRPRx_3Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstRSRPRx3)))).flatten.flatten.flatten
      val InstMeasuredRSRPVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstMeasuredRSRP)))).flatten.flatten.flatten
      val InstRSRQRx_0Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstRSRQRx0)))).flatten.flatten.flatten
      val InstRSRQRx_1Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstRSRQRx1)))).flatten.flatten.flatten
      val InstRSRQRx_2Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstRSRQRx2)))).flatten.flatten.flatten
      val InstRSRQRx_3Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstRSRQRx3)))).flatten.flatten.flatten
      val InstRSRQVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstRSRQ)))).flatten.flatten.flatten
      val InstRSSIRx_0Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstRSSIRx0)))).flatten.flatten.flatten
      val InstRSSIRx_1Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstRSSIRx1)))).flatten.flatten.flatten
      val InstRSSIRx_2Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstRSSIRx2)))).flatten.flatten.flatten
      val InstRSSIRx_3Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstRSSIRx3)))).flatten.flatten.flatten
      val InstRSSIVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.InstRSSI)))).flatten.flatten.flatten
      val ResidualFrequencyErrorVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.ResidualFrequencyError)))).flatten.flatten.flatten
      val FTLSNRRx_0Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.FTLSNRRx0)))).flatten.flatten.flatten
      val FTLSNRRx_1Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.FTLSNRRx1)))).flatten.flatten.flatten
      val FTLSNRRx_2Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.FTLSNRRx2)))).flatten.flatten.flatten
      val FTLSNRRx_3Val = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.FTLSNRRx3)))).flatten.flatten.flatten
      val ProjectedSirVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.ProjectedSir)))).flatten.flatten.flatten
      val PostIcRsrqVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.sr_41075_inst_list.asScala.toList.map(_.PostIcRsrq)))).flatten.flatten.flatten
      val NumberofSubPacketsVal = bean45459_40.NumberofSubPackets
      val SubPacketIDVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.SubPacketID)
      val SubPacketSizeVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.SubPacketSize)).flatten
      val EARFCNVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
      val NumofCellVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.NumofCell))).flatten.flatten
      val ValidRxVal = bean45459_40.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_41072_inst_list.asScala.toList.map(_.ValidRx))).flatten.flatten
      logRecordB193Obj = logRecordB193Obj.copy(
        cinr0 = Try(List(cinr0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr1 = Try(List(cinr1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr2 = Try(List(cinr2Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr3 = Try(List(cinr3Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        PhysicalCellID_B193 = Try(List(PhysicalCellIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ServingCellIndex_B193 = Try(List(ServingCellIndex_B193Val(0))).getOrElse(null),
        IsServingCell = Try(List(IsServingCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSFN = Try(List(CurrentSFNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSubframeNumber = Try(List(CurrentSubframeNumberVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        IsRestricted = Try(List(IsRestrictedVal.asInstanceOf[Integer])).getOrElse(null),
        CellTiming_0 = Try(List(CellTiming_0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        CellTiming_1 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_0 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_1 = Try(List(CellTimingSFN_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        InstRSRPRx_0 = Try(List(InstRSRPRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_1 = Try(List(InstRSRPRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_2 = Try(List(InstRSRPRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_3 = Try(List(InstRSRPRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstMeasuredRSRP = Try(List(InstMeasuredRSRPVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_0 = Try(List(InstRSRQRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_1 = Try(List(InstRSRQRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_2 = Try(List(InstRSRQRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_3 = Try(List(InstRSRQRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQ = Try(List(InstRSRQVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_0 = Try(List(InstRSSIRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_1 = Try(List(InstRSSIRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_2 = Try(List(InstRSSIRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_3 = Try(List(InstRSSIRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ResidualFrequencyError = Try(List(ResidualFrequencyErrorVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        FTLSNRRx_0 = Try(List(FTLSNRRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_1 = Try(List(FTLSNRRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_2 = Try(List(FTLSNRRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_3 = Try(List(FTLSNRRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ProjectedSir = Try(List(ProjectedSirVal(0).toLong.asInstanceOf[Long])).getOrElse(null),
        PostIcRsrq = Try(List(PostIcRsrqVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        NumberofSubPackets = Try(NumberofSubPacketsVal.toInt.asInstanceOf[Integer]).getOrElse(null),
        SubPacketID = Try(List(SubPacketIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        SubPacketSize = Try(List(SubPacketSizeVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        EARFCN_B193 = Try(List(EARFCNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        NumofCell = Try(List(NumofCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ValidRx = Try(List(ValidRxVal(0))).getOrElse(null),
        InstRSSI = Try(List(InstRSSIVal(0).toDouble.asInstanceOf[Double])).getOrElse(null)
      )
      logRecord = logRecord.copy(logRecordName = "B193", logCode = "45459",
        hexLogCode = "0xB193", version = 40, logRecordB193 = logRecordB193Obj)
    }

    def parse41 = {
      val bean45459_41 = process45459_41(x, exceptionUseArray)
      //logger.info("0xB193_41 json >>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45459_41))
      val cinr0Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.CINRRX0)))).flatten.flatten.flatten
      val cinr1Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.CINRRX1)))).flatten.flatten.flatten
      val cinr2Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.CINRRX2)))).flatten.flatten.flatten
      val cinr3Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.CINRRX3)))).flatten.flatten.flatten
      val PhysicalCellIDVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.PhysicalCellID)))).flatten.flatten.flatten
      val ServingCellIndex_B193Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.ServingCellIndex)))).flatten.flatten.flatten
      val IsServingCellVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.IsServingCell)))).flatten.flatten.flatten
      val CurrentSFNVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.CurrentSFN)))).flatten.flatten.flatten
      val CurrentSubframeNumberVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.CurrentSubframeNumber)))).flatten.flatten.flatten
      val IsRestrictedVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.IsRestricted)))).flatten.flatten.flatten
      val CellTiming_0Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.CellTiming0)))).flatten.flatten.flatten
      val CellTiming_1Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.CellTiming1)))).flatten.flatten.flatten
      val CellTimingSFN_0Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.CellTimingSFN0)))).flatten.flatten.flatten
      val CellTimingSFN_1Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.CellTimingSFN1)))).flatten.flatten.flatten
      val InstRSRPRx_0Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstRSRPRx0)))).flatten.flatten.flatten
      val InstRSRPRx_1Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstRSRPRx1)))).flatten.flatten.flatten
      val InstRSRPRx_2Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstRSRPRx2)))).flatten.flatten.flatten
      val InstRSRPRx_3Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstRSRPRx3)))).flatten.flatten.flatten
      val InstMeasuredRSRPVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstMeasuredRSRP)))).flatten.flatten.flatten
      val InstRSRQRx_0Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstRSRQRx0)))).flatten.flatten.flatten
      val InstRSRQRx_1Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstRSRQRx1)))).flatten.flatten.flatten
      val InstRSRQRx_2Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstRSRQRx2)))).flatten.flatten.flatten
      val InstRSRQRx_3Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstRSRQRx3)))).flatten.flatten.flatten
      val InstRSRQVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstRSRQ)))).flatten.flatten.flatten
      val InstRSSIRx_0Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstRSSIRx0)))).flatten.flatten.flatten
      val InstRSSIRx_1Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstRSSIRx1)))).flatten.flatten.flatten
      val InstRSSIRx_2Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstRSSIRx2)))).flatten.flatten.flatten
      val InstRSSIRx_3Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstRSSIRx3)))).flatten.flatten.flatten
      val InstRSSIVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.InstRSSI)))).flatten.flatten.flatten
      val ResidualFrequencyErrorVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.ResidualFrequencyError)))).flatten.flatten.flatten
      val FTLSNRRx_0Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.FTLSNRRx0)))).flatten.flatten.flatten
      val FTLSNRRx_1Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.FTLSNRRx1)))).flatten.flatten.flatten
      val FTLSNRRx_2Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.FTLSNRRx2)))).flatten.flatten.flatten
      val FTLSNRRx_3Val = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.FTLSNRRx3)))).flatten.flatten.flatten
      val ProjectedSirVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.ProjectedSir)))).flatten.flatten.flatten
      val PostIcRsrqVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.sr_88214_inst_list.asScala.toList.map(_.PostIcRsrq)))).flatten.flatten.flatten
      val NumberofSubPacketsVal = bean45459_41.NumberofSubPackets
      val SubPacketIDVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.SubPacketID)
      val SubPacketSizeVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.SubPacketSize)).flatten
      val EARFCNVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
      val NumofCellVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.NumofCell))).flatten.flatten
      val ValidRxVal = bean45459_41.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_88177_inst_list.asScala.toList.map(_.ValidRx))).flatten.flatten
      logRecordB193Obj = logRecordB193Obj.copy(
        cinr0 = Try(List(cinr0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr1 = Try(List(cinr1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr2 = Try(List(cinr2Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr3 = Try(List(cinr3Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        PhysicalCellID_B193 = Try(List(PhysicalCellIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ServingCellIndex_B193 = Try(List(ServingCellIndex_B193Val(0))).getOrElse(null),
        IsServingCell = Try(List(IsServingCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSFN = Try(List(CurrentSFNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSubframeNumber = Try(List(CurrentSubframeNumberVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        IsRestricted = Try(List(IsRestrictedVal.asInstanceOf[Integer])).getOrElse(null),
        CellTiming_0 = Try(List(CellTiming_0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        CellTiming_1 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_0 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_1 = Try(List(CellTimingSFN_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        InstRSRPRx_0 = Try(List(InstRSRPRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_1 = Try(List(InstRSRPRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_2 = Try(List(InstRSRPRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_3 = Try(List(InstRSRPRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstMeasuredRSRP = Try(List(InstMeasuredRSRPVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_0 = Try(List(InstRSRQRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_1 = Try(List(InstRSRQRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_2 = Try(List(InstRSRQRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_3 = Try(List(InstRSRQRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQ = Try(List(InstRSRQVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_0 = Try(List(InstRSSIRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_1 = Try(List(InstRSSIRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_2 = Try(List(InstRSSIRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_3 = Try(List(InstRSSIRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ResidualFrequencyError = Try(List(ResidualFrequencyErrorVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        FTLSNRRx_0 = Try(List(FTLSNRRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_1 = Try(List(FTLSNRRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_2 = Try(List(FTLSNRRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_3 = Try(List(FTLSNRRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ProjectedSir = Try(List(ProjectedSirVal(0).toLong.asInstanceOf[Long])).getOrElse(null),
        PostIcRsrq = Try(List(PostIcRsrqVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        NumberofSubPackets = Try(NumberofSubPacketsVal.toInt.asInstanceOf[Integer]).getOrElse(null),
        SubPacketID = Try(List(SubPacketIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        SubPacketSize = Try(List(SubPacketSizeVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        EARFCN_B193 = Try(List(EARFCNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        NumofCell = Try(List(NumofCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ValidRx = Try(List(ValidRxVal(0))).getOrElse(null),
        InstRSSI = Try(List(InstRSSIVal(0).toDouble.asInstanceOf[Double])).getOrElse(null)
      )
      logRecord = logRecord.copy(logRecordName = "B193", logCode = "45459",
        hexLogCode = "0xB193", version = 41, logRecordB193 = logRecordB193Obj)
    }

    def parse42 = {
      val bean45459_42 = process45459_42(x, exceptionUseArray)
      val cinr0Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.CINRRX0)))).flatten.flatten.flatten
      val cinr1Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.CINRRX1)))).flatten.flatten.flatten
      val cinr2Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.CINRRX2)))).flatten.flatten.flatten
      val cinr3Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.CINRRX3)))).flatten.flatten.flatten
      val PhysicalCellIDVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.PhysicalCellID)))).flatten.flatten.flatten
      val ServingCellIndex_B193Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.ServingCellIndex)))).flatten.flatten.flatten
      val IsServingCellVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.IsServingCell)))).flatten.flatten.flatten
      val CurrentSFNVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.CurrentSFN)))).flatten.flatten.flatten
      val CurrentSubframeNumberVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.CurrentSubframeNumber)))).flatten.flatten.flatten
      val IsRestrictedVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.IsRestricted)))).flatten.flatten.flatten
      val CellTiming_0Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.CellTiming0)))).flatten.flatten.flatten
      val CellTiming_1Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.CellTiming1)))).flatten.flatten.flatten
      val CellTimingSFN_0Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.CellTimingSFN0)))).flatten.flatten.flatten
      val CellTimingSFN_1Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.CellTimingSFN1)))).flatten.flatten.flatten
      val InstRSRPRx_0Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstRSRPRx0)))).flatten.flatten.flatten
      val InstRSRPRx_1Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstRSRPRx1)))).flatten.flatten.flatten
      val InstRSRPRx_2Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstRSRPRx2)))).flatten.flatten.flatten
      val InstRSRPRx_3Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstRSRPRx3)))).flatten.flatten.flatten
      val InstMeasuredRSRPVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstMeasuredRSRP)))).flatten.flatten.flatten
      val InstRSRQRx_0Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstRSRQRx0)))).flatten.flatten.flatten
      val InstRSRQRx_1Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstRSRQRx1)))).flatten.flatten.flatten
      val InstRSRQRx_2Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstRSRQRx2)))).flatten.flatten.flatten
      val InstRSRQRx_3Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstRSRQRx3)))).flatten.flatten.flatten
      val InstRSRQVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstRSRQ)))).flatten.flatten.flatten
      val InstRSSIRx_0Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstRSSIRx0)))).flatten.flatten.flatten
      val InstRSSIRx_1Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstRSSIRx1)))).flatten.flatten.flatten
      val InstRSSIRx_2Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstRSSIRx2)))).flatten.flatten.flatten
      val InstRSSIRx_3Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstRSSIRx3)))).flatten.flatten.flatten
      val InstRSSIVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.InstRSSI)))).flatten.flatten.flatten
      val ResidualFrequencyErrorVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.ResidualFrequencyError)))).flatten.flatten.flatten
      val FTLSNRRx_0Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.FTLSNRRx0)))).flatten.flatten.flatten
      val FTLSNRRx_1Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.FTLSNRRx1)))).flatten.flatten.flatten
      val FTLSNRRx_2Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.FTLSNRRx2)))).flatten.flatten.flatten
      val FTLSNRRx_3Val = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.FTLSNRRx3)))).flatten.flatten.flatten
      val ProjectedSirVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.ProjectedSir)))).flatten.flatten.flatten
      val PostIcRsrqVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.sr_100516_inst_list.asScala.toList.map(_.PostIcRsrq)))).flatten.flatten.flatten
      val NumberofSubPacketsVal = bean45459_42.NumberofSubPackets
      val SubPacketIDVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.SubPacketID)
      val SubPacketSizeVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.SubPacketSize)).flatten
      val EARFCNVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
      val NumofCellVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.NumofCell))).flatten.flatten
      val ValidRxVal = bean45459_42.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100453_inst_list.asScala.toList.map(_.ValidRx))).flatten.flatten
      logRecordB193Obj = logRecordB193Obj.copy(
        cinr0 = Try(List(cinr0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr1 = Try(List(cinr1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr2 = Try(List(cinr2Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr3 = Try(List(cinr3Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        PhysicalCellID_B193 = Try(List(PhysicalCellIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ServingCellIndex_B193 = Try(List(ServingCellIndex_B193Val(0))).getOrElse(null),
        IsServingCell = Try(List(IsServingCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSFN = Try(List(CurrentSFNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSubframeNumber = Try(List(CurrentSubframeNumberVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        IsRestricted = Try(List(IsRestrictedVal.asInstanceOf[Integer])).getOrElse(null),
        CellTiming_0 = Try(List(CellTiming_0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        CellTiming_1 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_0 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_1 = Try(List(CellTimingSFN_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        InstRSRPRx_0 = Try(List(InstRSRPRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_1 = Try(List(InstRSRPRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_2 = Try(List(InstRSRPRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_3 = Try(List(InstRSRPRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstMeasuredRSRP = Try(List(InstMeasuredRSRPVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_0 = Try(List(InstRSRQRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_1 = Try(List(InstRSRQRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_2 = Try(List(InstRSRQRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_3 = Try(List(InstRSRQRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQ = Try(List(InstRSRQVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_0 = Try(List(InstRSSIRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_1 = Try(List(InstRSSIRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_2 = Try(List(InstRSSIRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_3 = Try(List(InstRSSIRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ResidualFrequencyError = Try(List(ResidualFrequencyErrorVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        FTLSNRRx_0 = Try(List(FTLSNRRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_1 = Try(List(FTLSNRRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_2 = Try(List(FTLSNRRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_3 = Try(List(FTLSNRRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ProjectedSir = Try(List(ProjectedSirVal(0).toLong.asInstanceOf[Long])).getOrElse(null),
        PostIcRsrq = Try(List(PostIcRsrqVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        NumberofSubPackets = Try(NumberofSubPacketsVal.toInt.asInstanceOf[Integer]).getOrElse(null),
        SubPacketID = Try(List(SubPacketIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        SubPacketSize = Try(List(SubPacketSizeVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        EARFCN_B193 = Try(List(EARFCNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        NumofCell = Try(List(NumofCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ValidRx = Try(List(ValidRxVal(0))).getOrElse(null),
        InstRSSI = Try(List(InstRSSIVal(0).toDouble.asInstanceOf[Double])).getOrElse(null)
      )
      logRecord = logRecord.copy(logRecordName = "B193", logCode = "45459",
        hexLogCode = "0xB193", version = 42, logRecordB193 = logRecordB193Obj)
    }

    def parse48 = {
      val bean45459_48 = process45459_48(x, exceptionUseArray)
      //logger.info("0xB193_48 json >>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45459_48))
      val cinr0Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.CINRRX0)))).flatten.flatten.flatten
      val cinr1Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.CINRRX1)))).flatten.flatten.flatten
      val cinr2Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.CINRRX2)))).flatten.flatten.flatten
      val cinr3Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.CINRRX3)))).flatten.flatten.flatten
      val PhysicalCellIDVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.PhysicalCellID)))).flatten.flatten.flatten
      val ServingCellIndex_B193Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.ServingCellIndex)))).flatten.flatten.flatten
      val IsServingCellVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.IsServingCell)))).flatten.flatten.flatten
      val CurrentSFNVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.CurrentSFN)))).flatten.flatten.flatten
      val CurrentSubframeNumberVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.CurrentSubframeNumber)))).flatten.flatten.flatten
      val IsRestrictedVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.IsRestricted)))).flatten.flatten.flatten
      val CellTiming_0Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.CellTiming0)))).flatten.flatten.flatten
      val CellTiming_1Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.CellTiming1)))).flatten.flatten.flatten
      val CellTimingSFN_0Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.CellTimingSFN0)))).flatten.flatten.flatten
      val CellTimingSFN_1Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.CellTimingSFN1)))).flatten.flatten.flatten
      val InstRSRPRx_0Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstRSRPRx0)))).flatten.flatten.flatten
      val InstRSRPRx_1Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstRSRPRx1)))).flatten.flatten.flatten
      val InstRSRPRx_2Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstRSRPRx2)))).flatten.flatten.flatten
      val InstRSRPRx_3Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstRSRPRx3)))).flatten.flatten.flatten
      val InstMeasuredRSRPVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstMeasuredRSRP)))).flatten.flatten.flatten
      val InstRSRQRx_0Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstRSRQRx0)))).flatten.flatten.flatten
      val InstRSRQRx_1Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstRSRQRx1)))).flatten.flatten.flatten
      val InstRSRQRx_2Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstRSRQRx2)))).flatten.flatten.flatten
      val InstRSRQRx_3Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstRSRQRx3)))).flatten.flatten.flatten
      val InstRSRQVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstRSRQ)))).flatten.flatten.flatten
      val InstRSSIRx_0Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstRSSIRx0)))).flatten.flatten.flatten
      val InstRSSIRx_1Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstRSSIRx1)))).flatten.flatten.flatten
      val InstRSSIRx_2Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstRSSIRx2)))).flatten.flatten.flatten
      val InstRSSIRx_3Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstRSSIRx3)))).flatten.flatten.flatten
      val InstRSSIVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.InstRSSI)))).flatten.flatten.flatten
      val ResidualFrequencyErrorVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.ResidualFrequencyError)))).flatten.flatten.flatten
      val FTLSNRRx_0Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.FTLSNRRx0)))).flatten.flatten.flatten
      val FTLSNRRx_1Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.FTLSNRRx1)))).flatten.flatten.flatten
      val FTLSNRRx_2Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.FTLSNRRx2)))).flatten.flatten.flatten
      val FTLSNRRx_3Val = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.FTLSNRRx3)))).flatten.flatten.flatten
      val ProjectedSirVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.ProjectedSir)))).flatten.flatten.flatten
      val PostIcRsrqVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.sr_100301_inst_list.asScala.toList.map(_.PostIcRsrq)))).flatten.flatten.flatten
      val NumberofSubPacketsVal = bean45459_48.NumberofSubPackets
      val SubPacketIDVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.SubPacketID)
      val SubPacketSizeVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.SubPacketSize)).flatten
      val EARFCNVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
      val NumofCellVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.NumofCell))).flatten.flatten
      val ValidRxVal = bean45459_48.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_100238_inst_list.asScala.toList.map(_.ValidRx))).flatten.flatten
      logRecordB193Obj = logRecordB193Obj.copy(
        cinr0 = Try(List(cinr0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr1 = Try(List(cinr1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr2 = Try(List(cinr2Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr3 = Try(List(cinr3Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        PhysicalCellID_B193 = Try(List(PhysicalCellIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ServingCellIndex_B193 = Try(List(ServingCellIndex_B193Val(0))).getOrElse(null),
        IsServingCell = Try(List(IsServingCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSFN = Try(List(CurrentSFNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSubframeNumber = Try(List(CurrentSubframeNumberVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        IsRestricted = Try(List(IsRestrictedVal.asInstanceOf[Integer])).getOrElse(null),
        CellTiming_0 = Try(List(CellTiming_0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        CellTiming_1 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_0 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_1 = Try(List(CellTimingSFN_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        InstRSRPRx_0 = Try(List(InstRSRPRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_1 = Try(List(InstRSRPRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_2 = Try(List(InstRSRPRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_3 = Try(List(InstRSRPRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstMeasuredRSRP = Try(List(InstMeasuredRSRPVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_0 = Try(List(InstRSRQRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_1 = Try(List(InstRSRQRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_2 = Try(List(InstRSRQRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_3 = Try(List(InstRSRQRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQ = Try(List(InstRSRQVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_0 = Try(List(InstRSSIRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_1 = Try(List(InstRSSIRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_2 = Try(List(InstRSSIRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_3 = Try(List(InstRSSIRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ResidualFrequencyError = Try(List(ResidualFrequencyErrorVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        FTLSNRRx_0 = Try(List(FTLSNRRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_1 = Try(List(FTLSNRRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_2 = Try(List(FTLSNRRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_3 = Try(List(FTLSNRRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ProjectedSir = Try(List(ProjectedSirVal(0).toLong.asInstanceOf[Long])).getOrElse(null),
        PostIcRsrq = Try(List(PostIcRsrqVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        NumberofSubPackets = Try(NumberofSubPacketsVal.toInt.asInstanceOf[Integer]).getOrElse(null),
        SubPacketID = Try(List(SubPacketIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        SubPacketSize = Try(List(SubPacketSizeVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        EARFCN_B193 = Try(List(EARFCNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        NumofCell = Try(List(NumofCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ValidRx = Try(List(ValidRxVal(0))).getOrElse(null),
        InstRSSI = Try(List(InstRSSIVal(0).toDouble.asInstanceOf[Double])).getOrElse(null)
      )
      logRecord = logRecord.copy(logRecordName = "B193", logCode = "45459",
        hexLogCode = "0xB193", version = 48, logRecordB193 = logRecordB193Obj)
    }

    def parse50(b193Version: Int) = {
      val bean45459_50 = process45459_50(x, exceptionUseArray)
      //            logger.info("0xB193_50 json >>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45459_50))
      val cinr0Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.CINRRX0)))).flatten.flatten.flatten
      val cinr1Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.CINRRX1)))).flatten.flatten.flatten
      val cinr2Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.CINRRX2)))).flatten.flatten.flatten
      val cinr3Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.CINRRX3)))).flatten.flatten.flatten
      val PhysicalCellIDVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.PhysicalCellID)))).flatten.flatten.flatten
      val ServingCellIndex_B193Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.ServingCellIndex)))).flatten.flatten.flatten
      val IsServingCellVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.IsServingCell)))).flatten.flatten.flatten
      val CurrentSFNVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.CurrentSFN)))).flatten.flatten.flatten
      val CurrentSubframeNumberVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.CurrentSubframeNumber)))).flatten.flatten.flatten
      val IsRestrictedVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.IsRestricted)))).flatten.flatten.flatten
      val CellTiming_0Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.CellTiming0)))).flatten.flatten.flatten
      val CellTiming_1Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.CellTiming1)))).flatten.flatten.flatten
      val CellTimingSFN_0Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.CellTimingSFN0)))).flatten.flatten.flatten
      val CellTimingSFN_1Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.CellTimingSFN1)))).flatten.flatten.flatten
      val InstRSRPRx_0Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstRSRPRx0)))).flatten.flatten.flatten
      val InstRSRPRx_1Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstRSRPRx1)))).flatten.flatten.flatten
      val InstRSRPRx_2Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstRSRPRx2)))).flatten.flatten.flatten
      val InstRSRPRx_3Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstRSRPRx3)))).flatten.flatten.flatten
      val InstMeasuredRSRPVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstMeasuredRSRP)))).flatten.flatten.flatten
      val InstRSRQRx_0Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstRSRQRx0)))).flatten.flatten.flatten
      val InstRSRQRx_1Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstRSRQRx1)))).flatten.flatten.flatten
      val InstRSRQRx_2Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstRSRQRx2)))).flatten.flatten.flatten
      val InstRSRQRx_3Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstRSRQRx3)))).flatten.flatten.flatten
      val InstRSRQVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstRSRQ)))).flatten.flatten.flatten
      val InstRSSIRx_0Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstRSSIRx0)))).flatten.flatten.flatten
      val InstRSSIRx_1Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstRSSIRx1)))).flatten.flatten.flatten
      val InstRSSIRx_2Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstRSSIRx2)))).flatten.flatten.flatten
      val InstRSSIRx_3Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstRSSIRx3)))).flatten.flatten.flatten
      val InstRSSIVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.InstRSSI)))).flatten.flatten.flatten
      val ResidualFrequencyErrorVal = null //bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.ResidualFrequencyError)))).flatten.flatten.flatten
      val FTLSNRRx_0Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.FTLSNRRx0)))).flatten.flatten.flatten
      val FTLSNRRx_1Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.FTLSNRRx1)))).flatten.flatten.flatten
      val FTLSNRRx_2Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.FTLSNRRx2)))).flatten.flatten.flatten
      val FTLSNRRx_3Val = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.FTLSNRRx3)))).flatten.flatten.flatten
      val ProjectedSirVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.ProjectedSir)))).flatten.flatten.flatten
      val PostIcRsrqVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.PostIcRsrq)))).flatten.flatten.flatten
      val NumberofSubPacketsVal = bean45459_50.NumberofSubPackets
      val SubPacketIDVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.SubPacketID)
      val SubPacketSizeVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.SubPacketSize)).flatten
      val EARFCNVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
      val NumofCellVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.NumofCell))).flatten.flatten
      val ValidRxVal = bean45459_50.sr_12422_inst_list.asScala.toList.map(_.sr_12447_inst_list.asScala.toList.map(_.sr_119280_inst_list.asScala.toList.map(_.sr_119334_inst_list.asScala.toList.map(_.ValidRx)))).flatten.flatten.flatten
      logRecordB193Obj = logRecordB193Obj.copy(
        cinr0 = Try(List(cinr0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr1 = Try(List(cinr1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr2 = Try(List(cinr2Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr3 = Try(List(cinr3Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        PhysicalCellID_B193 = Try(List(PhysicalCellIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ServingCellIndex_B193 = Try(List(ServingCellIndex_B193Val(0))).getOrElse(null),
        IsServingCell = Try(List(IsServingCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSFN = Try(List(CurrentSFNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSubframeNumber = Try(List(CurrentSubframeNumberVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        IsRestricted = Try(List(IsRestrictedVal.asInstanceOf[Integer])).getOrElse(null),
        CellTiming_0 = Try(List(CellTiming_0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        CellTiming_1 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_0 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_1 = Try(List(CellTimingSFN_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        InstRSRPRx_0 = Try(List(InstRSRPRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_1 = Try(List(InstRSRPRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_2 = Try(List(InstRSRPRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_3 = Try(List(InstRSRPRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstMeasuredRSRP = Try(List(InstMeasuredRSRPVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_0 = Try(List(InstRSRQRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_1 = Try(List(InstRSRQRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_2 = Try(List(InstRSRQRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_3 = Try(List(InstRSRQRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQ = Try(List(InstRSRQVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_0 = Try(List(InstRSSIRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_1 = Try(List(InstRSSIRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_2 = Try(List(InstRSSIRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_3 = Try(List(InstRSSIRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ResidualFrequencyError = null, //Try(List(ResidualFrequencyErrorVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        FTLSNRRx_0 = Try(List(FTLSNRRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_1 = Try(List(FTLSNRRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_2 = Try(List(FTLSNRRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_3 = Try(List(FTLSNRRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ProjectedSir = Try(List(ProjectedSirVal(0).toLong.asInstanceOf[Long])).getOrElse(null),
        PostIcRsrq = Try(List(PostIcRsrqVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        NumberofSubPackets = Try(NumberofSubPacketsVal.toInt.asInstanceOf[Integer]).getOrElse(null),
        SubPacketID = Try(List(SubPacketIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        SubPacketSize = Try(List(SubPacketSizeVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        EARFCN_B193 = Try(List(EARFCNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        NumofCell = Try(List(NumofCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ValidRx = Try(List(ValidRxVal(0))).getOrElse(null),
        InstRSSI = Try(List(InstRSSIVal(0).toDouble.asInstanceOf[Double])).getOrElse(null)
      )
      logRecord = logRecord.copy(logRecordName = "B193", logCode = "45459",
        hexLogCode = "0xB193", version = b193Version, logRecordB193 = logRecordB193Obj)
    }

    def parse56(b193Version: Int) = {
      val bean45459_56 = process45459_56(x, exceptionUseArray)
      val cinr0Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.CINRRX0)))
      val cinr1Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.CINRRX1)))
      val cinr2Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.CINRRX2)))
      val cinr3Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.CINRRX3)))
      val PhysicalCellIDVal = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.PhysicalCellID)))
      val ServingCellIndex_B193Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.ServingCellIndex)))
      val IsServingCellVal = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.IsServingCell)))
      val CurrentSFNVal = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.CurrentSFN)))
      val CurrentSubframeNumberVal = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.CurrentSubframeNumber)))
      val IsRestrictedVal = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.IsRestricted)))
      val CellTiming_0Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.CellTiming0)))
      val CellTiming_1Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.CellTiming1)))
      val CellTimingSFN_0Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.CellTimingSFN0)))
      val CellTimingSFN_1Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.CellTimingSFN1)))
      val InstRSRPRx_0Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstRSRPRx0)))
      val InstRSRPRx_1Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstRSRPRx1)))
      val InstRSRPRx_2Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstRSRPRx2)))
      val InstRSRPRx_3Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstRSRPRx3)))
      val InstMeasuredRSRPVal = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstMeasuredRSRP)))
      val InstRSRQRx_0Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstRSRQRx0)))
      val InstRSRQRx_1Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstRSRQRx1)))
      val InstRSRQRx_2Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstRSRQRx2)))
      val InstRSRQRx_3Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstRSRQRx3)))
      val InstRSRQVal = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstRSRQ)))
      val InstRSSIRx_0Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstRSSIRx0)))
      val InstRSSIRx_1Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstRSSIRx1)))
      val InstRSSIRx_2Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstRSSIRx2)))
      val InstRSSIRx_3Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstRSSIRx3)))
      val InstRSSIVal = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.InstRSSI)))
      val ResidualFrequencyErrorVal = null //bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.ResidualFrequencyError)))
      val FTLSNRRx_0Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.FTLSNRRx0)))
      val FTLSNRRx_1Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.FTLSNRRx1)))
      val FTLSNRRx_2Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.FTLSNRRx2)))
      val FTLSNRRx_3Val = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.FTLSNRRx3)))
      val ProjectedSirVal = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.ProjectedSir)))
      val PostIcRsrqVal = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.PostIcRsrq)))
      val NumberofSubPacketsVal = bean45459_56.NumberofSubPackets
      val SubPacketIDVal = bean45459_56.sr_125414_inst_list.asScala.toList.map(_.SubPacketID)
      val SubPacketSizeVal = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.map(_.SubPacketSize))
      val EARFCNVal = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.map(_.EARFCN))
      val NumofCellVal = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.map(_.NumofCell))
      val ValidRxVal = bean45459_56.sr_125414_inst_list.asScala.toList.flatMap(_.sr_125416_inst_list.asScala.toList.flatMap(_.sr_125422_inst_list.asScala.toList.map(_.ValidRx)))
      logRecordB193Obj = logRecordB193Obj.copy(
        cinr0 = Try(List(cinr0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr1 = Try(List(cinr1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr2 = Try(List(cinr2Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        cinr3 = Try(List(cinr3Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        PhysicalCellID_B193 = Try(List(PhysicalCellIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ServingCellIndex_B193 = Try(List(ServingCellIndex_B193Val(0))).getOrElse(null),
        IsServingCell = Try(List(IsServingCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSFN = Try(List(CurrentSFNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CurrentSubframeNumber = Try(List(CurrentSubframeNumberVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        IsRestricted = Try(List(IsRestrictedVal.asInstanceOf[Integer])).getOrElse(null),
        CellTiming_0 = Try(List(CellTiming_0Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        CellTiming_1 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_0 = Try(List(CellTiming_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        CellTimingSFN_1 = Try(List(CellTimingSFN_1Val(0).toInt.asInstanceOf[Integer])).getOrElse(null), //Error
        InstRSRPRx_0 = Try(List(InstRSRPRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_1 = Try(List(InstRSRPRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_2 = Try(List(InstRSRPRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRPRx_3 = Try(List(InstRSRPRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstMeasuredRSRP = Try(List(InstMeasuredRSRPVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_0 = Try(List(InstRSRQRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_1 = Try(List(InstRSRQRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_2 = Try(List(InstRSRQRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQRx_3 = Try(List(InstRSRQRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSRQ = Try(List(InstRSRQVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_0 = Try(List(InstRSSIRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_1 = Try(List(InstRSSIRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_2 = Try(List(InstRSSIRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        InstRSSIRx_3 = Try(List(InstRSSIRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ResidualFrequencyError = null, //Try(List(ResidualFrequencyErrorVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        FTLSNRRx_0 = Try(List(FTLSNRRx_0Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_1 = Try(List(FTLSNRRx_1Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_2 = Try(List(FTLSNRRx_2Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        FTLSNRRx_3 = Try(List(FTLSNRRx_3Val(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        ProjectedSir = Try(List(ProjectedSirVal(0).toLong.asInstanceOf[Long])).getOrElse(null),
        PostIcRsrq = Try(List(PostIcRsrqVal(0).toDouble.asInstanceOf[Double])).getOrElse(null),
        NumberofSubPackets = Try(NumberofSubPacketsVal.toInt.asInstanceOf[Integer]).getOrElse(null),
        SubPacketID = Try(List(SubPacketIDVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        SubPacketSize = Try(List(SubPacketSizeVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        EARFCN_B193 = Try(List(EARFCNVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        NumofCell = Try(List(NumofCellVal(0).toInt.asInstanceOf[Integer])).getOrElse(null),
        ValidRx = Try(List(ValidRxVal(0))).getOrElse(null),
        InstRSSI = Try(List(InstRSSIVal(0).toDouble.asInstanceOf[Double])).getOrElse(null)
      )
      logRecord = logRecord.copy(logRecordName = "B193", logCode = "45459",
        hexLogCode = "0xB193", version = b193Version, logRecordB193 = logRecordB193Obj)
    }

    logCode match {
      case 45459 =>
        val index = 40
        val b193Version: Int = ParseUtils.GetIntFromBitArray(x, index, 8, true)
        //logger.info("0xB193 Version Check>>>>>>>>>>>>>>>"+b193Version);
        logRecord = logRecord.copy(version = b193Version)
        b193Version match {
          case 22 =>
            parse22
          case 24 =>
            parse24
          case 35 =>
            parse35
          case 18 =>
            parse18
          case 36 =>
            parse36
          case 39 =>
            parse39
          case 40 =>
            parse40
          case 41 =>
            parse41
          case 42 =>
            parse42
          case 48 =>
            parse48
          case 50 =>
            parse50(b193Version)
          case 56 =>
            parse56(b193Version)
          case _ =>
            logRecord = logRecord.copy(logRecordName = "B193", logCode = "45459", hexLogCode = "0xB193", missingVersion = b193Version)
        }
    }
    logRecord
  }
}
