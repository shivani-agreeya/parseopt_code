package com.verizon.oneparser.parselogs

import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.ProcessLogCodes
import com.verizon.oneparser.schema._
import org.apache.spark.sql.Row

import scala.collection.immutable.List

object LogRecordParserB192 extends ProcessLogCodes with LazyLogging{

  def parseLogRecordB192(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordB192Obj:LogRecordB192 = LogRecordB192()
    logCode match {
      case 45458 =>
        //logger.info("[12] Position>>>>>>>>>>>>>>>>"+logVersion)
        logVersion match {
          case 1 =>
            val bean45458 = process45458_1(x, exceptionUseArray)
            //logger.info("0xB192 json >>>>>>>>>>>>>>>" + Constants.getJsonFromBean(bean45458))
            val subPacketIdList = bean45458.sr_12422_inst_list.asScala.toList.map(_.SubPacketID)
            val subPacketVersionVal: List[Integer] = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.Version)).flatten
            //logger.info("B192 Version Check >>>>>>>>>>>>>>>>>>>>>>>>>>>"+versionVal.head)
            var subPacketId = 0
            if (subPacketIdList.size > 1) {
              subPacketId = subPacketIdList.reverse.head
            }
            else if (subPacketIdList.size == 1) {
              subPacketId = subPacketIdList.head
            }
            //logger.info("Sub Packet ID Info>>>>>>>>>>>>>>>>>>>>>"+subPacketId)
            if (subPacketId == 27) {
              //logger.info("27 Version Matched>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
              subPacketVersionVal.head.toInt match {
                case 1 =>
                  val NumberofSubPacketsVal = bean45458.NumberofSubPackets
                  val SubPacketIDVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.SubPacketID)
                  val NumofCellVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13536_inst_list.asScala.toList.map(_.NumCells))).flatten.flatten
                  val DuplexingModeVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13536_inst_list.asScala.toList.map(_.DuplexingMode))).flatten.flatten
                  val EARFCNVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13536_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
                  val PhysicalCellIDVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13536_inst_list.asScala.toList.map(_.sr_13668_inst_list.asScala.toList.map(_.PhysicalCellID)))).flatten.flatten.flatten
                  val FTLCumulativeFreqOffsetVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13536_inst_list.asScala.toList.map(_.sr_13668_inst_list.asScala.toList.map(_.FTLCumulativeFreqOffset)))).flatten.flatten.flatten
                  val InstRSRPRx_0Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13536_inst_list.asScala.toList.map(_.sr_13668_inst_list.asScala.toList.map(_.InstRSRPRx_0.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRPRx_1Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13536_inst_list.asScala.toList.map(_.sr_13668_inst_list.asScala.toList.map(_.InstRSRPRx_1.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstMeasuredRSRPVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13536_inst_list.asScala.toList.map(_.sr_13668_inst_list.asScala.toList.map(_.InstMeasuredRSRP.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRQRx_0Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13536_inst_list.asScala.toList.map(_.sr_13668_inst_list.asScala.toList.map(_.InstRSRQRx_0.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRQRx_1Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13536_inst_list.asScala.toList.map(_.sr_13668_inst_list.asScala.toList.map(_.InstRSRQRx_1.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRQVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13536_inst_list.asScala.toList.map(_.sr_13668_inst_list.asScala.toList.map(_.InstRSRQ.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSSIRx_0Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13536_inst_list.asScala.toList.map(_.sr_13668_inst_list.asScala.toList.map(_.InstRSSIRx_0.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSSIRx_1Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13536_inst_list.asScala.toList.map(_.sr_13668_inst_list.asScala.toList.map(_.InstRSSIRx_1.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSSIVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13536_inst_list.asScala.toList.map(_.sr_13668_inst_list.asScala.toList.map(_.InstRSSI.asInstanceOf[Double])))).flatten.flatten.flatten
                  logRecordB192Obj = logRecordB192Obj.copy(EARFCN_B192 = EARFCNVal, DuplexingMode_B192 =DuplexingModeVal,
                    PhysicalCellID_B192 = PhysicalCellIDVal, FTLCumulativeFreqOffset_B192 = FTLCumulativeFreqOffsetVal, NumberofSubPackets_B192 = NumberofSubPacketsVal,
                    InstRSRPRx_0_B192 = InstRSRPRx_0Val, InstRSRPRx_1_B192 = InstRSRPRx_1Val,
                    InstMeasuredRSRP_B192 = InstMeasuredRSRPVal, InstRSRQRx_0_B192 = InstRSRQRx_0Val, InstRSRQRx_1_B192 = InstRSRQRx_1Val,
                    InstRSRQ_B192 = InstRSRQVal, InstRSSIRx_0_B192 = InstRSSIRx_0Val, InstRSSIRx_1_B192 = InstRSSIRx_1Val, InstRSSI_B192 = InstRSSIVal, NumofCell_B192 = NumofCellVal, SubPacketID_B192 = SubPacketIDVal, subPacketVersion_B192 = subPacketVersionVal.head)
                  logRecord = logRecord.copy(logRecordName = "B192", logCode = "45458",
                  hexLogCode = "0xB192", version = logVersion, logRecordB192 = logRecordB192Obj)
                case 2 =>
                  val NumberofSubPacketsVal = bean45458.NumberofSubPackets
                  val SubPacketIDVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.SubPacketID)
                  val NumofCellVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13537_inst_list.asScala.toList.map(_.NumCells))).flatten.flatten
                  val DuplexingModeVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13537_inst_list.asScala.toList.map(_.DuplexingMode))).flatten.flatten
                  val EARFCNVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13537_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
                  val PhysicalCellIDVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13537_inst_list.asScala.toList.map(_.sr_13614_inst_list.asScala.toList.map(_.PhysicalCellID)))).flatten.flatten.flatten
                  val FTLCumulativeFreqOffsetVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13537_inst_list.asScala.toList.map(_.sr_13614_inst_list.asScala.toList.map(_.FTLCumulativeFreqOffset)))).flatten.flatten.flatten
                  val InstRSRPRx_0Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13537_inst_list.asScala.toList.map(_.sr_13614_inst_list.asScala.toList.map(_.InstRSRPRx_0.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRPRx_1Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13537_inst_list.asScala.toList.map(_.sr_13614_inst_list.asScala.toList.map(_.InstRSRPRx_1.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstMeasuredRSRPVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13537_inst_list.asScala.toList.map(_.sr_13614_inst_list.asScala.toList.map(_.InstMeasuredRSRP.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRQRx_0Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13537_inst_list.asScala.toList.map(_.sr_13614_inst_list.asScala.toList.map(_.InstRSRQRx_0.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRQRx_1Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13537_inst_list.asScala.toList.map(_.sr_13614_inst_list.asScala.toList.map(_.InstRSRQRx_1.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRQVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13537_inst_list.asScala.toList.map(_.sr_13614_inst_list.asScala.toList.map(_.InstRSRQ.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSSIRx_0Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13537_inst_list.asScala.toList.map(_.sr_13614_inst_list.asScala.toList.map(_.InstRSSIRx_0.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSSIRx_1Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13537_inst_list.asScala.toList.map(_.sr_13614_inst_list.asScala.toList.map(_.InstRSSIRx_1.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSSIVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13537_inst_list.asScala.toList.map(_.sr_13614_inst_list.asScala.toList.map(_.InstRSSI.asInstanceOf[Double])))).flatten.flatten.flatten
                  logRecordB192Obj = logRecordB192Obj.copy(EARFCN_B192 = EARFCNVal, DuplexingMode_B192 =DuplexingModeVal,
                    PhysicalCellID_B192 = PhysicalCellIDVal, FTLCumulativeFreqOffset_B192 = FTLCumulativeFreqOffsetVal, NumberofSubPackets_B192 = NumberofSubPacketsVal,
                    InstRSRPRx_0_B192 = InstRSRPRx_0Val, InstRSRPRx_1_B192 = InstRSRPRx_1Val,
                    InstMeasuredRSRP_B192 = InstMeasuredRSRPVal, InstRSRQRx_0_B192 = InstRSRQRx_0Val, InstRSRQRx_1_B192 = InstRSRQRx_1Val,
                    InstRSRQ_B192 = InstRSRQVal, InstRSSIRx_0_B192 = InstRSSIRx_0Val, InstRSSIRx_1_B192 = InstRSSIRx_1Val, InstRSSI_B192 = InstRSSIVal, NumofCell_B192 = NumofCellVal, SubPacketID_B192 = SubPacketIDVal, subPacketVersion_B192 = subPacketVersionVal.head)
                  logRecord = logRecord.copy(logRecordName = "B192", logCode = "45458",
                    hexLogCode = "0xB192", version = logVersion, logRecordB192 = logRecordB192Obj)
                case 3 =>
                  val NumberofSubPacketsVal = bean45458.NumberofSubPackets
                  val SubPacketIDVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.SubPacketID)
                  val NumofCellVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13538_inst_list.asScala.toList.map(_.NumCells))).flatten.flatten
                  val DuplexingModeVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13538_inst_list.asScala.toList.map(_.DuplexingMode))).flatten.flatten
                  val EARFCNVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13538_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
                  val PhysicalCellIDVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13538_inst_list.asScala.toList.map(_.sr_13608_inst_list.asScala.toList.map(_.PhysicalCellID)))).flatten.flatten.flatten
                  val FTLCumulativeFreqOffsetVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13538_inst_list.asScala.toList.map(_.sr_13608_inst_list.asScala.toList.map(_.FTLCumulativeFreqOffset)))).flatten.flatten.flatten
                  //val InstRSRPRx_0Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13538_inst_list.asScala.toList.map(_.sr_13608_inst_list.asScala.toList.map(_.InstRSRPRx_0)))).flatten.flatten.flatten
                  val InstRSRPRx_1Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13538_inst_list.asScala.toList.map(_.sr_13608_inst_list.asScala.toList.map(_.InstRSRPRx_1.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstMeasuredRSRPVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13538_inst_list.asScala.toList.map(_.sr_13608_inst_list.asScala.toList.map(_.InstMeasuredRSRP.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRQRx_0Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13538_inst_list.asScala.toList.map(_.sr_13608_inst_list.asScala.toList.map(_.InstRSRQRx_0.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRQRx_1Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13538_inst_list.asScala.toList.map(_.sr_13608_inst_list.asScala.toList.map(_.InstRSRQRx_1.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRQVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13538_inst_list.asScala.toList.map(_.sr_13608_inst_list.asScala.toList.map(_.InstRSRQ.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSSIRx_0Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13538_inst_list.asScala.toList.map(_.sr_13608_inst_list.asScala.toList.map(_.InstRSSIRx_0.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSSIRx_1Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13538_inst_list.asScala.toList.map(_.sr_13608_inst_list.asScala.toList.map(_.InstRSSIRx_1.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSSIVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13538_inst_list.asScala.toList.map(_.sr_13608_inst_list.asScala.toList.map(_.InstRSSI.asInstanceOf[Double])))).flatten.flatten.flatten
                  logRecordB192Obj = logRecordB192Obj.copy(EARFCN_B192 = EARFCNVal, DuplexingMode_B192 =DuplexingModeVal,
                    PhysicalCellID_B192 = PhysicalCellIDVal, FTLCumulativeFreqOffset_B192 = FTLCumulativeFreqOffsetVal, NumberofSubPackets_B192 = NumberofSubPacketsVal,
                    InstRSRPRx_1_B192 = InstRSRPRx_1Val,
                    InstMeasuredRSRP_B192 = InstMeasuredRSRPVal, InstRSRQRx_0_B192 = InstRSRQRx_0Val, InstRSRQRx_1_B192 = InstRSRQRx_1Val,
                    InstRSRQ_B192 = InstRSRQVal, InstRSSIRx_0_B192 = InstRSSIRx_0Val, InstRSSIRx_1_B192 = InstRSSIRx_1Val, InstRSSI_B192 = InstRSSIVal, NumofCell_B192 = NumofCellVal, SubPacketID_B192 = SubPacketIDVal, subPacketVersion_B192 = subPacketVersionVal.head)
                  logRecord = logRecord.copy(logRecordName = "B192", logCode = "45458",
                    hexLogCode = "0xB192", version = logVersion, logRecordB192 = logRecordB192Obj)
                case 4 =>
                  val NumberofSubPacketsVal = bean45458.NumberofSubPackets
                  val SubPacketIDVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.SubPacketID)
                  val NumofCellVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13539_inst_list.asScala.toList.map(_.NumCells))).flatten.flatten
                  val DuplexingModeVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13539_inst_list.asScala.toList.map(_.DuplexingMode))).flatten.flatten
                  val EARFCNVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13539_inst_list.asScala.toList.map(_.EARFCN))).flatten.flatten
                  val PhysicalCellIDVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13539_inst_list.asScala.toList.map(_.sr_13546_inst_list.asScala.toList.map(_.PhysicalCellID)))).flatten.flatten.flatten
                  val FTLCumulativeFreqOffsetVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13539_inst_list.asScala.toList.map(_.sr_13546_inst_list.asScala.toList.map(_.FTLCumulativeFreqOffset)))).flatten.flatten.flatten
                  val InstRSRPRx_0Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13539_inst_list.asScala.toList.map(_.sr_13546_inst_list.asScala.toList.map(_.InstRSRPRx_0.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRPRx_1Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13539_inst_list.asScala.toList.map(_.sr_13546_inst_list.asScala.toList.map(_.InstRSRPRx_1.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstMeasuredRSRPVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13539_inst_list.asScala.toList.map(_.sr_13546_inst_list.asScala.toList.map(_.InstMeasuredRSRP.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRQRx_0Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13539_inst_list.asScala.toList.map(_.sr_13546_inst_list.asScala.toList.map(_.InstRSRQRx_0.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRQRx_1Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13539_inst_list.asScala.toList.map(_.sr_13546_inst_list.asScala.toList.map(_.InstRSRQRx_1.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSRQVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13539_inst_list.asScala.toList.map(_.sr_13546_inst_list.asScala.toList.map(_.InstRSRQ.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSSIRx_0Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13539_inst_list.asScala.toList.map(_.sr_13546_inst_list.asScala.toList.map(_.InstRSSIRx_0.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSSIRx_1Val = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13539_inst_list.asScala.toList.map(_.sr_13546_inst_list.asScala.toList.map(_.InstRSSIRx_1.asInstanceOf[Double])))).flatten.flatten.flatten
                  val InstRSSIVal = bean45458.sr_12422_inst_list.asScala.toList.map(_.sr_12449_inst_list.asScala.toList.map(_.sr_13539_inst_list.asScala.toList.map(_.sr_13546_inst_list.asScala.toList.map(_.InstRSSI.asInstanceOf[Double])))).flatten.flatten.flatten

                  logRecordB192Obj = logRecordB192Obj.copy(EARFCN_B192 = EARFCNVal, DuplexingMode_B192 =DuplexingModeVal,
                    PhysicalCellID_B192 = PhysicalCellIDVal, FTLCumulativeFreqOffset_B192 = FTLCumulativeFreqOffsetVal, NumberofSubPackets_B192 = NumberofSubPacketsVal,
                    InstRSRPRx_0_B192 = InstRSRPRx_0Val, InstRSRPRx_1_B192 = InstRSRPRx_1Val,
                    InstMeasuredRSRP_B192 = InstMeasuredRSRPVal, InstRSRQRx_0_B192 = InstRSRQRx_0Val, InstRSRQRx_1_B192 = InstRSRQRx_1Val,
                    InstRSRQ_B192 = InstRSRQVal, InstRSSIRx_0_B192 = InstRSSIRx_0Val, InstRSSIRx_1_B192 = InstRSSIRx_1Val, InstRSSI_B192 = InstRSSIVal, NumofCell_B192 = NumofCellVal, SubPacketID_B192 = SubPacketIDVal, subPacketVersion_B192 = subPacketVersionVal.head)
                  logRecord = logRecord.copy(logRecordName = "B192", logCode = "45458",
                    hexLogCode = "0xB192", version = logVersion, logRecordB192 = logRecordB192Obj)
                case _ =>
                  logRecord = logRecord.copy(logRecordName = "B192", logCode = "45458", hexLogCode = "0xB192", missingVersion = subPacketVersionVal.head.toInt)
              }
            }
        }
    }
    logRecord
  }

}
