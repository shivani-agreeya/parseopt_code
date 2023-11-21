package com.verizon.oneparser.parselogs

import com.verizon.oneparser.ProcessLogCodes
import com.verizon.oneparser.common.Constants
import com.verizon.oneparser.parselogs.LogRecordParser5G.process47271_196612
import com.verizon.oneparser.schema.{cdrxKpi0xB890List, _}
import com.vzwdt.DNLP5G_ph3.mac.{Dynamic_47271_131075_SR_48775_DTO, Dynamic_47271_196612_SR_58684_DTO}
import com.vzwdt.DNLP5G_ph3.{Dynamic_47235_131079_SR_20969_DTO, Dynamic_47235_131080_SR_21640_DTO, Dynamic_47235_131083_SR_27932_DTO, Dynamic_47239_131076_SR_19721_DTO, Dynamic_47239_131076_SR_19776_DTO, Dynamic_47239_131077_SR_27681_DTO, Dynamic_47239_131077_SR_27701_DTO, Dynamic_47239_65545_SR_16612_DTO, Dynamic_47239_65545_SR_16646_DTO, Dynamic_47239_65547_SR_16370_DTO, Dynamic_47239_65547_SR_16425_DTO, Dynamic_47249_65537, Dynamic_47271_131073_SR_28370_DTO, Dynamic_47487_131075_ENUMERATION_27591_DTO, Dynamic_47487_131075_SR_27567_DTO, Dynamic_47487_131075_SR_27574_DTO, Dynamic_47487_131078_ENUMERATION_27527_DTO, Dynamic_47487_131078_SR_27414_DTO, Dynamic_47487_131078_SR_27424_DTO, Dynamic_47487_131079_ENUMERATION_27791_DTO, Dynamic_47487_131079_SR_27767_DTO, Dynamic_47487_131079_SR_27774_DTO}
import com.vzwdt.DNLP5G_ph3.ml1.{Dynamic_47487_131080_ENUMERATION_48620_DTO, Dynamic_47487_131080_SR_48626_DTO, Dynamic_47487_131081_ENUMERATION_57536_DTO, Dynamic_47487_131081_SR_57542_DTO}
import com.vzwdt.parseutils.ParseUtils

import java.lang
import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.language.postfixOps

object LogRecordParser5G extends ProcessLogCodes {

  def parseLogRecordQCOMM5GSet1(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQComm5GObj: LogRecordQComm5G = LogRecordQComm5G()
    var logRecordQComm5G2Obj: LogRecordQComm5G2 = LogRecordQComm5G2()

    //    logger.info("Parsing logCode=" + logCode + " with logVersion=" + logVersion)

    def parse47138 = {
      logVersion match {
        case 1 =>
          val bean47138_1 = process47138_1(x, exceptionUseArray)
          //logger.info("bean47138_2 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47138_2))

          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSDLFREQUENCY = bean47138_1.DL_Frequency, NRSPHYCELLID = bean47138_1.Physical_Cell_ID)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47138", hexLogCode = "0xB822", logRecordQComm5G = logRecordQComm5GObj)
        case 2 =>
          val bean47138_2 = process47138_2(x, exceptionUseArray)
          //logger.info("bean47138_2 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47138_2))
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSDLFREQUENCY = bean47138_2.DL_Frequency, NRSPHYCELLID = bean47138_2.Physical_Cell_ID)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47138", hexLogCode = "0xB822", logRecordQComm5G = logRecordQComm5GObj)
        case 3 =>
          val bean47138_3 = process47138_3(x, exceptionUseArray)
          //logger.info("bean47138_2 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47138_2))
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSDLFREQUENCY = bean47138_3.DL_Frequency, NRSPHYCELLID = bean47138_3.Physical_Cell_ID)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47138", hexLogCode = "0xB822", logRecordQComm5G = logRecordQComm5GObj)
        case 131072 =>
          val bean47138_131072 = process47138_131072(x, exceptionUseArray)
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSDLFREQUENCY = bean47138_131072.DLFrequency, NRSPHYCELLID = bean47138_131072.PhysicalCellID)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47138", hexLogCode = "0xB822", logRecordQComm5G = logRecordQComm5GObj)
        case _ =>
          //logger.info("Missing Version for logcode:47138, hexlogCode:0xB822, Version:" + logVersion)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47138", hexLogCode = "0xB822", missingVersion = logVersion)
      }
    }

    def parse47170 = {
      logVersion match {
        case 1 =>
          val bean47170_1 = process47170_1(x, exceptionUseArray)
          val dataPDUBytesReceivedAllRBSum0xB842: lang.Long = bean47170_1.sr_38_inst_list.asScala.toList.map(_.Data_PDU_Bytes_Received).filter(str => str != null && str.trim.size > 0).map(str => str.toLong).sum
          val controlPDUBytesReceivedAllRBSum0xB842: lang.Long = bean47170_1.sr_38_inst_list.asScala.toList.map(_.Control_PDU_Bytes_Received).filter(str => str != null && str.trim.size > 0).map(str => str.toLong).sum
          val numMissPDUToUpperLayerSum_0xB842Val: lang.Long = bean47170_1.sr_38_inst_list.asScala.toList.map(_.getNum_Miss_PDU_To_Upper_Layer).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numDataPDUReceivedSum_0xB842Val: lang.Long = bean47170_1.sr_38_inst_list.asScala.toList.map(_.getNum_Data_PDU_Received).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          //dataPDUBytesReceivedAllRBSum_0xB842,controlPDUBytesReceivedAllRBSum_0xB842
          logRecordQComm5GObj = logRecordQComm5GObj.copy(dataPDUBytesReceivedAllRBSum_0xB842 = dataPDUBytesReceivedAllRBSum0xB842, controlPDUBytesReceivedAllRBSum_0xB842 = controlPDUBytesReceivedAllRBSum0xB842,
            numMissPDUToUpperLayerSum_0xB842 = numMissPDUToUpperLayerSum_0xB842Val, numDataPDUReceivedSum_0xB842 = numDataPDUReceivedSum_0xB842Val)
          //TODO: RB Config index field is not available in dtos
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47170", hexLogCode = "0xB842", logRecordQComm5G = logRecordQComm5GObj)
        case 2 =>
          val bean47170_2 = process47170_2(x, exceptionUseArray)
          val controlPDUBytesReceivedAllRBSum0xB842: lang.Long = bean47170_2.sr_63_inst_list.asScala.toList.map(_.Control_PDU_Bytes_Received).
            filter(str => str != null && str.trim.size > 0).map(str => str.toLong).sum
          val numMissPDUToUpperLayerSum_0xB842Val: lang.Long = bean47170_2.sr_63_inst_list.asScala.toList.map(_.getNum_Miss_PDU_To_Upper_Layer).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numDataPDUReceivedSum_0xB842Val: lang.Long = bean47170_2.sr_63_inst_list.asScala.toList.map(_.getNum_Data_PDU_Received).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          logRecordQComm5GObj = logRecordQComm5GObj.copy(controlPDUBytesReceivedAllRBSum_0xB842 = controlPDUBytesReceivedAllRBSum0xB842)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47170",
            hexLogCode = "0xB842", logRecordQComm5G = logRecordQComm5GObj)
        case 3 =>
          val bean47170_3 = process47170_3(x, exceptionUseArray)
          //logger.info("bean47170_3 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47170_3))
          val dataPDUBytesReceivedAllRBSum0xB842 = bean47170_3.sr_18642_inst_list.asScala.toList.map(_.DataPDUBytesReceived).filter(str => str != null && str.trim.size > 0).map(str => str.toLong).sum
          val controlPDUBytesReceivedAllRBSum0xB842 = bean47170_3.sr_18642_inst_list.asScala.toList.map(_.ControlPDUBytesReceived).filter(str => str != null && str.trim.size > 0).map(str => str.toLong).sum
          val numMissPDUToUpperLayerSum_0xB842Val: lang.Long = bean47170_3.sr_18642_inst_list.asScala.toList.map(_.getNumMissPDUToUpperLayer).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numDataPDUReceivedSum_0xB842Val: lang.Long = bean47170_3.sr_18642_inst_list.asScala.toList.map(_.getNumDataPDUReceived).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          logRecordQComm5GObj = logRecordQComm5GObj.copy(dataPDUBytesReceivedAllRBSum_0xB842 = dataPDUBytesReceivedAllRBSum0xB842, controlPDUBytesReceivedAllRBSum_0xB842 = controlPDUBytesReceivedAllRBSum0xB842,
            numMissPDUToUpperLayerSum_0xB842 = numMissPDUToUpperLayerSum_0xB842Val, numDataPDUReceivedSum_0xB842 = numDataPDUReceivedSum_0xB842Val)
          //logger.info(s"numMissPDUToUpperLayerSum_0xB842 = $numMissPDUToUpperLayerSum_0xB842Val, numDataPDUReceivedSum_0xB842 = $numDataPDUReceivedSum_0xB842Val, time = ${logRecord.dmTimeStamp}")
          //TODO: RB Config index field is not available in dtos
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47170", hexLogCode = "0xB842", logRecordQComm5G = logRecordQComm5GObj)
        case 4 =>
          val bean47170_4 = process47170_4(x, exceptionUseArray)
          val dataPDUBytesReceivedAllRBSum0xB842 = bean47170_4.sr_57030_inst_list.asScala.toList.map(_.DataPDUBytesReceived).filter(str => str != null && str.trim.size > 0).map(str => str.toLong).sum
          val controlPDUBytesReceivedAllRBSum0xB842 = bean47170_4.sr_57030_inst_list.asScala.toList.map(_.ControlPDUBytesReceived).filter(str => str != null && str.trim.size > 0).map(str => str.toLong).sum
          val numMissPDUToUpperLayerSum_0xB842Val: lang.Long = bean47170_4.sr_57030_inst_list.asScala.toList.map(_.getNumMissPDUToUpperLayer).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numDataPDUReceivedSum_0xB842Val: lang.Long = bean47170_4.sr_57030_inst_list.asScala.toList.map(_.getNumDataPDUReceived).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          logRecordQComm5GObj = logRecordQComm5GObj.copy(dataPDUBytesReceivedAllRBSum_0xB842 = dataPDUBytesReceivedAllRBSum0xB842, controlPDUBytesReceivedAllRBSum_0xB842 = controlPDUBytesReceivedAllRBSum0xB842,
            numMissPDUToUpperLayerSum_0xB842 = numMissPDUToUpperLayerSum_0xB842Val, numDataPDUReceivedSum_0xB842 = numDataPDUReceivedSum_0xB842Val)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47170", hexLogCode = "0xB842", logRecordQComm5G = logRecordQComm5GObj)
        case 5 =>
          val bean47170_5 = process47170_5(x, exceptionUseArray)
          //logger.info("bean47170_5 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47170_5))
          val dataPDUBytesReceivedAllRBSum0xB842 = bean47170_5.sr_27306_inst_list.asScala.toList.map(_.DataPDUBytesReceived).filter(str => str != null && str.trim.size > 0).map(str => str.toLong).sum
          val controlPDUBytesReceivedAllRBSum0xB842 = bean47170_5.sr_27306_inst_list.asScala.toList.map(_.ControlPDUBytesReceived).filter(str => str != null && str.trim.size > 0).map(str => str.toLong).sum
          val numMissPDUToUpperLayerSum_0xB842Val: lang.Long = bean47170_5.sr_27306_inst_list.asScala.toList.map(_.getNumMissPDUToUpperLayer).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numDataPDUReceivedSum_0xB842Val: lang.Long = bean47170_5.sr_27306_inst_list.asScala.toList.map(_.getNumDataPDUReceived).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          logRecordQComm5GObj = logRecordQComm5GObj.copy(dataPDUBytesReceivedAllRBSum_0xB842 = dataPDUBytesReceivedAllRBSum0xB842, controlPDUBytesReceivedAllRBSum_0xB842 = controlPDUBytesReceivedAllRBSum0xB842,
            numMissPDUToUpperLayerSum_0xB842 = numMissPDUToUpperLayerSum_0xB842Val, numDataPDUReceivedSum_0xB842 = numDataPDUReceivedSum_0xB842Val)
          //logger.info(s"numMissPDUToUpperLayerSum_0xB842 = $numMissPDUToUpperLayerSum_0xB842Val, numDataPDUReceivedSum_0xB842 = $numDataPDUReceivedSum_0xB842Val, time = ${logRecord.dmTimeStamp}")
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47170", hexLogCode = "0xB842", logRecordQComm5G = logRecordQComm5GObj)
        case 6 =>
          val bean47170_6 = process47170_6(x, exceptionUseArray)
          //logger.info("bean47170_6 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47170_6))
          val dataPDUBytesReceivedAllRBSum0xB842 = bean47170_6.sr_42271_inst_list.asScala.toList.map(_.DataPDUBytesReceived).filter(str => str != null && str.trim.size > 0).map(str => str.toLong).sum
          val controlPDUBytesReceivedAllRBSum0xB842 = bean47170_6.sr_42271_inst_list.asScala.toList.map(_.ControlPDUBytesReceived).filter(str => str != null && str.trim.size > 0).map(str => str.toLong).sum
          val numMissPDUToUpperLayerSum_0xB842Val: lang.Long = bean47170_6.sr_42271_inst_list.asScala.toList.map(_.getNumMissPDUToUpperLayer).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numDataPDUReceivedSum_0xB842Val: lang.Long = bean47170_6.sr_42271_inst_list.asScala.toList.map(_.getNumDataPDUReceived).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          logRecordQComm5GObj = logRecordQComm5GObj.copy(dataPDUBytesReceivedAllRBSum_0xB842 = dataPDUBytesReceivedAllRBSum0xB842, controlPDUBytesReceivedAllRBSum_0xB842 = controlPDUBytesReceivedAllRBSum0xB842,
            numMissPDUToUpperLayerSum_0xB842 = numMissPDUToUpperLayerSum_0xB842Val, numDataPDUReceivedSum_0xB842 = numDataPDUReceivedSum_0xB842Val)
          //logger.info(s"numMissPDUToUpperLayerSum_0xB842 = $numMissPDUToUpperLayerSum_0xB842Val, numDataPDUReceivedSum_0xB842 = $numDataPDUReceivedSum_0xB842Val, time = ${logRecord.dmTimeStamp}")
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47170", hexLogCode = "0xB842", logRecordQComm5G = logRecordQComm5GObj)
        case _ =>
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47170", hexLogCode = "0xB842", missingVersion = logVersion)

      }
    }

    def parse47181 = {
      logVersion match {
        case 1 =>
          val bean47181_1 = process47181_1(x, exceptionUseArray)
          //logger.info("bean47181_1 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47181_1))
          val NumInvalidPDUsVal = bean47181_1.sr_337_inst_list.asScala.toList.map(_.Num_Invalid_PDUs)
          val NumDuplicatePDUVal = bean47181_1.sr_337_inst_list.asScala.toList.map(_.Num_Duplicate_PDU)
          val NumDroppedPDUVal = bean47181_1.sr_337_inst_list.asScala.toList.map(_.Num_Dropped_PDU)
          val numReTxPDUSum_0xB84DVal: lang.Long = bean47181_1.sr_337_inst_list.asScala.toList.map(_.Num_ReTx_PDU).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numMissedUMPDUSum_0xB84DVal: lang.Long = bean47181_1.sr_337_inst_list.asScala.toList.map(_.Num_Missed_UM_PDU).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numDroppedPDUSum_0xB84DVal: lang.Long = bean47181_1.sr_337_inst_list.asScala.toList.map(_.Num_Dropped_PDU).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numDataPDUSum_0xB84DVal: lang.Long = bean47181_1.sr_337_inst_list.asScala.toList.map(_.Num_Data_PDU).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          //logger.info(s"numReTxPDUSum_0xB84D = $numReTxPDUSum_0xB84DVal, numMissedUMPDUSum_0xB84D = $numMissedUMPDUSum_0xB84DVal, numDroppedPDUSum_0xB84D = $numDroppedPDUSum_0xB84DVal, numDataPDUSum_0xB84D = $numDataPDUSum_0xB84DVal, time = ${logRecord.dmTimeStamp}")
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NumRB_0xB84D = bean47181_1.Num_RB, NumInvalidPDUs = NumInvalidPDUsVal, NumDuplicatePDU = NumDuplicatePDUVal, NumDroppedPDU = NumDroppedPDUVal,
            numReTxPDUSum_0xB84D = numReTxPDUSum_0xB84DVal, numMissedUMPDUSum_0xB84D = numMissedUMPDUSum_0xB84DVal, numDroppedPDUSum_0xB84D = numDroppedPDUSum_0xB84DVal, numDataPDUSum_0xB84D = numDataPDUSum_0xB84DVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47181", hexLogCode = "0xB84D", logRecordQComm5G = logRecordQComm5GObj)

        case 2 =>
          val bean47181_2 = process47181_2(x, exceptionUseArray)
          //logger.info("bean47181_2 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47181_2))
          val NumInvalidPDUsVal = bean47181_2.sr_381_inst_list.asScala.toList.map(_.Num_Invalid_PDUs)
          val NumDuplicatePDUVal = bean47181_2.sr_381_inst_list.asScala.toList.map(_.Num_Duplicate_PDU)
          val NumDroppedPDUVal = bean47181_2.sr_381_inst_list.asScala.toList.map(_.Num_Dropped_PDU)
          val numReTxPDUSum_0xB84DVal: lang.Long = bean47181_2.sr_381_inst_list.asScala.toList.map(_.Num_ReTx_PDU).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numMissedUMPDUSum_0xB84DVal: lang.Long = bean47181_2.sr_381_inst_list.asScala.toList.map(_.Num_Missed_UM_PDU).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numDroppedPDUSum_0xB84DVal: lang.Long = bean47181_2.sr_381_inst_list.asScala.toList.map(_.Num_Dropped_PDU).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numDataPDUSum_0xB84DVal: lang.Long = bean47181_2.sr_381_inst_list.asScala.toList.map(_.Num_Data_PDU).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          //logger.info(s"numReTxPDUSum_0xB84D = $numReTxPDUSum_0xB84DVal, numMissedUMPDUSum_0xB84D = $numMissedUMPDUSum_0xB84DVal, numDroppedPDUSum_0xB84D = $numDroppedPDUSum_0xB84DVal, numDataPDUSum_0xB84D = $numDataPDUSum_0xB84DVal, time = ${logRecord.dmTimeStamp}")
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NumRB_0xB84D = bean47181_2.Num_RB, NumInvalidPDUs = NumInvalidPDUsVal, NumDuplicatePDU = NumDuplicatePDUVal, NumDroppedPDU = NumDroppedPDUVal,
            numReTxPDUSum_0xB84D = numReTxPDUSum_0xB84DVal, numMissedUMPDUSum_0xB84D = numMissedUMPDUSum_0xB84DVal, numDroppedPDUSum_0xB84D = numDroppedPDUSum_0xB84DVal, numDataPDUSum_0xB84D = numDataPDUSum_0xB84DVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47181", hexLogCode = "0xB84D", logRecordQComm5G = logRecordQComm5GObj)

        case 3 =>
          val bean47181_3 = process47181_3(x, exceptionUseArray)
          //logger.info("bean47181_3 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47181_3))
          val NumInvalidPDUsVal = bean47181_3.sr_12774_inst_list.asScala.toList.map(_.Num_Invalid_PDUs)
          val NumDuplicatePDUVal = bean47181_3.sr_12774_inst_list.asScala.toList.map(_.Num_Duplicate_PDU)
          val NumDroppedPDUVal = bean47181_3.sr_12774_inst_list.asScala.toList.map(_.Num_Dropped_PDU)
          val RLCDataBytesVal = bean47181_3.sr_12774_inst_list.asScala.toList.map(_.RLC_Data_Bytes.trim().toLong).filter(_ > 0)
          val RLCDataBytesVal0xB84D: lang.Long = if (RLCDataBytesVal != null && RLCDataBytesVal.size > 0) RLCDataBytesVal.last else null

          val RBConfigIndexList: List[String] = bean47181_3.sr_12774_inst_list.asScala.toList.map(_.RBConfigIndex).distinct
          var numReTxPDUSum_0xB84DVal: lang.Long = 0
          var numMissedUMPDUSum_0xB84DVal: lang.Long = 0
          var numDroppedPDUSum_0xB84DVal: lang.Long = 0
          var numDataPDUSum_0xB84DVal: lang.Long = 0
          for (index <- RBConfigIndexList) {
            val lastRBStats = bean47181_3.sr_12774_inst_list.asScala.toList.filter(_.RBConfigIndex == index).last
            val num_ReTx_PDU = lastRBStats.Num_ReTx_PDU
            val num_Missed_UM_PDU = lastRBStats.Num_Missed_UM_PDU
            val num_Dropped_PDU = lastRBStats.Num_Dropped_PDU
            val num_Data_PDU = lastRBStats.Num_Data_PDU

            numReTxPDUSum_0xB84DVal = numReTxPDUSum_0xB84DVal + (if (num_ReTx_PDU != null && num_ReTx_PDU.trim.nonEmpty) parseLong(num_ReTx_PDU) else 0)
            numMissedUMPDUSum_0xB84DVal = numMissedUMPDUSum_0xB84DVal + (if (num_Missed_UM_PDU != null && num_Missed_UM_PDU.trim.nonEmpty) parseLong(num_Missed_UM_PDU) else 0)
            numDroppedPDUSum_0xB84DVal = numDroppedPDUSum_0xB84DVal + (if (num_Dropped_PDU != null && num_Dropped_PDU.trim.nonEmpty) parseLong(num_Dropped_PDU) else 0)
            numDataPDUSum_0xB84DVal = numDataPDUSum_0xB84DVal + (if (num_Data_PDU != null && num_Data_PDU.trim.nonEmpty) parseLong(num_Data_PDU) else 0)
          }

          //logger.info(s"numReTxPDUSum_0xB84D = $numReTxPDUSum_0xB84DVal, numMissedUMPDUSum_0xB84D = $numMissedUMPDUSum_0xB84DVal, numDroppedPDUSum_0xB84D = $numDroppedPDUSum_0xB84DVal, numDataPDUSum_0xB84D = $numDataPDUSum_0xB84DVal, time = ${logRecord.dmTimeStamp}")
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NumRB_0xB84D = bean47181_3.Num_RB, NumInvalidPDUs = NumInvalidPDUsVal,
            NumDuplicatePDU = NumDuplicatePDUVal, NumDroppedPDU = NumDroppedPDUVal, rlcDataBytesVal_0xB84D = RLCDataBytesVal0xB84D,
            numReTxPDUSum_0xB84D = numReTxPDUSum_0xB84DVal, numMissedUMPDUSum_0xB84D = numMissedUMPDUSum_0xB84DVal, numDroppedPDUSum_0xB84D = numDroppedPDUSum_0xB84DVal, numDataPDUSum_0xB84D = numDataPDUSum_0xB84DVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47181", hexLogCode = "0xB84D", logRecordQComm5G = logRecordQComm5GObj)

        case 4 =>
          val bean47181_4 = process47181_4(x, exceptionUseArray)
          //logger.info("bean47181_4 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47181_4))
          val NumInvalidPDUsVal = bean47181_4.sr_18605_inst_list.asScala.toList.map(_.NumInvalidPDUs)
          val NumDuplicatePDUVal = bean47181_4.sr_18605_inst_list.asScala.toList.map(_.NumDuplicatePDU)
          val NumDroppedPDUVal = bean47181_4.sr_18605_inst_list.asScala.toList.map(_.NumDroppedPDU)
          val RLCDataBytesVal = bean47181_4.sr_18605_inst_list.asScala.toList.map(_.RLCDataBytes.trim().toLong).filter(_ > 0)
          val RLCDataBytesVal0xB84D: lang.Long = if (RLCDataBytesVal != null && RLCDataBytesVal.size > 0) RLCDataBytesVal.last else null

          val RBConfigIndexList: List[String] = bean47181_4.sr_18605_inst_list.asScala.toList.map(_.RBConfigIndex).distinct
          var numReTxPDUSum_0xB84DVal: lang.Long = 0
          var numMissedUMPDUSum_0xB84DVal: lang.Long = 0
          var numDroppedPDUSum_0xB84DVal: lang.Long = 0
          var numDataPDUSum_0xB84DVal: lang.Long = 0
          for (index <- RBConfigIndexList) {
            val lastRBStats = bean47181_4.sr_18605_inst_list.asScala.toList.filter(_.RBConfigIndex == index).last
            val num_ReTx_PDU = lastRBStats.NumReTxPDU
            val num_Missed_UM_PDU = lastRBStats.NumMissedUMPDU
            val num_Dropped_PDU = lastRBStats.NumDroppedPDU
            val num_Data_PDU = lastRBStats.NumDataPDU

            numReTxPDUSum_0xB84DVal = numReTxPDUSum_0xB84DVal + (if (num_ReTx_PDU != null && num_ReTx_PDU.trim.nonEmpty) parseLong(num_ReTx_PDU) else 0)
            numMissedUMPDUSum_0xB84DVal = numMissedUMPDUSum_0xB84DVal + (if (num_Missed_UM_PDU != null && num_Missed_UM_PDU.trim.nonEmpty) parseLong(num_Missed_UM_PDU) else 0)
            numDroppedPDUSum_0xB84DVal = numDroppedPDUSum_0xB84DVal + (if (num_Dropped_PDU != null && num_Dropped_PDU.trim.nonEmpty) parseLong(num_Dropped_PDU) else 0)
            numDataPDUSum_0xB84DVal = numDataPDUSum_0xB84DVal + (if (num_Data_PDU != null && num_Data_PDU.trim.nonEmpty) parseLong(num_Data_PDU) else 0)
          }

          //logger.info(s"numReTxPDUSum_0xB84D = $numReTxPDUSum_0xB84DVal, numMissedUMPDUSum_0xB84D = $numMissedUMPDUSum_0xB84DVal, numDroppedPDUSum_0xB84D = $numDroppedPDUSum_0xB84DVal, numDataPDUSum_0xB84D = $numDataPDUSum_0xB84DVal, time = ${logRecord.dmTimeStamp}")
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NumRB_0xB84D = bean47181_4.NumRB, NumInvalidPDUs = NumInvalidPDUsVal,
            NumDuplicatePDU = NumDuplicatePDUVal, NumDroppedPDU = NumDroppedPDUVal, rlcDataBytesVal_0xB84D = RLCDataBytesVal0xB84D,
            numReTxPDUSum_0xB84D = numReTxPDUSum_0xB84DVal, numMissedUMPDUSum_0xB84D = numMissedUMPDUSum_0xB84DVal, numDroppedPDUSum_0xB84D = numDroppedPDUSum_0xB84DVal, numDataPDUSum_0xB84D = numDataPDUSum_0xB84DVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47181", hexLogCode = "0xB84D", logRecordQComm5G = logRecordQComm5GObj)

        case _ =>
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47181", hexLogCode = "0xB84D", missingVersion = logVersion)
      }
    }

    def parse47247 = {
      logVersion match {
        case 131075 => {
          val bean47247_131075 = process47247_131075(x, exceptionUseArray)
          //          logger.info("bean47247_131075 -> Bean as JSON >>>>>>>>" + Constants.getJsonFromBean(bean47247_131075))
          val pccBeamInfoList = bean47247_131075.sr_57380_inst_list.asScala.toList.flatMap(_.sr_57413_inst_list.asScala.toList).filter(bi => bi.CarrierID.equalsIgnoreCase("0"))
          val scc1BeamInfoList = bean47247_131075.sr_57380_inst_list.asScala.toList.flatMap(_.sr_57413_inst_list.asScala.toList).filter(bi => bi.CarrierID.equalsIgnoreCase("1"))
          val pccBeamIdList = pccBeamInfoList.flatMap(_.sr_57436_inst_list.asScala.toList).map(_.BeamID).filter(id => id.trim.nonEmpty).map(id => parseInteger(id))
          val scc1BeamIdList = scc1BeamInfoList.flatMap(_.sr_57436_inst_list.asScala.toList).map(_.BeamID).filter(id => id.trim.nonEmpty).map(id => parseInteger(id))

          logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRTxBeamIndexPCC = pccBeamIdList, NRTxBeamIndexSCC1 = scc1BeamIdList)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47247", hexLogCode = "0xB88F", logRecordQComm5G2 = logRecordQComm5G2Obj)
        }
        case 65538 => {
          val bean47247_65538 = process47247_65538(x, exceptionUseArray)
          //          logger.info("bean47247_65538 -> Bean as JSON >>>>>>>>" + Constants.getJsonFromBean(bean47247_65538))
          val pccBeamInfoList = bean47247_65538.sr_11985_inst_list.asScala.toList.flatMap(_.sr_12001_inst_list.asScala.toList).filter(bi => bi.CarrierID.equalsIgnoreCase("0"))
          val scc1BeamInfoList = bean47247_65538.sr_11985_inst_list.asScala.toList.flatMap(_.sr_12001_inst_list.asScala.toList).filter(bi => bi.CarrierID.equalsIgnoreCase("1"))
          val pccBeamIdList = pccBeamInfoList.flatMap(_.sr_12049_inst_list.asScala.toList).map(_.BeamID).filter(id => id.trim.nonEmpty).map(id => parseInteger(id))
          val scc1BeamIdList = scc1BeamInfoList.flatMap(_.sr_12049_inst_list.asScala.toList).map(_.BeamID).filter(id => id.trim.nonEmpty).map(id => parseInteger(id))

          logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRTxBeamIndexPCC = pccBeamIdList, NRTxBeamIndexSCC1 = scc1BeamIdList)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47247", hexLogCode = "0xB88F", logRecordQComm5G2 = logRecordQComm5G2Obj)
        }
        case _ =>
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47247", hexLogCode = "0xB88F", missingVersion = logVersion)
      }
    }

    def parse47249 = {
      //logRecordQComm5GObj = logRecordQComm5GObj.copy(is5GTechnology = true)
      //Beam kpis
      logVersion match {
        case 65536 => {
          val bean47249_65536 = process47249_65536(x, exceptionUseArray)
          //logger.info("bean47249_65536 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47249_65536))
          //logger.info("bean47249_65536 ------ type "+bean47249_65536.getClass())
          val rsrp1arr: List[String] = bean47249_65536.getSr_2922_inst_list().asScala.toList.
            map(_.getSr_2958_inst_list.asScala.toList.filter(_.Resource_Type.equalsIgnoreCase("SSB")).map(_.RSRP_1)).flatten

          val rsrp1 = if (rsrp1arr != null && rsrp1arr.size > 0) rsrp1arr.head else null
          val resource1arr = bean47249_65536.getSr_2922_inst_list().asScala.toList.
            map(_.getSr_2958_inst_list.asScala.toList.filter(_.Resource_Type.equalsIgnoreCase("SSB")).map(_.Resource_1)).flatten
          val resource1 = if (resource1arr != null && resource1arr.size > 0) resource1arr.head else null
          //bean47249_65536
          logRecordQComm5GObj = logRecordQComm5GObj.copy(rsrp1_0xB891 = rsrp1, resource1_0xB891 = resource1)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47249", hexLogCode = "0xB891", logRecordQComm5G = logRecordQComm5GObj)
        }
        case 65537 => {
          val bean47249_65537 = process47249_65537(x, exceptionUseArray)
          //logger.info("bean47249_65537 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47249_65537))
          //logger.info("bean47249_65537 ------ type "+bean47249_65537.getClass())
          val rsrp1arr: List[String] = bean47249_65537.getSr_22716_inst_list.asScala.toList
            .filter(_.ReportQuantity.contains("RSRP,SSB_INDEX"))
            .map(_.sr_22742_inst_list.asScala.toList.map(_.RSRP1)).flatten
          val rsrp1 = if (rsrp1arr != null && rsrp1arr.size > 0) rsrp1arr.head else null
          val resource1arr: List[String] = bean47249_65537.getSr_22716_inst_list.asScala.toList
            .filter(_.ReportQuantity.contains("RSRP,SSB_INDEX"))
            .map(_.sr_22742_inst_list.asScala.toList.map(_.Resource1)).flatten
          val resource1 = if (resource1arr != null && resource1arr.size > 0) resource1arr.head else null
          logRecordQComm5GObj = logRecordQComm5GObj.copy(rsrp1_0xB891 = rsrp1, resource1_0xB891 = resource1)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47249", hexLogCode = "0xB891", logRecordQComm5G = logRecordQComm5GObj)
        }
        case _ =>
          //logger.info(s"bean47249_$logVersion -> Bean as JSON not found!!!!!!!!!!!>>>>>>>>")
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47249", hexLogCode = "0xB891", missingVersion = logVersion)
      }
    }

    def parse47271 = {
      var NRRankIndexPCCVal: Integer = null
      var NRRankIndexSCC1Val: Integer = null
      var NRRankIndexSCC2Val: Integer = null
      var NRRankIndexSCC3Val: Integer = null
      var NRRankIndexSCC4Val: Integer = null
      var NRRankIndexSCC5Val: Integer = null
      var NRRankIndexSCC6Val: Integer = null
      var NRRankIndexSCC7Val: Integer = null
      var NRCQIWBPCCVal: Integer = null
      var NRCQIWBSCC1Val: Integer = null
      var NRCQIWBSCC2Val: Integer = null
      var NRCQIWBSCC3Val: Integer = null
      var NRCQIWBSCC4Val: Integer = null
      var NRCQIWBSCC5Val: Integer = null
      var NRCQIWBSCC6Val: Integer = null
      var NRCQIWBSCC7Val: Integer = null
      val ReportQuantityBitmask = "CRI,RI,PMI,CQI,"

      try {
        logVersion match {
          case 131073 =>
            val bean47271_131073 = process47271_131073(x, exceptionUseArray)
            //          logger.info("bean47271_131073 -> Bean as JSON>>>>>>>>" + Constants.getJsonFromBean(bean47271_131073))

            def getRankIndex(inst: Dynamic_47271_131073_SR_28370_DTO): Integer = {
              try {
                Integer.parseInt(inst.sr_28395_inst_list.asScala.toList.head.sr_28397_inst_list.asScala.toList.head.RI)
              } catch {
                case _: NumberFormatException => null
              }
            }

            def getCQIWB(inst: Dynamic_47271_131073_SR_28370_DTO): Integer = {
              try {
                Integer.parseInt(inst.sr_28395_inst_list.asScala.toList.head.sr_28397_inst_list.asScala.toList.head.WBCQI)
              } catch {
                case _: NumberFormatException => null
              }
            }

            bean47271_131073.sr_28334_inst_list.asScala.toList.flatMap(_.sr_28370_inst_list.asScala.toList).filter(_.ReportQuantityBitmask.equals(ReportQuantityBitmask)).foreach {
              case inst if inst.CarrierID.equals("0") =>
                NRRankIndexPCCVal = getRankIndex(inst)
                //              logger.info("NRRankIndexPCCVal=" + NRRankIndexPCCVal)
                NRCQIWBPCCVal = getCQIWB(inst)
              //              logger.info("NRCQIWBPCCVal=" + NRCQIWBPCCVal)
              case inst if inst.CarrierID.equals("1") =>
                NRRankIndexSCC1Val = getRankIndex(inst)
                //              logger.info("NRRankIndexSCC1Val=" + NRRankIndexSCC1Val)
                NRCQIWBSCC1Val = getCQIWB(inst)
              //              logger.info("NRCQIWBSCC1Val=" + NRCQIWBSCC1Val)
              case inst if inst.CarrierID.equals("2") =>
                NRRankIndexSCC2Val = getRankIndex(inst)
                NRCQIWBSCC2Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("3") =>
                NRRankIndexSCC3Val = getRankIndex(inst)
                NRCQIWBSCC3Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("4") =>
                NRRankIndexSCC4Val = getRankIndex(inst)
                NRCQIWBSCC4Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("5") =>
                NRRankIndexSCC5Val = getRankIndex(inst)
                NRCQIWBSCC5Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("6") =>
                NRRankIndexSCC6Val = getRankIndex(inst)
                NRCQIWBSCC6Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("7") =>
                NRRankIndexSCC7Val = getRankIndex(inst)
                NRCQIWBSCC7Val = getCQIWB(inst)
            }

            logRecordQComm5GObj = logRecordQComm5GObj.copy(
              NRRankIndexPCC = NRRankIndexPCCVal, NRRankIndexSCC1 = NRRankIndexSCC1Val, NRRankIndexSCC2 = NRRankIndexSCC2Val, NRRankIndexSCC3 = NRRankIndexSCC3Val,
              NRRankIndexSCC4 = NRRankIndexSCC4Val, NRRankIndexSCC5 = NRRankIndexSCC5Val, NRRankIndexSCC6 = NRRankIndexSCC6Val, NRRankIndexSCC7 = NRRankIndexSCC7Val,
              NRCQIWBPCC = NRCQIWBPCCVal, NRCQIWBSCC1 = NRCQIWBSCC1Val, NRCQIWBSCC2 = NRCQIWBSCC2Val, NRCQIWBSCC3 = NRCQIWBSCC3Val,
              NRCQIWBSCC4 = NRCQIWBSCC4Val, NRCQIWBSCC5 = NRCQIWBSCC5Val, NRCQIWBSCC6 = NRCQIWBSCC6Val, NRCQIWBSCC7 = NRCQIWBSCC7Val)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47271", hexLogCode = "0xB8A7", logRecordQComm5G = logRecordQComm5GObj)

          case 131075 =>
            val bean47271_131075 = process47271_131075(x, exceptionUseArray)

            def getRankIndex(inst: Dynamic_47271_131075_SR_48775_DTO): Integer = {
              try {
                Integer.parseInt(inst.sr_48776_inst_list.asScala.toList.head.sr_48779_inst_list.asScala.toList.head.RI)
              } catch {
                case _: NumberFormatException => null
              }
            }

            def getCQIWB(inst: Dynamic_47271_131075_SR_48775_DTO): Integer = {
              try {
                Integer.parseInt(inst.sr_48776_inst_list.asScala.toList.head.sr_48779_inst_list.asScala.toList.head.WBCQI)
              } catch {
                case _: NumberFormatException => null
              }
            }

            bean47271_131075.sr_48766_inst_list.asScala.toList.flatMap(_.sr_48775_inst_list.asScala.toList).filter(_.ReportQuantityBitmask.equals(ReportQuantityBitmask)).foreach {
              case inst if inst.CarrierID.equals("0") =>
                NRRankIndexPCCVal = getRankIndex(inst)
                NRCQIWBPCCVal = getCQIWB(inst)
              case inst if inst.CarrierID.equals("1") =>
                NRRankIndexSCC1Val = getRankIndex(inst)
                NRCQIWBSCC1Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("2") =>
                NRRankIndexSCC2Val = getRankIndex(inst)
                NRCQIWBSCC2Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("3") =>
                NRRankIndexSCC3Val = getRankIndex(inst)
                NRCQIWBSCC3Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("4") =>
                NRRankIndexSCC4Val = getRankIndex(inst)
                NRCQIWBSCC4Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("5") =>
                NRRankIndexSCC5Val = getRankIndex(inst)
                NRCQIWBSCC5Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("6") =>
                NRRankIndexSCC6Val = getRankIndex(inst)
                NRCQIWBSCC6Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("7") =>
                NRRankIndexSCC7Val = getRankIndex(inst)
                NRCQIWBSCC7Val = getCQIWB(inst)
            }

            logRecordQComm5GObj = logRecordQComm5GObj.copy(
              NRRankIndexPCC = NRRankIndexPCCVal, NRRankIndexSCC1 = NRRankIndexSCC1Val, NRRankIndexSCC2 = NRRankIndexSCC2Val, NRRankIndexSCC3 = NRRankIndexSCC3Val,
              NRRankIndexSCC4 = NRRankIndexSCC4Val, NRRankIndexSCC5 = NRRankIndexSCC5Val, NRRankIndexSCC6 = NRRankIndexSCC6Val, NRRankIndexSCC7 = NRRankIndexSCC7Val,
              NRCQIWBPCC = NRCQIWBPCCVal, NRCQIWBSCC1 = NRCQIWBSCC1Val, NRCQIWBSCC2 = NRCQIWBSCC2Val, NRCQIWBSCC3 = NRCQIWBSCC3Val,
              NRCQIWBSCC4 = NRCQIWBSCC4Val, NRCQIWBSCC5 = NRCQIWBSCC5Val, NRCQIWBSCC6 = NRCQIWBSCC6Val, NRCQIWBSCC7 = NRCQIWBSCC7Val)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47271", hexLogCode = "0xB8A7", logRecordQComm5G = logRecordQComm5GObj)

          case 196612 =>
            val bean47271_196612 = process47271_196612(x, exceptionUseArray)

            def getRankIndex(inst: Dynamic_47271_196612_SR_58684_DTO): Integer = {
              try {
                Integer.parseInt(inst.sr_58682_inst_list.asScala.toList.head.sr_58678_inst_list.asScala.toList.head.RI)
              } catch {
                case _: NumberFormatException => null
              }
            }

            def getCQIWB(inst: Dynamic_47271_196612_SR_58684_DTO): Integer = {
              try {
                Integer.parseInt(inst.sr_58682_inst_list.asScala.toList.head.sr_58678_inst_list.asScala.toList.head.WBCQI)
              } catch {
                case _: NumberFormatException => null
              }
            }

            bean47271_196612.sr_58703_inst_list.asScala.toList.flatMap(_.sr_58684_inst_list.asScala.toList).filter(_.ReportQuantityBitmask.equals(ReportQuantityBitmask)).foreach {
              case inst if inst.CarrierID.equals("0") =>
                NRRankIndexPCCVal = getRankIndex(inst)
                NRCQIWBPCCVal = getCQIWB(inst)
              case inst if inst.CarrierID.equals("1") =>
                NRRankIndexSCC1Val = getRankIndex(inst)
                NRCQIWBSCC1Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("2") =>
                NRRankIndexSCC2Val = getRankIndex(inst)
                NRCQIWBSCC2Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("3") =>
                NRRankIndexSCC3Val = getRankIndex(inst)
                NRCQIWBSCC3Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("4") =>
                NRRankIndexSCC4Val = getRankIndex(inst)
                NRCQIWBSCC4Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("5") =>
                NRRankIndexSCC5Val = getRankIndex(inst)
                NRCQIWBSCC5Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("6") =>
                NRRankIndexSCC6Val = getRankIndex(inst)
                NRCQIWBSCC6Val = getCQIWB(inst)
              case inst if inst.CarrierID.equals("7") =>
                NRRankIndexSCC7Val = getRankIndex(inst)
                NRCQIWBSCC7Val = getCQIWB(inst)
            }

            logRecordQComm5GObj = logRecordQComm5GObj.copy(
              NRRankIndexPCC = NRRankIndexPCCVal, NRRankIndexSCC1 = NRRankIndexSCC1Val, NRRankIndexSCC2 = NRRankIndexSCC2Val, NRRankIndexSCC3 = NRRankIndexSCC3Val,
              NRRankIndexSCC4 = NRRankIndexSCC4Val, NRRankIndexSCC5 = NRRankIndexSCC5Val, NRRankIndexSCC6 = NRRankIndexSCC6Val, NRRankIndexSCC7 = NRRankIndexSCC7Val,
              NRCQIWBPCC = NRCQIWBPCCVal, NRCQIWBSCC1 = NRCQIWBSCC1Val, NRCQIWBSCC2 = NRCQIWBSCC2Val, NRCQIWBSCC3 = NRCQIWBSCC3Val,
              NRCQIWBSCC4 = NRCQIWBSCC4Val, NRCQIWBSCC5 = NRCQIWBSCC5Val, NRCQIWBSCC6 = NRCQIWBSCC6Val, NRCQIWBSCC7 = NRCQIWBSCC7Val)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47271", hexLogCode = "0xB8A7", logRecordQComm5G = logRecordQComm5GObj)

          case _ =>
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47271", hexLogCode = "0xB8A7", missingVersion = logVersion)
        }
      } catch {
        case e: Throwable =>
          logger.error(s"Got exception for logCode=$logCode, logVersion=$logVersion", e)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47271", hexLogCode = "0xB8A7", exceptionOccured = true, exceptionCause = e.toString)
      }
    }

    def parse47477 = {
      logRecordQComm5GObj = logRecordQComm5GObj.copy(is5GTechnology = true)
      logVersion match {
        case 65537 =>
          //logger.info("bean47477_65537 -> Bean as JSON>>>>>>>>")
          val bean47477_65537 = process47477_65537(x, exceptionUseArray)
          //logger.info("bean47477_65537 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47477_65537))
          val NRSBRSRPVal = bean47477_65537.sr_8366_inst_list.asScala.toList.map(_.BRSRP_Filtered_Value)
          val NRSBRSRQVal = bean47477_65537.sr_8367_inst_list.asScala.toList.map(_.RSRQ_Filtered_Value)
          val NRSBSNRVal = bean47477_65537.SNR
          val NRPCIVal = bean47477_65537.PCI
          val NRBEAMCOUNTVal = bean47477_65537.Num_Detected_Beams
          val CCIndexVal = bean47477_65537.CC_Index
          val NRServingBeamIndexVal: Integer = if (Try(bean47477_65537.Serving_Beam_SSB_Index.toInt).isSuccess) bean47477_65537.Serving_Beam_SSB_Index.toInt.asInstanceOf[Integer] else null
          logRecordQComm5GObj = logRecordQComm5GObj.copy(
            NRSBRSRP = NRSBRSRPVal, NRSBRSRQ = NRSBRSRQVal, NRSBSNR = NRSBSNRVal, NRBEAMCOUNT = NRBEAMCOUNTVal, CCIndex = CCIndexVal, NRPCI = NRPCIVal, NRServingBeamIndex = NRServingBeamIndexVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47477", hexLogCode = "0xB975", logRecordQComm5G = logRecordQComm5GObj)
        case 65538 =>
          //logger.info("bean47477_65537 -> Bean as JSON>>>>>>>>")
          val bean47477_65538 = process47477_65538(x, exceptionUseArray)
          //logger.info("bean47477_65538 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47477_2))
          val NRSBRSRPVal = bean47477_65538.sr_11909_inst_list.asScala.toList.map(_.BRSRPFilteredValue)
          val NRSBRSRQVal = bean47477_65538.sr_11910_inst_list.asScala.toList.map(_.RSRQFilteredValue)
          val NRSBSNRVal = bean47477_65538.SNR
          val NRPCIVal = bean47477_65538.PCI
          val NRBEAMCOUNTVal = bean47477_65538.NumDetectedBeams
          val CCIndexVal = bean47477_65538.CCIndex
          val NRServingBeamIndexVal = if (Try(bean47477_65538.ServingBeamSSBIndex.toInt).isSuccess) bean47477_65538.ServingBeamSSBIndex.toInt.asInstanceOf[Integer] else null
          logRecordQComm5GObj = logRecordQComm5GObj.copy(
            NRSBRSRP = NRSBRSRPVal, NRSBRSRQ = NRSBRSRQVal, NRSBSNR = NRSBSNRVal, NRBEAMCOUNT = NRBEAMCOUNTVal, CCIndex = CCIndexVal, NRPCI = NRPCIVal, NRServingBeamIndex = NRServingBeamIndexVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47477", hexLogCode = "0xB975", logRecordQComm5G = logRecordQComm5GObj)
        case 65539 =>
          //logger.info("bean47477_65537 -> Bean as JSON>>>>>>>>")
          val bean47477_65539 = process47477_65539(x, exceptionUseArray)
          //logger.info("bean47477_65539 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47477_65539))
          val NRSBRSRPVal = bean47477_65539.sr_16174_inst_list.asScala.toList.map(_.BRSRPFilteredValue)
          val NRSBRSRQVal = bean47477_65539.sr_16157_inst_list.asScala.toList.map(_.RSRQFilteredValue)
          val NRSBSNRVal = bean47477_65539.SNR
          val NRPCIVal = bean47477_65539.PCI
          val NRBEAMCOUNTVal = bean47477_65539.NumDetectedBeams
          val CCIndexVal = bean47477_65539.CCIndex
          val NRServingBeamIndexVal = if (Try(bean47477_65539.ServingBeamSSBIndex.toInt).isSuccess) bean47477_65539.ServingBeamSSBIndex.toInt.asInstanceOf[Integer] else null
          logRecordQComm5GObj = logRecordQComm5GObj.copy(
            NRSBRSRP = NRSBRSRPVal, NRSBRSRQ = NRSBRSRQVal, NRSBSNR = NRSBSNRVal, NRBEAMCOUNT = NRBEAMCOUNTVal, CCIndex = CCIndexVal, NRPCI = NRPCIVal, NRServingBeamIndex = NRServingBeamIndexVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47477", hexLogCode = "0xB975", logRecordQComm5G = logRecordQComm5GObj)
        case 65540 =>
          //logger.info("bean47477_65537 -> Bean as JSON>>>>>>>>")
          val bean47477_65540 = process47477_65540(x, exceptionUseArray)
          val NRSBRSRPVal = bean47477_65540.sr_17043_inst_list.asScala.toList.map(_.BRSRPFilteredValue)
          val NRSBRSRQVal = bean47477_65540.sr_17033_inst_list.asScala.toList.map(_.RSRQFilteredValue)
          val NRSBSNRVal = bean47477_65540.SNR
          val NRPCIVal = bean47477_65540.PCI
          val NRBEAMCOUNTVal = bean47477_65540.NumDetectedBeams
          val CCIndexVal = bean47477_65540.CCIndex
          val NRServingBeamIndexVal = if (Try(bean47477_65540.ServingBeamSSBIndex.toInt).isSuccess) bean47477_65540.ServingBeamSSBIndex.toInt.asInstanceOf[Integer] else null
          logRecordQComm5GObj = logRecordQComm5GObj.copy(
            NRSBRSRP = NRSBRSRPVal, NRSBRSRQ = NRSBRSRQVal, NRSBSNR = NRSBSNRVal, NRBEAMCOUNT = NRBEAMCOUNTVal, CCIndex = CCIndexVal, NRPCI = NRPCIVal, NRServingBeamIndex = NRServingBeamIndexVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47477", hexLogCode = "0xB975", logRecordQComm5G = logRecordQComm5GObj)
        case 65541 =>
          //logger.info("bean47477_65537 -> Bean as JSON>>>>>>>>")
          val bean47477_65541 = process47477_65541(x, exceptionUseArray)
          val NRSBRSRPVal = bean47477_65541.sr_17058_inst_list.asScala.toList.map(_.BRSRPFilteredValue)
          val NRSBRSRQVal = bean47477_65541.sr_17048_inst_list.asScala.toList.map(_.RSRQFilteredValue)
          val NRSBSNRVal = bean47477_65541.SNR
          val NRPCIVal = bean47477_65541.PCI
          val NRBEAMCOUNTVal = bean47477_65541.NumDetectedBeams
          val CCIndexVal = bean47477_65541.CCIndex
          val NRServingBeamIndexVal = if (Try(bean47477_65541.ServingBeamSSBIndex.toInt).isSuccess) bean47477_65541.ServingBeamSSBIndex.toInt.asInstanceOf[Integer] else null
          logRecordQComm5GObj = logRecordQComm5GObj.copy(
            NRSBRSRP = NRSBRSRPVal, NRSBRSRQ = NRSBRSRQVal, NRSBSNR = NRSBSNRVal, NRBEAMCOUNT = NRBEAMCOUNTVal, CCIndex = CCIndexVal, NRPCI = NRPCIVal, NRServingBeamIndex = NRServingBeamIndexVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47477", hexLogCode = "0xB975", logRecordQComm5G = logRecordQComm5GObj)
        case 131073 =>
          //logger.info("bean47477_65537 -> Bean as JSON>>>>>>>>")
          val bean47477_131073 = process47477_131073(x, exceptionUseArray)
          val NRSBRSRPVal = bean47477_131073.sr_19472_inst_list.asScala.toList.map(_.BRSRPFilteredValue)
          val NRSBRSRQVal = bean47477_131073.sr_19473_inst_list.asScala.toList.map(_.RSRQFilteredValue)
          val NRPCIVal = bean47477_131073.PCI
          val NRBEAMCOUNTVal = bean47477_131073.NumDetectedBeams
          val CCIndexVal = bean47477_131073.CCIndex
          val NRServingBeamIndexVal = if (Try(bean47477_131073.ServingBeamSSBIndex.toInt).isSuccess) bean47477_131073.ServingBeamSSBIndex.toInt.asInstanceOf[Integer] else null
          logRecordQComm5GObj = logRecordQComm5GObj.copy(
            NRSBRSRP = NRSBRSRPVal, NRSBRSRQ = NRSBRSRQVal, NRBEAMCOUNT = NRBEAMCOUNTVal, CCIndex = CCIndexVal, NRPCI = NRPCIVal, NRServingBeamIndex = NRServingBeamIndexVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47477", hexLogCode = "0xB975", logRecordQComm5G = logRecordQComm5GObj)

        case _ =>
          //logger.info("bean47477_65537 -> Bean as JSON>>>>>>>>")
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47477", hexLogCode = "0xB975", missingVersion = logVersion)
      }
    }

    logCode match {
      case 47138 =>
        parse47138
      case 47170 =>
        parse47170
      case 47181 =>
        parse47181
      case 47247 =>
        parse47247
      case 47249 =>
        parse47249
      case 47271 =>
        parse47271
      case 47477 =>
        parse47477
    }
    logRecord
  }

  def parseLogRecordQCOMM5GSet2(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQComm5GObj: LogRecordQComm5G = LogRecordQComm5G()

    def parse47236 = {
      logVersion match {
        case 65538 =>
          val bean47236_65538 = process47236_65538(x, exceptionUseArray)
          val PhychanBitMaskVal = bean47236_65538.sr_1240_inst_list.asScala.toList.map(_.sr_1260_inst_list.asScala.toList.map(_.Phychan_Bit_Mask)).flatten
          val PUSCHDLPathLossVal = bean47236_65538.sr_1240_inst_list.asScala.toList.map(_.sr_1260_inst_list.asScala.toList.map(_.sr_1267_inst_list.asScala.toList.map(_.DL_Path_Loss))).flatten.flatten
          val PUCCHDLPathLossVal = bean47236_65538.sr_1240_inst_list.asScala.toList.map(_.sr_1260_inst_list.asScala.toList.map(_.sr_1268_inst_list.asScala.toList.map(_.sr_1285_inst_list.asScala.toList.map(_.DL_Path_Loss)))).flatten.flatten.flatten
          val SRSDLPathLossVal = bean47236_65538.sr_1240_inst_list.asScala.toList.map(_.sr_1260_inst_list.asScala.toList.map(_.sr_1269_inst_list.asScala.toList.map(_.DL_Path_Loss))).flatten.flatten
          val PRACDLPathLossVal = bean47236_65538.sr_1240_inst_list.asScala.toList.map(_.sr_1260_inst_list.asScala.toList.map(_.sr_1270_inst_list.asScala.toList.map(_.DL_Path_Loss))).flatten.flatten
          val PUSCHTxPwrVal = bean47236_65538.sr_1240_inst_list.asScala.toList.map(_.sr_1260_inst_list.asScala.toList.map(_.sr_1267_inst_list.asScala.toList.map(_.PUSCH_Transmit_Power))).flatten.flatten
          logRecordQComm5GObj = logRecordQComm5GObj.copy(PhychanBitMask_0xB884 = PhychanBitMaskVal, PUSCHDLPathLoss_0xB884 = PUSCHDLPathLossVal, PUCCHDLPathLoss_0xB884 = PUCCHDLPathLossVal,
            SRSDLPathLoss_0xB884 = SRSDLPathLossVal, PRACPathLoss_0xB884 = PRACDLPathLossVal, PUSCHTxPwr_0xB884 = PUSCHTxPwrVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47236", hexLogCode = "0xB884", logRecordQComm5G = logRecordQComm5GObj)
        case 65539 =>
          val bean47236_65539 = process47236_65539(x, exceptionUseArray)
          val PhychanBitMaskVal = bean47236_65539.sr_12478_inst_list.asScala.toList.map(_.sr_12479_inst_list.asScala.toList.map(_.Phychan_Bit_Mask)).flatten
          val PUSCHDLPathLossVal = bean47236_65539.sr_12478_inst_list.asScala.toList.map(_.sr_12479_inst_list.asScala.toList.map(_.sr_12516_inst_list.asScala.toList.map(_.DL_Path_Loss))).flatten.flatten
          val PUCCHDLPathLossVal = bean47236_65539.sr_12478_inst_list.asScala.toList.map(_.sr_12479_inst_list.asScala.toList.map(_.sr_12517_inst_list.asScala.toList.map(_.sr_12509_inst_list.asScala.toList.map(_.DL_Path_Loss)))).flatten.flatten.flatten
          val SRSDLPathLossVal = bean47236_65539.sr_12478_inst_list.asScala.toList.map(_.sr_12479_inst_list.asScala.toList.map(_.sr_12518_inst_list.asScala.toList.map(_.DL_Path_Loss))).flatten.flatten
          val PRACDLPathLossVal = bean47236_65539.sr_12478_inst_list.asScala.toList.map(_.sr_12479_inst_list.asScala.toList.map(_.sr_12519_inst_list.asScala.toList.map(_.DL_Path_Loss))).flatten.flatten
          val PUSCHTxPwrVal = bean47236_65539.sr_12478_inst_list.asScala.toList.map(_.sr_12479_inst_list.asScala.toList.map(_.sr_12516_inst_list.asScala.toList.map(_.PUSCH_Transmit_Power))).flatten.flatten
          logRecordQComm5GObj = logRecordQComm5GObj.copy(PhychanBitMask_0xB884 = PhychanBitMaskVal, PUSCHDLPathLoss_0xB884 = PUSCHDLPathLossVal, PUCCHDLPathLoss_0xB884 = PUCCHDLPathLossVal,
            SRSDLPathLoss_0xB884 = SRSDLPathLossVal, PRACPathLoss_0xB884 = PRACDLPathLossVal, PUSCHTxPwr_0xB884 = PUSCHTxPwrVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47236", hexLogCode = "0xB884", logRecordQComm5G = logRecordQComm5GObj)
        case 65540 =>
          val bean47236_65540 = process47236_65540(x, exceptionUseArray)
          val PhychanBitMaskVal = bean47236_65540.sr_12904_inst_list.asScala.toList.map(_.sr_12936_inst_list.asScala.toList.map(_.PhychanBitMask)).flatten
          val PUSCHDLPathLossVal = bean47236_65540.sr_12904_inst_list.asScala.toList.map(_.sr_12936_inst_list.asScala.toList.map(_.sr_12946_inst_list.asScala.toList.map(_.DLPathLoss))).flatten.flatten
          val PUCCHDLPathLossVal = bean47236_65540.sr_12904_inst_list.asScala.toList.map(_.sr_12936_inst_list.asScala.toList.map(_.sr_12957_inst_list.asScala.toList.map(_.sr_12914_inst_list.asScala.toList.map(_.DLPathLoss)))).flatten.flatten.flatten
          val SRSDLPathLossVal = bean47236_65540.sr_12904_inst_list.asScala.toList.map(_.sr_12936_inst_list.asScala.toList.map(_.sr_12958_inst_list.asScala.toList.map(_.DLPathLoss))).flatten.flatten
          val PRACDLPathLossVal = bean47236_65540.sr_12904_inst_list.asScala.toList.map(_.sr_12936_inst_list.asScala.toList.map(_.sr_12958_inst_list.asScala.toList.map(_.DLPathLoss))).flatten.flatten
          val PUSCHTxPwrVal = bean47236_65540.sr_12904_inst_list.asScala.toList.map(_.sr_12936_inst_list.asScala.toList.map(_.sr_12946_inst_list.asScala.toList.map(_.PUSCHTransmitPower))).flatten.flatten
          logRecordQComm5GObj = logRecordQComm5GObj.copy(PhychanBitMask_0xB884 = PhychanBitMaskVal, PUSCHDLPathLoss_0xB884 = PUSCHDLPathLossVal, PUCCHDLPathLoss_0xB884 = PUCCHDLPathLossVal,
            SRSDLPathLoss_0xB884 = SRSDLPathLossVal, PRACPathLoss_0xB884 = PRACDLPathLossVal, PUSCHTxPwr_0xB884 = PUSCHTxPwrVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47236", hexLogCode = "0xB884", logRecordQComm5G = logRecordQComm5GObj)
        case 65541 =>
          val bean47236_65541 = process47236_65541(x, exceptionUseArray)
          val PhychanBitMaskVal = bean47236_65541.sr_19530_inst_list.asScala.toList.map(_.sr_19564_inst_list.asScala.toList.map(_.PhychanBitMask)).flatten
          val PUSCHDLPathLossVal = bean47236_65541.sr_19530_inst_list.asScala.toList.map(_.sr_19564_inst_list.asScala.toList.map(_.sr_19576_inst_list.asScala.toList.map(_.DLPathLoss))).flatten.flatten
          val PUCCHDLPathLossVal = bean47236_65541.sr_19530_inst_list.asScala.toList.map(_.sr_19564_inst_list.asScala.toList.map(_.sr_19580_inst_list.asScala.toList.map(_.sr_19535_inst_list.asScala.toList.map(_.DLPathLoss)))).flatten.flatten.flatten
          val SRSDLPathLossVal = bean47236_65541.sr_19530_inst_list.asScala.toList.map(_.sr_19564_inst_list.asScala.toList.map(_.sr_19581_inst_list.asScala.toList.map(_.DLPathLoss))).flatten.flatten
          val PRACDLPathLossVal = bean47236_65541.sr_19530_inst_list.asScala.toList.map(_.sr_19564_inst_list.asScala.toList.map(_.sr_19579_inst_list.asScala.toList.map(_.DLPathLoss))).flatten.flatten
          val PUSCHTxPwrVal = bean47236_65541.sr_19530_inst_list.asScala.toList.map(_.sr_19564_inst_list.asScala.toList.map(_.sr_19576_inst_list.asScala.toList.map(_.PUSCHTransmitPower))).flatten.flatten
          logRecordQComm5GObj = logRecordQComm5GObj.copy(PhychanBitMask_0xB884 = PhychanBitMaskVal, PUSCHDLPathLoss_0xB884 = PUSCHDLPathLossVal, PUCCHDLPathLoss_0xB884 = PUCCHDLPathLossVal,
            SRSDLPathLoss_0xB884 = SRSDLPathLossVal, PRACPathLoss_0xB884 = PRACDLPathLossVal, PUSCHTxPwr_0xB884 = PUSCHTxPwrVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47236", hexLogCode = "0xB884", logRecordQComm5G = logRecordQComm5GObj)
        case 196611 =>
          val bean47236_196611 = process47236_196611(x, exceptionUseArray)
          val PUSCHDLPathLossVal = bean47236_196611.sr_58239_inst_list.asScala.toList.flatMap(_.sr_58265_inst_list.asScala.toList.flatMap(_.sr_58312_inst_list.asScala.toList.filter(_.ChannelType.equalsIgnoreCase("PUSCH")).map(_.Pathloss)))
          val PUCCHDLPathLossVal = bean47236_196611.sr_58239_inst_list.asScala.toList.flatMap(_.sr_58265_inst_list.asScala.toList.flatMap(_.sr_58312_inst_list.asScala.toList.filter(_.ChannelType.equalsIgnoreCase("PUCCH")).map(_.Pathloss)))
          val SRSDLPathLossVal = bean47236_196611.sr_58239_inst_list.asScala.toList.flatMap(_.sr_58265_inst_list.asScala.toList.flatMap(_.sr_58312_inst_list.asScala.toList.filter(_.ChannelType.equalsIgnoreCase("SRS")).map(_.Pathloss)))
          val PRACDLPathLossVal = bean47236_196611.sr_58239_inst_list.asScala.toList.flatMap(_.sr_58265_inst_list.asScala.toList.flatMap(_.sr_58312_inst_list.asScala.toList.filter(_.ChannelType.equalsIgnoreCase("PRACH")).map(_.Pathloss)))
          val PUSCHTransmitPower = bean47236_196611.sr_58239_inst_list.asScala.toList.flatMap(_.sr_58265_inst_list.asScala.toList.flatMap(_.sr_58312_inst_list.asScala.toList.filter(_.ChannelType.equalsIgnoreCase("PUSCH")).map(_.TransmitPower)))
          val PUSCHPHRMTPL = bean47236_196611.sr_58239_inst_list.asScala.toList.flatMap(_.sr_58265_inst_list.asScala.toList.flatMap(_.sr_58312_inst_list.asScala.toList.filter(_.ChannelType.equalsIgnoreCase("PUSCH")).map(_.PHRMTPL)))
          var PUSCHTxPwrVal: List[String] = null
          if (PUSCHTransmitPower.nonEmpty && PUSCHPHRMTPL.nonEmpty) {
            PUSCHTxPwrVal = List(math.min(parseDouble(PUSCHTransmitPower.head), parseDouble(PUSCHPHRMTPL.head)).toString)
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(PUSCHDLPathLoss_0xB884 = PUSCHDLPathLossVal, PUCCHDLPathLoss_0xB884 = PUCCHDLPathLossVal,
            SRSDLPathLoss_0xB884 = SRSDLPathLossVal, PRACPathLoss_0xB884 = PRACDLPathLossVal, PUSCHTxPwr_0xB884 = PUSCHTxPwrVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47236", hexLogCode = "0xB884", logRecordQComm5G = logRecordQComm5GObj)
        case _ =>
          //logger.info("Missing Version for logcode:47236, hexlogCode:0xB884, Version:" + logVersion)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47236", hexLogCode = "0xB884", missingVersion = logVersion)
      }
    }

    def parse47240 = {
      logVersion match {
        case 1 =>
          val bean47240_1 = process47240_1(x, exceptionUseArray)
          //logger.info("bean47240_1 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47240_1))
          val NRDLBLERVal = if (Try(bean47240_1.sr_2472_inst_list.asScala.toList.map(_.BLER)).isSuccess) bean47240_1.sr_2472_inst_list.asScala.toList.map(_.BLER) else null
          val NRResidualBLERVal = if (Try(bean47240_1.sr_2472_inst_list.asScala.toList.map(_.Residual_BLER)).isSuccess) bean47240_1.sr_2472_inst_list.asScala.toList.map(_.Residual_BLER) else null
          val crcPassTbSizeVal: List[String] = bean47240_1.sr_2472_inst_list.asScala.toList.map(_.CRC_Pass_TB_Bytes)

          var TB_Size: lang.Long = 0L
          var carrierID: Integer = null
          var tbSizeSum: Long = 0
          var tbSizeSumPcc: Long = 0
          var tbSizeSumScc1: Long = 0
          var tbSizeSumScc2: Long = 0
          var tbSizeSumScc3: Long = 0
          var tbSizeSumScc4: Long = 0
          var tbSizeSumScc5: Long = 0
          var tbSizeSumScc6: Long = 0
          var tbSizeSumScc7: Long = 0

          if (crcPassTbSizeVal != null) {
            for (i <- 0 to crcPassTbSizeVal.size - 1) {
              TB_Size += crcPassTbSizeVal(i).toLong
              carrierID = i
              if (carrierID != null) {
                carrierID.toInt match {
                  case 0 =>
                    tbSizeSumPcc += crcPassTbSizeVal(i).toLong
                  case 1 =>
                    tbSizeSumScc1 += crcPassTbSizeVal(i).toLong
                  case 2 =>
                    tbSizeSumScc2 += crcPassTbSizeVal(i).toLong
                  case 3 =>
                    tbSizeSumScc3 += crcPassTbSizeVal(i).toLong
                  case 4 =>
                    tbSizeSumScc4 += crcPassTbSizeVal(i).toLong
                  case 5 =>
                    tbSizeSumScc5 += crcPassTbSizeVal(i).toLong
                  case 6 =>
                    tbSizeSumScc6 += crcPassTbSizeVal(i).toLong
                  case 7 =>
                    tbSizeSumScc7 += crcPassTbSizeVal(i).toLong
                  case _ =>
                }
              }
            }
          }

          /*   var TB_Size: java.lang.Long = 0L
            if (crcPassTbSizeVal != null) {
              for (i <- 0 to crcPassTbSizeVal.size - 1) {
                TB_Size += crcPassTbSizeVal(i).toLong
              }
            }*/

          logRecordQComm5GObj = logRecordQComm5GObj.copy(
            NRResidualBLER = NRResidualBLERVal,
            NRDLBLER = NRDLBLERVal,
            TB_Size_0xB888 = TB_Size, TB_SizePcc_0xB888 = tbSizeSumPcc, TB_SizeScc1_0xB888 = tbSizeSumScc1, TB_SizeScc2_0xB888 = tbSizeSumScc2,
            TB_SizeScc3_0xB888 = tbSizeSumScc3, TB_SizeScc4_0xB888 = tbSizeSumScc4, TB_SizeScc5_0xB888 = tbSizeSumScc5, TB_SizeScc6_0xB888 = tbSizeSumScc6, TB_SizeScc7_0xB888 = tbSizeSumScc7, status0xB888 = true)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion,
            logCode = "47240",
            hexLogCode = "0xB888", logRecordQComm5G = logRecordQComm5GObj)
        case 65537 =>
          val bean47240_65537 = process47240_65537(x, exceptionUseArray)
          //logger.info("bean47240_65537 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47240_65537))
          val NRDLBLERVal = bean47240_65537.sr_2494_inst_list.asScala.toList.map(_.BLER)
          val NRResidualBLERVal = bean47240_65537.sr_2494_inst_list.asScala.toList.map(_.Residual_BLER)
          val crcPassTbSizeVal: List[String] = bean47240_65537.sr_2494_inst_list.asScala.toList.map(_.CRC_Pass_TB_Bytes)

          var TB_Size: lang.Long = 0L
          var carrierID: Integer = null
          var tbSizeSum: Long = 0
          var tbSizeSumPcc: Long = 0
          var tbSizeSumScc1: Long = 0
          var tbSizeSumScc2: Long = 0
          var tbSizeSumScc3: Long = 0
          var tbSizeSumScc4: Long = 0
          var tbSizeSumScc5: Long = 0
          var tbSizeSumScc6: Long = 0
          var tbSizeSumScc7: Long = 0

          if (crcPassTbSizeVal != null) {
            for (i <- 0 to crcPassTbSizeVal.size - 1) {
              TB_Size += crcPassTbSizeVal(i).toLong
              carrierID = i
              if (carrierID != null) {
                carrierID.toInt match {
                  case 0 =>
                    tbSizeSumPcc += crcPassTbSizeVal(i).toLong
                  case 1 =>
                    tbSizeSumScc1 += crcPassTbSizeVal(i).toLong
                  case 2 =>
                    tbSizeSumScc2 += crcPassTbSizeVal(i).toLong
                  case 3 =>
                    tbSizeSumScc3 += crcPassTbSizeVal(i).toLong
                  case 4 =>
                    tbSizeSumScc4 += crcPassTbSizeVal(i).toLong
                  case 5 =>
                    tbSizeSumScc5 += crcPassTbSizeVal(i).toLong
                  case 6 =>
                    tbSizeSumScc6 += crcPassTbSizeVal(i).toLong
                  case 7 =>
                    tbSizeSumScc7 += crcPassTbSizeVal(i).toLong
                  case _ =>
                }
              }
            }
          }

          /*
            if (crcPassTbSizeVal != null) {
              for (i <- 0 to crcPassTbSizeVal.size - 1) {
                TB_Size += crcPassTbSizeVal(i).toLong
              }
            }*/
          logRecordQComm5GObj = logRecordQComm5GObj.copy(
            NRResidualBLER = NRResidualBLERVal,
            NRDLBLER = NRDLBLERVal, TB_SizePcc_0xB888 = tbSizeSumPcc, TB_SizeScc1_0xB888 = tbSizeSumScc1, TB_SizeScc2_0xB888 = tbSizeSumScc2,
            TB_SizeScc3_0xB888 = tbSizeSumScc3, TB_SizeScc4_0xB888 = tbSizeSumScc4, TB_SizeScc5_0xB888 = tbSizeSumScc5, TB_SizeScc6_0xB888 = tbSizeSumScc6, TB_SizeScc7_0xB888 = tbSizeSumScc7,
            TB_Size_0xB888 = TB_Size, status0xB888 = true)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion,
            logCode = "47240",
            hexLogCode = "0xB888", logRecordQComm5G = logRecordQComm5GObj)
        case 65538 =>
          val bean47240_65538 = process47240_65538(x, exceptionUseArray)
          //logger.info("bean47240_65538 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47240_65538))
          val NRDLBLERVal = bean47240_65538.sr_2527_inst_list.asScala.toList.map(_.BLER)
          //val NRResidualBLERVal = bean47240_65538.sr_2527_inst_list.asScala.toList.map(_.Residual_BLER)

          val crcPassTbSizeValStr = bean47240_65538.sr_2527_inst_list.asScala.toList.map(_.CRC_Pass_TB_Bytes)
          //logger.info("crcPassTbSizeValStr: " + crcPassTbSizeValStr)
          val crcPassTbSizeVal: List[String] = bean47240_65538.sr_2527_inst_list.asScala.toList.map(_.CRC_Pass_TB_Bytes)


          var TB_Size: lang.Long = 0L
          var carrierID: Integer = null
          var tbSizeSum: Long = 0
          var tbSizeSumPcc: Long = 0
          var tbSizeSumScc1: Long = 0
          var tbSizeSumScc2: Long = 0
          var tbSizeSumScc3: Long = 0
          var tbSizeSumScc4: Long = 0
          var tbSizeSumScc5: Long = 0
          var tbSizeSumScc6: Long = 0
          var tbSizeSumScc7: Long = 0

          if (crcPassTbSizeVal != null) {
            for (i <- 0 to crcPassTbSizeVal.size - 1) {
              TB_Size += crcPassTbSizeVal(i).toLong
              carrierID = i
              if (carrierID != null) {
                carrierID.toInt match {
                  case 0 =>
                    tbSizeSumPcc += crcPassTbSizeVal(i).toLong
                  case 1 =>
                    tbSizeSumScc1 += crcPassTbSizeVal(i).toLong
                  case 2 =>
                    tbSizeSumScc2 += crcPassTbSizeVal(i).toLong
                  case 3 =>
                    tbSizeSumScc3 += crcPassTbSizeVal(i).toLong
                  case 4 =>
                    tbSizeSumScc4 += crcPassTbSizeVal(i).toLong
                  case 5 =>
                    tbSizeSumScc5 += crcPassTbSizeVal(i).toLong
                  case 6 =>
                    tbSizeSumScc6 += crcPassTbSizeVal(i).toLong
                  case 7 =>
                    tbSizeSumScc7 += crcPassTbSizeVal(i).toLong
                  case _ =>
                }
              }
            }
          }


          /* for (i <- 0 to crcPassTbSizeVal.size - 1) {
              TB_Size += crcPassTbSizeVal(i).toLong
              //println("crcPassTbSizeValStr: " + crcPassTbSizeVal(i))
            }*/
          //println("TB_Size: " + TB_Size)

          logRecordQComm5GObj = logRecordQComm5GObj.copy(
            NRDLBLER = NRDLBLERVal,
            TB_Size_0xB888 = TB_Size, TB_SizePcc_0xB888 = tbSizeSumPcc, TB_SizeScc1_0xB888 = tbSizeSumScc1, TB_SizeScc2_0xB888 = tbSizeSumScc2,
            TB_SizeScc3_0xB888 = tbSizeSumScc3, TB_SizeScc4_0xB888 = tbSizeSumScc4, TB_SizeScc5_0xB888 = tbSizeSumScc5, TB_SizeScc6_0xB888 = tbSizeSumScc6, TB_SizeScc7_0xB888 = tbSizeSumScc7, status0xB888 = true)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47240",
            hexLogCode = "0xB888", logRecordQComm5G = logRecordQComm5GObj)
        case 65539 =>
          val bean47240_65539 = process47240_65539(x, exceptionUseArray)
          val NRDLBLERVal = bean47240_65539.sr_2563_inst_list.asScala.toList.map(_.BLER)
          // logger.info("bean47240_65539 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47240_65539))
          val NRResidualBLERVal = bean47240_65539.sr_2563_inst_list.asScala.toList.map(_.Residual_BLER)
          val crcPassTbSizeVal: List[String] = bean47240_65539.sr_2563_inst_list.asScala.toList.map(_.CRC_Pass_TB_Bytes)

          var TB_Size: lang.Long = 0L
          var carrierID: Integer = null
          var tbSizeSum: Long = 0
          var tbSizeSumPcc: Long = 0
          var tbSizeSumScc1: Long = 0
          var tbSizeSumScc2: Long = 0
          var tbSizeSumScc3: Long = 0
          var tbSizeSumScc4: Long = 0
          var tbSizeSumScc5: Long = 0
          var tbSizeSumScc6: Long = 0
          var tbSizeSumScc7: Long = 0

          if (crcPassTbSizeVal != null) {
            for (i <- 0 to crcPassTbSizeVal.size - 1) {
              TB_Size += crcPassTbSizeVal(i).toLong
              carrierID = i
              if (carrierID != null) {
                carrierID.toInt match {
                  case 0 =>
                    tbSizeSumPcc += crcPassTbSizeVal(i).toLong
                  case 1 =>
                    tbSizeSumScc1 += crcPassTbSizeVal(i).toLong
                  case 2 =>
                    tbSizeSumScc2 += crcPassTbSizeVal(i).toLong
                  case 3 =>
                    tbSizeSumScc3 += crcPassTbSizeVal(i).toLong
                  case 4 =>
                    tbSizeSumScc4 += crcPassTbSizeVal(i).toLong
                  case 5 =>
                    tbSizeSumScc5 += crcPassTbSizeVal(i).toLong
                  case 6 =>
                    tbSizeSumScc6 += crcPassTbSizeVal(i).toLong
                  case 7 =>
                    tbSizeSumScc7 += crcPassTbSizeVal(i).toLong
                  case _ =>
                }
              }
            }
          }

          /* var TB_Size: java.lang.Long = 0L
            if (crcPassTbSizeVal != null) {
              for (i <- 0 to crcPassTbSizeVal.size - 1) {
                TB_Size += crcPassTbSizeVal(i).toLong
              }
            }*/
          logRecordQComm5GObj = logRecordQComm5GObj.copy(
            NRResidualBLER = NRResidualBLERVal,
            NRDLBLER = NRDLBLERVal,
            TB_Size_0xB888 = TB_Size, TB_SizePcc_0xB888 = tbSizeSumPcc, TB_SizeScc1_0xB888 = tbSizeSumScc1, TB_SizeScc2_0xB888 = tbSizeSumScc2,
            TB_SizeScc3_0xB888 = tbSizeSumScc3, TB_SizeScc4_0xB888 = tbSizeSumScc4, TB_SizeScc5_0xB888 = tbSizeSumScc5, TB_SizeScc6_0xB888 = tbSizeSumScc6, TB_SizeScc7_0xB888 = tbSizeSumScc7, status0xB888 = true)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion,
            logCode = "47240",
            hexLogCode = "0xB888", logRecordQComm5G = logRecordQComm5GObj)
        case 131072 =>
          val bean47240_131072 = process47240_131072(x, exceptionUseArray)
          //logger.info("bean47240_131072 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47240_131072))

          val NRDLBLERVal = bean47240_131072.sr_19845_inst_list.asScala.toList.map(_.BLER)
          //val NRResidualBLERVal = bean47240_131072.sr_19845_inst_list.asScala.toList.map(_.ResidualBLER)
          val NRResidualBLERVal = bean47240_131072.sr_19845_inst_list.asScala.toList.map(_.ResidualBler)
          val crcPassTbSizeVal: List[String] = bean47240_131072.sr_19845_inst_list.asScala.toList.map(_.CRCPassTBBytes)

          var carrierID: Integer = null
          var tbSizeSumPcc: Long = 0
          var tbSizeSumScc1: Long = 0
          var tbSizeSumScc2: Long = 0
          var tbSizeSumScc3: Long = 0
          var tbSizeSumScc4: Long = 0
          var tbSizeSumScc5: Long = 0
          var tbSizeSumScc6: Long = 0
          var tbSizeSumScc7: Long = 0

          /*var TB_Size: java.lang.Long = 0L
           if (crcPassTbSizeVal != null) {
             for (i <- 0 to crcPassTbSizeVal.size - 1) {
               TB_Size += crcPassTbSizeVal(i).toLong
             }
           }*/

          var TB_Size: lang.Long = 0L
          if (crcPassTbSizeVal != null) {
            for (i <- 0 to crcPassTbSizeVal.size - 1) {
              TB_Size += crcPassTbSizeVal(i).toLong
              carrierID = i
              if (carrierID != null) {
                carrierID.toInt match {
                  case 0 =>
                    tbSizeSumPcc += crcPassTbSizeVal(i).toLong
                  case 1 =>
                    tbSizeSumScc1 += crcPassTbSizeVal(i).toLong
                  case 2 =>
                    tbSizeSumScc2 += crcPassTbSizeVal(i).toLong
                  case 3 =>
                    tbSizeSumScc3 += crcPassTbSizeVal(i).toLong
                  case 4 =>
                    tbSizeSumScc4 += crcPassTbSizeVal(i).toLong
                  case 5 =>
                    tbSizeSumScc5 += crcPassTbSizeVal(i).toLong
                  case 6 =>
                    tbSizeSumScc6 += crcPassTbSizeVal(i).toLong
                  case 7 =>
                    tbSizeSumScc7 += crcPassTbSizeVal(i).toLong
                  case _ =>
                }
              }
            }
          }


          logRecordQComm5GObj = logRecordQComm5GObj.copy(
            NRResidualBLER = NRResidualBLERVal, TB_Size_0xB888 = TB_Size,
            NRDLBLER = NRDLBLERVal, TB_SizePcc_0xB888 = tbSizeSumPcc, TB_SizeScc1_0xB888 = tbSizeSumScc1, TB_SizeScc2_0xB888 = tbSizeSumScc2,
            TB_SizeScc3_0xB888 = tbSizeSumScc3, TB_SizeScc4_0xB888 = tbSizeSumScc4, TB_SizeScc5_0xB888 = tbSizeSumScc5, TB_SizeScc6_0xB888 = tbSizeSumScc6, TB_SizeScc7_0xB888 = tbSizeSumScc7,
            status0xB888 = true)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion,
            logCode = "47240",
            hexLogCode = "0xB888", logRecordQComm5G = logRecordQComm5GObj)
        case 131074 =>
          val bean47240_131074 = process47240_131074(x, exceptionUseArray)
          val NRDLBLERVal = bean47240_131074.sr_27379_inst_list.asScala.toList.map(_.BLER)
          val NRResidualBLERVal = bean47240_131074.sr_27379_inst_list.asScala.toList.map(_.ResidualBLER)
          val crcPassTbSizeVal: List[String] = bean47240_131074.sr_27379_inst_list.asScala.toList.map(_.CRCPassTBBytes)
          val carrierIdVal: List[String] = bean47240_131074.sr_27379_inst_list.asScala.toList.map(_.CarrierID)

          var carrierID: Integer = null
          var tbSizeSumPcc: Long = 0
          var tbSizeSumScc1: Long = 0
          var tbSizeSumScc2: Long = 0
          var tbSizeSumScc3: Long = 0
          var tbSizeSumScc4: Long = 0
          var tbSizeSumScc5: Long = 0
          var tbSizeSumScc6: Long = 0
          var tbSizeSumScc7: Long = 0

          var TB_Size: lang.Long = 0L
          if (carrierIdVal != null && crcPassTbSizeVal != null) {
            for (i <- 0 to carrierIdVal.size - 1) {
              TB_Size += crcPassTbSizeVal(i).toLong
              carrierID = carrierIdVal(i).toInt
              if (carrierID != null) {
                carrierID.toInt match {
                  case 0 =>
                    tbSizeSumPcc += crcPassTbSizeVal(i).toLong
                  case 1 =>
                    tbSizeSumScc1 += crcPassTbSizeVal(i).toLong
                  case 2 =>
                    tbSizeSumScc2 += crcPassTbSizeVal(i).toLong
                  case 3 =>
                    tbSizeSumScc3 += crcPassTbSizeVal(i).toLong
                  case 4 =>
                    tbSizeSumScc4 += crcPassTbSizeVal(i).toLong
                  case 5 =>
                    tbSizeSumScc5 += crcPassTbSizeVal(i).toLong
                  case 6 =>
                    tbSizeSumScc6 += crcPassTbSizeVal(i).toLong
                  case 7 =>
                    tbSizeSumScc7 += crcPassTbSizeVal(i).toLong
                  case _ =>
                }
              }
            }
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(
            NRResidualBLER = NRResidualBLERVal,
            NRDLBLER = NRDLBLERVal,
            TB_Size_0xB888 = TB_Size, TB_SizePcc_0xB888 = tbSizeSumPcc, TB_SizeScc1_0xB888 = tbSizeSumScc1, TB_SizeScc2_0xB888 = tbSizeSumScc2,
            TB_SizeScc3_0xB888 = tbSizeSumScc3, TB_SizeScc4_0xB888 = tbSizeSumScc4, TB_SizeScc5_0xB888 = tbSizeSumScc5, TB_SizeScc6_0xB888 = tbSizeSumScc6, TB_SizeScc7_0xB888 = tbSizeSumScc7, status0xB888 = true)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47240", hexLogCode = "0xB888", logRecordQComm5G = logRecordQComm5GObj)
        case 196608 =>
          val bean47240_196608 = process47240_196608(x, exceptionUseArray)
          val NRDLBLERVal = bean47240_196608.sr_38725_inst_list.asScala.toList.map(_.BLER)
          //           val NRResidualBLERVal = bean47240_196608.sr_38725_inst_list.asScala.toList.map(_.ResidualBLER) TODO
          val crcPassTbSizeVal: List[String] = bean47240_196608.sr_38725_inst_list.asScala.toList.map(_.CRCPassTBBytes)
          val carrierIdVal: List[String] = bean47240_196608.sr_38725_inst_list.asScala.toList.map(_.CarrierID)

          var carrierID: Integer = null
          var tbSizeSumPcc: Long = 0
          var tbSizeSumScc1: Long = 0
          var tbSizeSumScc2: Long = 0
          var tbSizeSumScc3: Long = 0
          var tbSizeSumScc4: Long = 0
          var tbSizeSumScc5: Long = 0
          var tbSizeSumScc6: Long = 0
          var tbSizeSumScc7: Long = 0

          var TB_Size: lang.Long = 0L
          if (carrierIdVal != null && crcPassTbSizeVal != null) {
            for (i <- 0 to carrierIdVal.size - 1) {
              TB_Size += crcPassTbSizeVal(i).toLong
              carrierID = carrierIdVal(i).toInt
              if (carrierID != null) {
                carrierID.toInt match {
                  case 0 =>
                    tbSizeSumPcc += crcPassTbSizeVal(i).toLong
                  case 1 =>
                    tbSizeSumScc1 += crcPassTbSizeVal(i).toLong
                  case 2 =>
                    tbSizeSumScc2 += crcPassTbSizeVal(i).toLong
                  case 3 =>
                    tbSizeSumScc3 += crcPassTbSizeVal(i).toLong
                  case 4 =>
                    tbSizeSumScc4 += crcPassTbSizeVal(i).toLong
                  case 5 =>
                    tbSizeSumScc5 += crcPassTbSizeVal(i).toLong
                  case 6 =>
                    tbSizeSumScc6 += crcPassTbSizeVal(i).toLong
                  case 7 =>
                    tbSizeSumScc7 += crcPassTbSizeVal(i).toLong
                  case _ =>
                }
              }
            }
          }
          //          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRResidualBLER = NRResidualBLERVal, NRDLBLER = NRDLBLERVal,
          //            TB_Size_0xB888 = TB_Size, TB_SizePcc_0xB888 = tbSizeSumPcc, TB_SizeScc1_0xB888 = tbSizeSumScc1, TB_SizeScc2_0xB888 = tbSizeSumScc2,
          //            TB_SizeScc3_0xB888 = tbSizeSumScc3, TB_SizeScc4_0xB888 = tbSizeSumScc4, TB_SizeScc5_0xB888 = tbSizeSumScc5, TB_SizeScc6_0xB888 = tbSizeSumScc6, TB_SizeScc7_0xB888 = tbSizeSumScc7, status0xB888 = true)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47240", hexLogCode = "0xB888", logRecordQComm5G = logRecordQComm5GObj)
        case _ =>
          //logger.info("Missing Version for logcode:47240, hexlogCode:0xB888, Version:" + logVersion)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47240", hexLogCode = "0xB888", missingVersion = logVersion)
      }
    }

    def parse47241 = {
      logVersion match {
        case 65536 =>
          val bean47241_65536 = process47241_65536(x, exceptionUseArray)
          val CRNTIVal = bean47241_65536.CRNTI
          val RachReasonVal = bean47241_65536.Rach_Reason
          val CarrierIdVal = bean47241_65536.Carrier_Id
          val RACHContentionVal = bean47241_65536.RACH_Contention
          val rachTrigger0xb889 = bean47241_65536.getLogRecordDescription
          val rachTriggered = rachTrigger0xb889.contains("Trigger")
          val NRBeamFailureEvtVal: Integer = if (!RachReasonVal.isEmpty && RachReasonVal.equalsIgnoreCase("BEAM_FAILURE")) 1 else null
          logRecordQComm5GObj = logRecordQComm5GObj.copy(CRNTI_0xB889 = CRNTIVal, RachReason = RachReasonVal,
            CarrierId_0xB889 = CarrierIdVal, RACHContention = RACHContentionVal, rachTriggered_0xb889 = rachTriggered, NRBeamFailureEvt = NRBeamFailureEvtVal)

          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47241", hexLogCode = "0xB889", logRecordQComm5G = logRecordQComm5GObj)
        case 131073 =>
          val bean47241_131073 = process47241_131073(x, exceptionUseArray)
          val CRNTIVal = bean47241_131073.CRNTI
          val RachReasonVal = bean47241_131073.RachReason
          val CarrierIdVal = bean47241_131073.CarrierId
          val RACHContentionVal = bean47241_131073.RACHContention

          val rachTrigger0xb889 = bean47241_131073.getLogRecordDescription
          val rachTriggered = rachTrigger0xb889.contains("Trigger")
          val NRBeamFailureEvtVal: Integer = if (!RachReasonVal.isEmpty && RachReasonVal.equalsIgnoreCase("BEAM_FAILURE")) 1 else null
          logRecordQComm5GObj = logRecordQComm5GObj.copy(CRNTI_0xB889 = CRNTIVal, RachReason = RachReasonVal,
            CarrierId_0xB889 = CarrierIdVal, RACHContention = RACHContentionVal, rachTriggered_0xb889 = rachTriggered, NRBeamFailureEvt = NRBeamFailureEvtVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47241", hexLogCode = "0xB889", logRecordQComm5G = logRecordQComm5GObj)
        case 131074 =>
          val bean47241_131074 = process47241_131074(x, exceptionUseArray)
          val CRNTIVal = bean47241_131074.sr_59220_inst_list.asScala.toList.map(_.CRNTI)
          val RachReasonVal = bean47241_131074.sr_59220_inst_list.asScala.toList.map(_.RachReason)
          val CarrierIdVal = bean47241_131074.sr_59220_inst_list.asScala.toList.map(_.CAID)
          val RACHContentionVal = bean47241_131074.sr_59220_inst_list.asScala.toList.map(_.RACHContention)

          val rachTrigger0xb889 = bean47241_131074.getLogRecordDescription
          val rachTriggered = rachTrigger0xb889.contains("Trigger")
          val NRBeamFailureEvtVal: Integer = if (!RachReasonVal.isEmpty && RachReasonVal.head.equalsIgnoreCase("BEAM_FAILURE")) 1 else null
          logRecordQComm5GObj = logRecordQComm5GObj.copy(CRNTI_0xB889 = if (CRNTIVal.nonEmpty) CRNTIVal.head else null, RachReason = if (RachReasonVal.nonEmpty) RachReasonVal.head else null,
            CarrierId_0xB889 = if (CarrierIdVal.nonEmpty) CarrierIdVal.head else null, RACHContention = if (RACHContentionVal.nonEmpty) RACHContentionVal.head else null, rachTriggered_0xb889 = rachTriggered, NRBeamFailureEvt = NRBeamFailureEvtVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47241", hexLogCode = "0xB889", logRecordQComm5G = logRecordQComm5GObj)
        case 196613 =>
          val bean47241_196613 = process47241_196613(x, exceptionUseArray)
          val CRNTIVal = bean47241_196613.sr_58380_inst_list.asScala.toList.map(_.CRNTI)
          val RachReasonVal = bean47241_196613.sr_58380_inst_list.asScala.toList.map(_.RachReason)
          val CarrierIdVal = bean47241_196613.sr_58380_inst_list.asScala.toList.map(_.CAID)
          val RACHContentionVal = bean47241_196613.sr_58380_inst_list.asScala.toList.map(_.RACHContention)

          val rachTrigger0xb889 = bean47241_196613.LogRecordDescription
          val rachTriggered = rachTrigger0xb889.contains("Trigger")
          val NRBeamFailureEvtVal: Integer = if (RachReasonVal.nonEmpty && RachReasonVal.head.equalsIgnoreCase("BEAM_FAILURE")) 1 else null
          logRecordQComm5GObj = logRecordQComm5GObj.copy(CRNTI_0xB889 = if (CRNTIVal.nonEmpty) CRNTIVal.head else null, RachReason = if (RachReasonVal.nonEmpty) RachReasonVal.head else null,
            CarrierId_0xB889 = if (CarrierIdVal.nonEmpty) CarrierIdVal.head else null, RACHContention = if (RACHContentionVal.nonEmpty) RACHContentionVal.head else null, rachTriggered_0xb889 = rachTriggered, NRBeamFailureEvt = NRBeamFailureEvtVal)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47241", hexLogCode = "0xB889", logRecordQComm5G = logRecordQComm5GObj)
        case _ =>
          //logger.info("Missing Version for logcode:47241, hexlogCode:0xB889, Version:" + logVersion)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47241", hexLogCode = "0xB889", missingVersion = logVersion)
      }
    }

    def parse47314 = {
      logVersion match {
        case 1 =>
          val bean47314_1 = process47314_1(x, exceptionUseArray)
          val Code = bean47314_1.sr_33725_inst_list.asScala.toList.map(_.Code).filter(_.equals("POWER INFO"))
          val Version = bean47314_1.sr_33725_inst_list.asScala.toList.filter(_.Code.equals("POWER INFO")).map(_.sr_33652_inst_list.asScala.toList.map(_.sr_33639_inst_list.asScala.toList.map(_.Version))).flatten.flatten
          val bean47314_1_33639 = bean47314_1.sr_33725_inst_list.asScala.toList.filter(_.Code.equals("POWER INFO")).map(_.sr_33652_inst_list.asScala.toList.map(_.sr_33639_inst_list.asScala.toList)).flatten.flatten
          var PUSCHTxPwrVal: List[String] = null
          var PUSCHDLPathLossVal: List[String] = null
          var PRACDLPathLossVal: List[String] = null
          var SRSDLPathLossVal: List[String] = null
          var PUCCHDLPathLossVal: List[String] = null

          if (Code.nonEmpty && Version.nonEmpty && bean47314_1_33639.size > 0)
            Version.head match {
              case "0" =>
                val ChannelType = bean47314_1_33639.map(_.sr_33774_inst_list.asScala.toList.map(_.sr_33782_inst_list.asScala.toList.map(_.ChannelType))).flatten.flatten
                PUSCHTxPwrVal = bean47314_1_33639.map(_.sr_33774_inst_list.asScala.toList.map(_.sr_33782_inst_list.asScala.toList.map(_.sr_33748_inst_list.asScala.toList.map(_.TransmitPower)))).flatten.flatten.flatten
                PUSCHDLPathLossVal = bean47314_1_33639.map(_.sr_33774_inst_list.asScala.toList.map(_.sr_33782_inst_list.asScala.toList.map(_.sr_33748_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten
                PRACDLPathLossVal = bean47314_1_33639.map(_.sr_33774_inst_list.asScala.toList.map(_.sr_33782_inst_list.asScala.toList.map(_.sr_33767_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten
                SRSDLPathLossVal = bean47314_1_33639.map(_.sr_33774_inst_list.asScala.toList.map(_.sr_33782_inst_list.asScala.toList.map(_.sr_33640_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten
                PUCCHDLPathLossVal = bean47314_1_33639.map(_.sr_33774_inst_list.asScala.toList.map(_.sr_33782_inst_list.asScala.toList.map(_.sr_33761_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten

              case "1" =>
                val ChannelType = bean47314_1_33639.map(_.sr_33775_inst_list.asScala.toList.map(_.sr_33780_inst_list.asScala.toList.map(_.ChannelType))).flatten.flatten
                PUSCHTxPwrVal = bean47314_1_33639.map(_.sr_33775_inst_list.asScala.toList.map(_.sr_33780_inst_list.asScala.toList.map(_.sr_33655_inst_list.asScala.toList.map(_.TransmitPower)))).flatten.flatten.flatten
                PUSCHDLPathLossVal = bean47314_1_33639.map(_.sr_33775_inst_list.asScala.toList.map(_.sr_33780_inst_list.asScala.toList.map(_.sr_33655_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten
                PRACDLPathLossVal = bean47314_1_33639.map(_.sr_33775_inst_list.asScala.toList.map(_.sr_33780_inst_list.asScala.toList.map(_.sr_33658_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten
                SRSDLPathLossVal = bean47314_1_33639.map(_.sr_33775_inst_list.asScala.toList.map(_.sr_33780_inst_list.asScala.toList.map(_.sr_33657_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten
                PUCCHDLPathLossVal = bean47314_1_33639.map(_.sr_33775_inst_list.asScala.toList.map(_.sr_33780_inst_list.asScala.toList.map(_.sr_33656_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten

              case "2" =>
                val ChannelType = bean47314_1_33639.map(_.sr_33598_inst_list.asScala.toList.map(_.sr_33600_inst_list.asScala.toList.map(_.ChannelType))).flatten.flatten
                PUSCHTxPwrVal = bean47314_1_33639.map(_.sr_33598_inst_list.asScala.toList.map(_.sr_33600_inst_list.asScala.toList.map(_.sr_33603_inst_list.asScala.toList.map(_.TransmitPower)))).flatten.flatten.flatten
                PUSCHDLPathLossVal = bean47314_1_33639.map(_.sr_33598_inst_list.asScala.toList.map(_.sr_33600_inst_list.asScala.toList.map(_.sr_33603_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten
                PRACDLPathLossVal = bean47314_1_33639.map(_.sr_33598_inst_list.asScala.toList.map(_.sr_33600_inst_list.asScala.toList.map(_.getSr_33606_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten
                SRSDLPathLossVal = bean47314_1_33639.map(_.sr_33598_inst_list.asScala.toList.map(_.sr_33600_inst_list.asScala.toList.map(_.sr_33605_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten
                PUCCHDLPathLossVal = bean47314_1_33639.map(_.sr_33598_inst_list.asScala.toList.map(_.sr_33600_inst_list.asScala.toList.map(_.sr_33604_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten

              case "3" =>
                val ChannelType = bean47314_1_33639.map(_.sr_33799_inst_list.asScala.toList.map(_.sr_33788_inst_list.asScala.toList.map(_.ChannelType))).flatten.flatten
                PUSCHTxPwrVal = bean47314_1_33639.map(_.sr_33799_inst_list.asScala.toList.map(_.sr_33788_inst_list.asScala.toList.map(_.sr_33802_inst_list.asScala.toList.map(_.TransmitPower)))).flatten.flatten.flatten
                PUSCHDLPathLossVal = bean47314_1_33639.map(_.sr_33799_inst_list.asScala.toList.map(_.sr_33788_inst_list.asScala.toList.map(_.sr_33802_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten
                PRACDLPathLossVal = bean47314_1_33639.map(_.sr_33799_inst_list.asScala.toList.map(_.sr_33788_inst_list.asScala.toList.map(_.sr_33805_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten
                SRSDLPathLossVal = bean47314_1_33639.map(_.sr_33799_inst_list.asScala.toList.map(_.sr_33788_inst_list.asScala.toList.map(_.sr_33804_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten
                PUCCHDLPathLossVal = bean47314_1_33639.map(_.sr_33799_inst_list.asScala.toList.map(_.sr_33788_inst_list.asScala.toList.map(_.sr_33803_inst_list.asScala.toList.map(_.Pathloss)))).flatten.flatten.flatten

              case _ =>
                logger.info("Unknown 47314=========>" + logVersion)

            }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(PUSCHDLPathLoss_0xB8D2 = Try(PUSCHDLPathLossVal.head).getOrElse(null), PUCCHDLPathLoss_0xB8D2 = Try(PUCCHDLPathLossVal.head).getOrElse(null),
            SRSDLPathLoss_0xB8D2 = Try(SRSDLPathLossVal.head).getOrElse(null), PRACPathLoss_0xB8D2 = Try(PRACDLPathLossVal.head).getOrElse(null), PUSCHTxPwr_0xB8D2 = Try(PUSCHTxPwrVal.head).getOrElse(null))
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47314", hexLogCode = "0xB8D2", logRecordQComm5G = logRecordQComm5GObj)
        case 65536 =>
          val bean47314_65536 = process47314_65536(x, exceptionUseArray)
          val Code = bean47314_65536.sr_42644_inst_list.asScala.toList.map(_.Code).filter(_.equals("POWER INFO"))
          val Version = bean47314_65536.sr_42644_inst_list.asScala.toList.filter(_.Code.equals("POWER INFO")).map(_.sr_42658_inst_list.asScala.toList.map(_.Version)).flatten
          val bean47314_65536_43298 = bean47314_65536.sr_42644_inst_list.asScala.toList.filter(_.Code.equals("POWER INFO")).map(_.sr_42658_inst_list.asScala.toList.map(_.sr_43298_inst_list.asScala.toList)).flatten.flatten
          var PUSCHTxPwrVal: List[String] = null
          var PUSCHDLPathLossVal: List[String] = null
          var PRACDLPathLossVal: List[String] = null
          var SRSDLPathLossVal: List[String] = null
          var PUCCHDLPathLossVal: List[String] = null

          if (Code.nonEmpty && Version.nonEmpty && bean47314_65536_43298.size > 0) {
            PUSCHTxPwrVal = bean47314_65536_43298.map(_.sr_42692_inst_list.asScala.toList.map(_.sr_42696_inst_list.asScala.toList.map(_.TransmitPower))).flatten.flatten
            PUSCHDLPathLossVal = bean47314_65536_43298.map(_.sr_42692_inst_list.asScala.toList.map(_.sr_42696_inst_list.asScala.toList.map(_.Pathloss))).flatten.flatten
            PRACDLPathLossVal = bean47314_65536_43298.map(_.sr_42692_inst_list.asScala.toList.map(_.sr_42728_inst_list.asScala.toList.map(_.Pathloss))).flatten.flatten
            SRSDLPathLossVal = bean47314_65536_43298.map(_.sr_42692_inst_list.asScala.toList.map(_.sr_42698_inst_list.asScala.toList.map(_.Pathloss))).flatten.flatten
            PUCCHDLPathLossVal = bean47314_65536_43298.map(_.sr_42692_inst_list.asScala.toList.map(_.sr_42697_inst_list.asScala.toList.map(_.Pathloss))).flatten.flatten
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(PUSCHDLPathLoss_0xB8D2 = Try(PUSCHDLPathLossVal.head).getOrElse(null), PUCCHDLPathLoss_0xB8D2 = Try(PUCCHDLPathLossVal.head).getOrElse(null),
            SRSDLPathLoss_0xB8D2 = Try(SRSDLPathLossVal.head).getOrElse(null), PRACPathLoss_0xB8D2 = Try(PRACDLPathLossVal.head).getOrElse(null), PUSCHTxPwr_0xB8D2 = Try(PUSCHTxPwrVal.head).getOrElse(null))
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47314", hexLogCode = "0xB8D2", logRecordQComm5G = logRecordQComm5GObj)

        case _ =>
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47314", hexLogCode = "0xB8D2", missingVersion = logVersion)
      }
    }

    def parse47320 = {
      logVersion match {
        case 131073 =>
          val bean47320_131073 = process47320_131073(x, exceptionUseArray)
          val snrList = if (bean47320_131073.getReferenceSignal.equalsIgnoreCase("SSB") && !bean47320_131073.getCarrierIndex.equals("0")) bean47320_131073.sr_57352_inst_list.asScala.toList.map(_.SNR).map(s => parseFloat(s)) else List()
          val NRSNR_0xB8D8Val: List[String] = if (snrList != null && snrList.nonEmpty) List(bean47320_131073.getCarrierIndex, snrList.max.toString) else List.empty

          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSNR_0xB8D8 = NRSNR_0xB8D8Val)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47320", hexLogCode = "0xB8D8", logRecordQComm5G = logRecordQComm5GObj)
        case 196609 =>
          val bean47320_196609 = process47320_196609(x, exceptionUseArray)
          val snrList = if (bean47320_196609.getReferenceSignal.equalsIgnoreCase("SSB") && !bean47320_196609.getCarrierIndex.equals("0")) bean47320_196609.sr_58671_inst_list.asScala.toList.map(_.SNR).map(s => parseFloat(s)) else List()
          val NRSNR_0xB8D8Val: List[String] = if (snrList != null && snrList.nonEmpty) List(bean47320_196609.getCarrierIndex, snrList.max.toString) else List.empty

          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSNR_0xB8D8 = NRSNR_0xB8D8Val)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47320", hexLogCode = "0xB8D8", logRecordQComm5G = logRecordQComm5GObj)

        case _ =>
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47320", hexLogCode = "0xB8D8", missingVersion = logVersion)
      }
    }

    def parse47325 = {
      logVersion match {
        case 7 =>
          val bean47325_7 = process47325_7(x, exceptionUseArray)
          //logger.info("bean47325_7 -> Bean as JSON>>>>>>>>" + Constants.getJsonFromBean(bean47325_7))
          val ftlSnrList = if (bean47325_7.CarrierIndex.equals("0")) bean47325_7.sr_32027_inst_list.asScala.toList.map(_.FTLSNR).map(s => parseDouble(s)) else List()

          val NRSBSNR_0xB8DDVal: java.lang.Double = if (ftlSnrList != null && ftlSnrList.nonEmpty) ftlSnrList.max else null
          val NRSBSNRRx0_0xB8DDVal: java.lang.Double = if (ftlSnrList != null && ftlSnrList.size > 0) ftlSnrList(0) else null
          val NRSBSNRRx1_0xB8DDVal: java.lang.Double = if (ftlSnrList != null && ftlSnrList.size > 1) ftlSnrList(1) else null
          val NRSBSNRRx2_0xB8DDVal: java.lang.Double = if (ftlSnrList != null && ftlSnrList.size > 2) ftlSnrList(2) else null
          val NRSBSNRRx3_0xB8DDVal: java.lang.Double = if (ftlSnrList != null && ftlSnrList.size > 3) ftlSnrList(3) else null

          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSBSNR_0xB8DD = NRSBSNR_0xB8DDVal, NRSBSNRRx0_0xB8DD = NRSBSNRRx0_0xB8DDVal,
            NRSBSNRRx1_0xB8DD = NRSBSNRRx1_0xB8DDVal, NRSBSNRRx2_0xB8DD = NRSBSNRRx2_0xB8DDVal, NRSBSNRRx3_0xB8DD = NRSBSNRRx3_0xB8DDVal)
          //logger.info(s"NRSBSNR_0xB8DD = $NRSBSNR_0xB8DDVal, NRSBSNRRx0_0xB8DD = $NRSBSNRRx0_0xB8DDVal, NRSBSNRRx1_0xB8DD = $NRSBSNRRx1_0xB8DDVal, NRSBSNRRx2_0xB8DD = $NRSBSNRRx2_0xB8DDVal, NRSBSNRRx3_0xB8DD = $NRSBSNRRx3_0xB8DDVal")
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47325", hexLogCode = "0xB8DD", logRecordQComm5G = logRecordQComm5GObj)

        case 131080 =>
          val bean47325_131080 = process47325_131080(x, exceptionUseArray)
          //logger.info("bean47325_131080 -> Bean as JSON>>>>>>>>" + Constants.getJsonFromBean(bean47325_131080))
          val ftlSnrList = if (bean47325_131080.CarrierIndex.equals("0")) bean47325_131080.sr_57365_inst_list.asScala.toList.map(_.FTLSNR).map(s => parseDouble(s)) else List()

          val NRSBSNR_0xB8DDVal: java.lang.Double = if (ftlSnrList != null && ftlSnrList.nonEmpty) ftlSnrList.max else null
          val NRSBSNRRx0_0xB8DDVal: java.lang.Double = if (ftlSnrList != null && ftlSnrList.size > 0) ftlSnrList(0) else null
          val NRSBSNRRx1_0xB8DDVal: java.lang.Double = if (ftlSnrList != null && ftlSnrList.size > 1) ftlSnrList(1) else null
          val NRSBSNRRx2_0xB8DDVal: java.lang.Double = if (ftlSnrList != null && ftlSnrList.size > 2) ftlSnrList(2) else null
          val NRSBSNRRx3_0xB8DDVal: java.lang.Double = if (ftlSnrList != null && ftlSnrList.size > 3) ftlSnrList(3) else null

          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSBSNR_0xB8DD = NRSBSNR_0xB8DDVal, NRSBSNRRx0_0xB8DD = NRSBSNRRx0_0xB8DDVal,
            NRSBSNRRx1_0xB8DD = NRSBSNRRx1_0xB8DDVal, NRSBSNRRx2_0xB8DD = NRSBSNRRx2_0xB8DDVal, NRSBSNRRx3_0xB8DD = NRSBSNRRx3_0xB8DDVal)
          //logger.info(s"NRSBSNR_0xB8DD = $NRSBSNR_0xB8DDVal, NRSBSNRRx0_0xB8DD = $NRSBSNRRx0_0xB8DDVal, NRSBSNRRx1_0xB8DD = $NRSBSNRRx1_0xB8DDVal, NRSBSNRRx2_0xB8DD = $NRSBSNRRx2_0xB8DDVal, NRSBSNRRx3_0xB8DD = $NRSBSNRRx3_0xB8DDVal")
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47325", hexLogCode = "0xB8DD", logRecordQComm5G = logRecordQComm5GObj)
        case _ =>
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47325", hexLogCode = "0xB8DD", missingVersion = logVersion)

      }
    }

    def parse47242 = {
      logVersion match {
        case 131075 =>
          val bean47242_131075 = process47242_131075(x, exceptionUseArray)
          val rachResult = bean47242_131075.RACHResult
          val rachAttempt0xb88a = bean47242_131075.getLogRecordDescription
          val rachAttempted = rachAttempt0xb88a.contains("Attempt")
          val rachFailure: Int = if (rachResult.toLowerCase.contains("failure")) 1 else 0
          logRecordQComm5GObj = logRecordQComm5GObj.copy(rachResult_0xb88a = rachResult, RachFailure_0xb88a = rachFailure, rachAttempted_0xb88a = rachAttempted)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47242", hexLogCode = "0xB88A", logRecordQComm5G = logRecordQComm5GObj)

        case 131078 =>
          val bean47242_131078 = process47242_131078(x, exceptionUseArray)
          val rachResult = bean47242_131078.RACHResult
          val rachAttempt0xb88a = bean47242_131078.getLogRecordDescription
          val rachAttempted = rachAttempt0xb88a.contains("Attempt")
          val rachFailure: Int = if (rachResult.toLowerCase.contains("failure")) 1 else 0
          logRecordQComm5GObj = logRecordQComm5GObj.copy(rachResult_0xb88a = rachResult, RachFailure_0xb88a = rachFailure, rachAttempted_0xb88a = rachAttempted)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47242", hexLogCode = "0xB88A", logRecordQComm5G = logRecordQComm5GObj)
        case 131079 =>
          val bean47242_131079 = process47242_131079(x, exceptionUseArray)
          val rachResult = bean47242_131079.RACHResult
          val rachAttempt0xb88a = bean47242_131079.getLogRecordDescription
          val rachAttempted = rachAttempt0xb88a.contains("Attempt")
          val rachFailure: Int = if (rachResult.toLowerCase.contains("failure")) 1 else 0
          logRecordQComm5GObj = logRecordQComm5GObj.copy(rachResult_0xb88a = rachResult, RachFailure_0xb88a = rachFailure, rachAttempted_0xb88a = rachAttempted)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47242", hexLogCode = "0xB88A", logRecordQComm5G = logRecordQComm5GObj)
        case 131081 =>
          val bean47242_131081 = process47242_131081(x, exceptionUseArray)
          val rachResult = bean47242_131081.sr_49043_inst_list.asScala.toList.map(_.RACHResult).head
          val rachAttempt0xb88a = bean47242_131081.getLogRecordDescription
          val rachAttempted = rachAttempt0xb88a.contains("Attempt")
          val rachFailure: Int = if (rachResult.toLowerCase.contains("failure")) 1 else 0
          logRecordQComm5GObj = logRecordQComm5GObj.copy(rachResult_0xb88a = rachResult, RachFailure_0xb88a = rachFailure, rachAttempted_0xb88a = rachAttempted)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47242", hexLogCode = "0xB88A", logRecordQComm5G = logRecordQComm5GObj)
        case 131083 =>
          val bean47242_131083 = process47242_131083(x, exceptionUseArray)
          val rachResult = bean47242_131083.sr_59246_inst_list.asScala.toList.map(_.RACHResult).head
          val rachAttempt0xb88a = bean47242_131083.getLogRecordDescription
          val rachAttempted = rachAttempt0xb88a.contains("Attempt")
          val rachFailure: Int = if (rachResult.toLowerCase.contains("failure")) 1 else 0
          logRecordQComm5GObj = logRecordQComm5GObj.copy(rachResult_0xb88a = rachResult, RachFailure_0xb88a = rachFailure, rachAttempted_0xb88a = rachAttempted)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47242", hexLogCode = "0xB88A", logRecordQComm5G = logRecordQComm5GObj)
        case 196613 =>
          val bean47242_196613 = process47242_196613(x, exceptionUseArray)
          val rachResult = bean47242_196613.sr_59039_inst_list.asScala.toList.map(_.RACHResult).head
          val rachAttempt0xb88a = bean47242_196613.getLogRecordDescription
          val rachAttempted = rachAttempt0xb88a.contains("Attempt")
          val rachFailure: Int = if (rachResult.toLowerCase.contains("failure")) 1 else 0
          logRecordQComm5GObj = logRecordQComm5GObj.copy(rachResult_0xb88a = rachResult, RachFailure_0xb88a = rachFailure, rachAttempted_0xb88a = rachAttempted)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47242", hexLogCode = "0xB88A", logRecordQComm5G = logRecordQComm5GObj)
        case 7 | 65536 =>
          val bean47242_65536 = process47242_65536(x, exceptionUseArray)
          val rachResult = bean47242_65536.RACH_Result
          val rachAttempt0xb88a = bean47242_65536.getLogRecordDescription
          val rachAttempted = rachAttempt0xb88a.contains("Attempt")
          val rachFailure: Int = if (rachResult.toLowerCase.contains("failure")) 1 else 0
          logRecordQComm5GObj = logRecordQComm5GObj.copy(rachResult_0xb88a = rachResult, RachFailure_0xb88a = rachFailure, rachAttempted_0xb88a = rachAttempted)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47242", hexLogCode = "0xB88A", logRecordQComm5G = logRecordQComm5GObj)

        case 65537 =>
          val bean47242_65537 = process47242_65537(x, exceptionUseArray)
          val rachResult = bean47242_65537.RACH_Result
          val rachAttempt0xb88a = bean47242_65537.getLogRecordDescription
          val rachAttempted = rachAttempt0xb88a.contains("Attempt")
          val rachFailure: Int = if (rachResult.toLowerCase.contains("failure")) 1 else 0
          logRecordQComm5GObj = logRecordQComm5GObj.copy(rachResult_0xb88a = rachResult, RachFailure_0xb88a = rachFailure, rachAttempted_0xb88a = rachAttempted)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47242", hexLogCode = "0xB88A", logRecordQComm5G = logRecordQComm5GObj)

        case _ =>
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47242", hexLogCode = "0xB88A", missingVersion = logVersion)
      }
    }

    def parse47525 = {
      logVersion match {
        case 131073 =>
          val bean47525_131073 = process47525_131073(x, exceptionUseArray)
          val bfdind = bean47525_131073.getBFDIND
          logRecordQComm5GObj = logRecordQComm5GObj.copy(BFDIND = if (bfdind.equals("1")) true else false)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47525", hexLogCode = "0xB9A5", logRecordQComm5G = logRecordQComm5GObj)
        case _ =>
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47525", hexLogCode = "0xB9A5", missingVersion = logVersion)
      }
    }

    logCode match {
      case 47236 =>
        parse47236
      case 47240 =>
        parse47240
      case 47241 =>
        parse47241
      case 47314 =>
        parse47314
      case 47320 =>
        parse47320
      case 47325 =>
        parse47325
      case 47242 =>
        parse47242
      case 47525 =>
        parse47525
    }

    logRecord
  }

  def parseLogRecordQCOMM5GSet3(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQComm5GObj: LogRecordQComm5G = LogRecordQComm5G()

    def parse65540 = {
      val bean47239_65540 = process47239_65540(x, exceptionUseArray)
      val NRDLMCSVal = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2317_inst_list.asScala.toList.map(_.MCS)).flatten
      val NRDLModTypeVal = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2317_inst_list.asScala.toList.map(_.Mod_Type)).flatten
      val NumRecordsVal = bean47239_65540.Num_Records
      val SystemTimeVal = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2314_inst_list.asScala.toList.map(_.System_Time)).flatten
      val FrameVal = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2314_inst_list.asScala.toList.map(_.Frame)).flatten
      val NumPDSCHStatusVal = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.Num_PDSCH_Status)
      val PDSCHStatusInfoVal = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2317_inst_list.asScala.toList.map(_.PDSCH_Status_Info)).flatten
      val CarrierID_0xB887Val = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2317_inst_list.asScala.toList.map(_.Carrier_ID)).flatten
      val CRCstatus_0xB887Val = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2317_inst_list.asScala.toList.map(_.CRC_status)).flatten
      val CRCstate_0xB887Val = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2317_inst_list.asScala.toList.map(_.CRC_State)).flatten
      val TBSize_0xB887Val = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2317_inst_list.asScala.toList.map(_.TB_Size)).flatten
      val CellId_0xB887Val = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2317_inst_list.asScala.toList.map(_.Cell_Id)).flatten
      val NumRbs_0xB887Val = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2317_inst_list.asScala.toList.map(_.Num_Rbs)).flatten
      val NumLayers_0xB887Val = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2317_inst_list.asScala.toList.map(_.Num_Layers)).flatten
      val RNTIType_0xB887Val = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2317_inst_list.asScala.toList.map(_.RNTI_Type)).flatten
      val TXMode_0xB887Val = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2317_inst_list.asScala.toList.map(_.TX_Mode)).flatten
      val NumReTx_0xB887Val = bean47239_65540.sr_2301_inst_list.asScala.toList.map(_.sr_2317_inst_list.asScala.toList.map(_.Num_ReTx)).flatten
      val numRecords = if (CRCstate_0xB887Val != null && CRCstate_0xB887Val.size > 0) CRCstate_0xB887Val.size else 0
      var NumReTx_cnt0 = 0
      var NumReTx_cnt1 = 0
      var NumReTx_cnt2 = 0
      var NumReTx_cnt3 = 0
      var NumReTx_cnt4 = 0
      var CRCFail_cnt0 = 0
      var CRCFail_cnt1 = 0
      var CRCFail_cnt2 = 0
      var CRCFail_cnt3 = 0
      var CRCFail_cnt4 = 0
      if (numRecords > 0) {
        for (i <- 0 to numRecords - 1) {
          if (NumReTx_0xB887Val != null && !NumReTx_0xB887Val.isEmpty && NumReTx_0xB887Val(i) != null) {
            NumReTx_0xB887Val(i).toInt match {
              case 0 =>
                NumReTx_cnt0 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt0 += 1
                }
              case 1 =>
                NumReTx_cnt1 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt1 += 1
                }
              case 2 =>
                NumReTx_cnt2 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt2 += 1
                }
              case 3 =>
                NumReTx_cnt3 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt3 += 1
                }
              case 4 =>
                NumReTx_cnt4 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt4 += 1
                }
              case _ =>
            }
          }
        }
      }
      logRecordQComm5GObj = logRecordQComm5GObj.copy(NRDLMCS = NRDLMCSVal, NRDLModType = NRDLModTypeVal,
        NumRecords_0xB887 = NumRecordsVal, SystemTime = SystemTimeVal, Frame = FrameVal,
        NumPDSCHStatus = NumPDSCHStatusVal, PDSCHStatusInfo = PDSCHStatusInfoVal, CarrierID_0xB887 = CarrierID_0xB887Val,
        CRCstatus_0xB887 = CRCstatus_0xB887Val, CRCstate_0xB887 = CRCstate_0xB887Val, TBSize_0xB887 = TBSize_0xB887Val,
        CellId_0xB887 = CellId_0xB887Val, NumRbs_0xB887 = NumRbs_0xB887Val, NumLayers_0xB887 = NumLayers_0xB887Val,
        RNTIType_0xB887 = RNTIType_0xB887Val, TXMode_0xB887 = TXMode_0xB887Val, status0xB887 = true,
        NumReTx_0xB887 = NumReTx_0xB887Val, NumReTx_cnt0_0xB887 = NumReTx_cnt0, NumReTx_cnt1_0xB887 = NumReTx_cnt1, NumReTx_cnt2_0xB887 = NumReTx_cnt2,
        NumReTx_cnt3_0xB887 = NumReTx_cnt3, NumReTx_cnt4_0xB887 = NumReTx_cnt4, CRCFail_cnt0_0xB887 = CRCFail_cnt0, CRCFail_cnt1_0xB887 = CRCFail_cnt1,
        CRCFail_cnt2_0xB887 = CRCFail_cnt2, CRCFail_cnt3_0xB887 = CRCFail_cnt3, CRCFail_cnt4_0xB887 = CRCFail_cnt4)
      logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47239", hexLogCode = "0xB887", logRecordQComm5G = logRecordQComm5GObj)
    }

    def parse65542 = {
      val bean47239_65542 = process47239_65542(x, exceptionUseArray)
      val NRDLMCSVal = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11663_inst_list.asScala.toList.map(_.MCS)).flatten
      val NRDLModTypeVal = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11663_inst_list.asScala.toList.map(_.Mod_Type)).flatten
      val NumRecordsVal = bean47239_65542.Num_Records
      val SystemTimeVal = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11658_inst_list.asScala.toList.map(_.System_Time)).flatten
      val FrameVal = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11658_inst_list.asScala.toList.map(_.Frame)).flatten
      val NumPDSCHStatusVal = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.Num_PDSCH_Status)
      val PDSCHStatusInfoVal = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11663_inst_list.asScala.toList.map(_.PDSCH_Status_Info)).flatten
      val CarrierID_0xB887Val = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11663_inst_list.asScala.toList.map(_.Carrier_ID)).flatten
      val CRCstatus_0xB887Val = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11663_inst_list.asScala.toList.map(_.CRC_status)).flatten
      val CRCstate_0xB887Val = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11663_inst_list.asScala.toList.map(_.CRC_State)).flatten
      val TBSize_0xB887Val = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11663_inst_list.asScala.toList.map(_.TB_Size)).flatten
      val CellId_0xB887Val = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11663_inst_list.asScala.toList.map(_.Physical_cell_ID)).flatten
      val NumRbs_0xB887Val = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11663_inst_list.asScala.toList.map(_.Num_Rbs)).flatten
      val NumLayers_0xB887Val = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11663_inst_list.asScala.toList.map(_.Num_Layers)).flatten
      val RNTIType_0xB887Val = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11663_inst_list.asScala.toList.map(_.RNTI_Type)).flatten
      val TXMode_0xB887Val = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11663_inst_list.asScala.toList.map(_.TX_Mode)).flatten
      val NumReTx_0xB887Val = bean47239_65542.sr_11661_inst_list.asScala.toList.map(_.sr_11663_inst_list.asScala.toList.map(_.Num_ReTx)).flatten
      val numRecords = if (CRCstate_0xB887Val != null && CRCstate_0xB887Val.size > 0) CRCstate_0xB887Val.size else 0
      var NumReTx_cnt0 = 0
      var NumReTx_cnt1 = 0
      var NumReTx_cnt2 = 0
      var NumReTx_cnt3 = 0
      var NumReTx_cnt4 = 0
      var CRCFail_cnt0 = 0
      var CRCFail_cnt1 = 0
      var CRCFail_cnt2 = 0
      var CRCFail_cnt3 = 0
      var CRCFail_cnt4 = 0
      if (numRecords > 0) {
        for (i <- 0 to numRecords - 1) {
          if (NumReTx_0xB887Val != null && !NumReTx_0xB887Val.isEmpty && NumReTx_0xB887Val(i) != null) {
            NumReTx_0xB887Val(i).toInt match {
              case 0 =>
                NumReTx_cnt0 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt0 += 1
                }
              case 1 =>
                NumReTx_cnt1 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt1 += 1
                }
              case 2 =>
                NumReTx_cnt2 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt2 += 1
                }
              case 3 =>
                NumReTx_cnt3 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt3 += 1
                }
              case 4 =>
                NumReTx_cnt4 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt4 += 1
                }
              case _ =>
            }
          }
        }
      }
      logRecordQComm5GObj = logRecordQComm5GObj.copy(NRDLMCS = NRDLMCSVal, NRDLModType = NRDLModTypeVal,
        NumRecords_0xB887 = NumRecordsVal, SystemTime = SystemTimeVal, Frame = FrameVal,
        NumPDSCHStatus = NumPDSCHStatusVal, PDSCHStatusInfo = PDSCHStatusInfoVal, CarrierID_0xB887 = CarrierID_0xB887Val,
        CRCstatus_0xB887 = CRCstatus_0xB887Val, CRCstate_0xB887 = CRCstate_0xB887Val, TBSize_0xB887 = TBSize_0xB887Val,
        CellId_0xB887 = CellId_0xB887Val, NumRbs_0xB887 = NumRbs_0xB887Val, NumLayers_0xB887 = NumLayers_0xB887Val,
        RNTIType_0xB887 = RNTIType_0xB887Val, TXMode_0xB887 = TXMode_0xB887Val, status0xB887 = true,
        NumReTx_0xB887 = NumReTx_0xB887Val, NumReTx_cnt0_0xB887 = NumReTx_cnt0, NumReTx_cnt1_0xB887 = NumReTx_cnt1, NumReTx_cnt2_0xB887 = NumReTx_cnt2,
        NumReTx_cnt3_0xB887 = NumReTx_cnt3, NumReTx_cnt4_0xB887 = NumReTx_cnt4, CRCFail_cnt0_0xB887 = CRCFail_cnt0, CRCFail_cnt1_0xB887 = CRCFail_cnt1,
        CRCFail_cnt2_0xB887 = CRCFail_cnt2, CRCFail_cnt3_0xB887 = CRCFail_cnt3, CRCFail_cnt4_0xB887 = CRCFail_cnt4)
      logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47239", hexLogCode = "0xB887", logRecordQComm5G = logRecordQComm5GObj)
    }

    def parse65543 = {
      val bean47239_65543 = process47239_65543(x, exceptionUseArray)
      val NRDLMCSVal = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13063_inst_list.asScala.toList.map(_.MCS)).flatten
      val NRDLModTypeVal = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13063_inst_list.asScala.toList.map(_.Mod_Type)).flatten
      val NumRecordsVal = bean47239_65543.Num_Records
      val SystemTimeVal = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13051_inst_list.asScala.toList.map(_.System_Time)).flatten
      val FrameVal = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13051_inst_list.asScala.toList.map(_.Frame)).flatten
      val NumPDSCHStatusVal = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.Num_PDSCH_Status)
      val PDSCHStatusInfoVal = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13063_inst_list.asScala.toList.map(_.PDSCH_Status_Info)).flatten
      val CarrierID_0xB887Val = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13063_inst_list.asScala.toList.map(_.Carrier_ID)).flatten
      val CRCstatus_0xB887Val = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13063_inst_list.asScala.toList.map(_.CRC_status)).flatten
      val CRCstate_0xB887Val = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13063_inst_list.asScala.toList.map(_.CRC_State)).flatten
      val TBSize_0xB887Val = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13063_inst_list.asScala.toList.map(_.TB_Size)).flatten
      val CellId_0xB887Val = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13063_inst_list.asScala.toList.map(_.Physical_cell_ID)).flatten
      val NumRbs_0xB887Val = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13063_inst_list.asScala.toList.map(_.Num_Rbs)).flatten
      val NumLayers_0xB887Val = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13063_inst_list.asScala.toList.map(_.Num_Layers)).flatten
      val RNTIType_0xB887Val = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13063_inst_list.asScala.toList.map(_.RNTI_Type)).flatten
      val TXMode_0xB887Val = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13063_inst_list.asScala.toList.map(_.TX_Mode)).flatten
      val NumReTx_0xB887Val = bean47239_65543.sr_13037_inst_list.asScala.toList.map(_.sr_13063_inst_list.asScala.toList.map(_.Num_ReTx)).flatten
      val numRecords = if (CRCstate_0xB887Val != null && CRCstate_0xB887Val.size > 0) CRCstate_0xB887Val.size else 0
      var NumReTx_cnt0 = 0
      var NumReTx_cnt1 = 0
      var NumReTx_cnt2 = 0
      var NumReTx_cnt3 = 0
      var NumReTx_cnt4 = 0
      var CRCFail_cnt0 = 0
      var CRCFail_cnt1 = 0
      var CRCFail_cnt2 = 0
      var CRCFail_cnt3 = 0
      var CRCFail_cnt4 = 0
      if (numRecords > 0) {
        for (i <- 0 to numRecords - 1) {
          if (NumReTx_0xB887Val != null && !NumReTx_0xB887Val.isEmpty && NumReTx_0xB887Val(i) != null) {
            NumReTx_0xB887Val(i).toInt match {
              case 0 =>
                NumReTx_cnt0 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt0 += 1
                }
              case 1 =>
                NumReTx_cnt1 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt1 += 1
                }
              case 2 =>
                NumReTx_cnt2 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt2 += 1
                }
              case 3 =>
                NumReTx_cnt3 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt3 += 1
                }
              case 4 =>
                NumReTx_cnt4 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt4 += 1
                }
              case _ =>
            }
          }
        }
      }
      logRecordQComm5GObj = logRecordQComm5GObj.copy(NRDLMCS = NRDLMCSVal, NRDLModType = NRDLModTypeVal,
        NumRecords_0xB887 = NumRecordsVal, SystemTime = SystemTimeVal, Frame = FrameVal,
        NumPDSCHStatus = NumPDSCHStatusVal, PDSCHStatusInfo = PDSCHStatusInfoVal, CarrierID_0xB887 = CarrierID_0xB887Val,
        CRCstatus_0xB887 = CRCstatus_0xB887Val, CRCstate_0xB887 = CRCstate_0xB887Val, TBSize_0xB887 = TBSize_0xB887Val,
        CellId_0xB887 = CellId_0xB887Val, NumRbs_0xB887 = NumRbs_0xB887Val, NumLayers_0xB887 = NumLayers_0xB887Val,
        RNTIType_0xB887 = RNTIType_0xB887Val, TXMode_0xB887 = TXMode_0xB887Val, status0xB887 = true,
        NumReTx_0xB887 = NumReTx_0xB887Val, NumReTx_cnt0_0xB887 = NumReTx_cnt0, NumReTx_cnt1_0xB887 = NumReTx_cnt1, NumReTx_cnt2_0xB887 = NumReTx_cnt2,
        NumReTx_cnt3_0xB887 = NumReTx_cnt3, NumReTx_cnt4_0xB887 = NumReTx_cnt4, CRCFail_cnt0_0xB887 = CRCFail_cnt0, CRCFail_cnt1_0xB887 = CRCFail_cnt1,
        CRCFail_cnt2_0xB887 = CRCFail_cnt2, CRCFail_cnt3_0xB887 = CRCFail_cnt3, CRCFail_cnt4_0xB887 = CRCFail_cnt4)
      logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47239", hexLogCode = "0xB887", logRecordQComm5G = logRecordQComm5GObj)
    }

    def parse65545 = {
      val bean47239_65545 = process47239_65545(x, exceptionUseArray)
      //logger.info("bean47239_65545 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47239_65545))
      val NRDLMCSVal = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16646_inst_list.asScala.toList.map(_.MCS)).flatten
      val NRDLModTypeVal = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16646_inst_list.asScala.toList.map(_.Mod_Type)).flatten
      val NumRecordsVal = bean47239_65545.Num_Records
      val SystemTimeVal = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16626_inst_list.asScala.toList.map(_.System_Time)).flatten
      val FrameVal = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16626_inst_list.asScala.toList.map(_.Frame)).flatten
      val NumPDSCHStatusVal = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.Num_PDSCH_Status)
      val PDSCHStatusInfoVal = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16646_inst_list.asScala.toList.map(_.PDSCH_Status_Info)).flatten
      val CarrierID_0xB887Val = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16646_inst_list.asScala.toList.map(_.Carrier_ID)).flatten
      val CRCstatus_0xB887Val = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16646_inst_list.asScala.toList.map(_.CRC_status)).flatten
      val CRCstate_0xB887Val = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16646_inst_list.asScala.toList.map(_.CRC_State)).flatten
      val TBSize_0xB887Val = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16646_inst_list.asScala.toList.map(_.TB_Size)).flatten
      val CellId_0xB887Val = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16646_inst_list.asScala.toList.map(_.Physical_cell_ID)).flatten
      val NumRbs_0xB887Val = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16646_inst_list.asScala.toList.map(_.Num_Rbs)).flatten
      val NumLayers_0xB887Val = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16646_inst_list.asScala.toList.map(_.Num_Layers)).flatten
      val RNTIType_0xB887Val = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16646_inst_list.asScala.toList.map(_.RNTI_Type)).flatten
      val TXMode_0xB887Val = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16646_inst_list.asScala.toList.map(_.TX_Mode)).flatten
      val NumReTx_0xB887Val = bean47239_65545.sr_16612_inst_list.asScala.toList.map(_.sr_16646_inst_list.asScala.toList.map(_.Num_ReTx)).flatten
      val numRecords = if (CRCstate_0xB887Val != null && CRCstate_0xB887Val.size > 0) CRCstate_0xB887Val.size else 0
      var NumReTx_cnt0 = 0
      var NumReTx_cnt1 = 0
      var NumReTx_cnt2 = 0
      var NumReTx_cnt3 = 0
      var NumReTx_cnt4 = 0
      var CRCFail_cnt0 = 0
      var CRCFail_cnt1 = 0
      var CRCFail_cnt2 = 0
      var CRCFail_cnt3 = 0
      var CRCFail_cnt4 = 0
      if (numRecords > 0) {
        for (i <- 0 to numRecords - 1) {
          if (NumReTx_0xB887Val != null && !NumReTx_0xB887Val.isEmpty && NumReTx_0xB887Val(i) != null) {
            NumReTx_0xB887Val(i).toInt match {
              case 0 =>
                NumReTx_cnt0 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt0 += 1
                }
              case 1 =>
                NumReTx_cnt1 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt1 += 1
                }
              case 2 =>
                NumReTx_cnt2 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt2 += 1
                }
              case 3 =>
                NumReTx_cnt3 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt3 += 1
                }
              case 4 =>
                NumReTx_cnt4 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt4 += 1
                }
              case _ =>
            }
          }
        }
      }
      logRecordQComm5GObj = logRecordQComm5GObj.copy(NRDLMCS = NRDLMCSVal, NRDLModType = NRDLModTypeVal,
        NumRecords_0xB887 = NumRecordsVal, SystemTime = SystemTimeVal, Frame = FrameVal,
        NumPDSCHStatus = NumPDSCHStatusVal, PDSCHStatusInfo = PDSCHStatusInfoVal, CarrierID_0xB887 = CarrierID_0xB887Val,
        CRCstatus_0xB887 = CRCstatus_0xB887Val, CRCstate_0xB887 = CRCstate_0xB887Val, TBSize_0xB887 = TBSize_0xB887Val,
        CellId_0xB887 = CellId_0xB887Val, NumRbs_0xB887 = NumRbs_0xB887Val, NumLayers_0xB887 = NumLayers_0xB887Val,
        RNTIType_0xB887 = RNTIType_0xB887Val, TXMode_0xB887 = TXMode_0xB887Val, status0xB887 = true,
        NumReTx_0xB887 = NumReTx_0xB887Val, NumReTx_cnt0_0xB887 = NumReTx_cnt0, NumReTx_cnt1_0xB887 = NumReTx_cnt1, NumReTx_cnt2_0xB887 = NumReTx_cnt2,
        NumReTx_cnt3_0xB887 = NumReTx_cnt3, NumReTx_cnt4_0xB887 = NumReTx_cnt4, CRCFail_cnt0_0xB887 = CRCFail_cnt0, CRCFail_cnt1_0xB887 = CRCFail_cnt1,
        CRCFail_cnt2_0xB887 = CRCFail_cnt2, CRCFail_cnt3_0xB887 = CRCFail_cnt3, CRCFail_cnt4_0xB887 = CRCFail_cnt4)

      //Throughput
      var pdschSum: Long = 0
      var pdschSumPcc: Long = 0
      var pdschSumScc1: Long = 0
      var pdschSumScc2: Long = 0
      var pdschSumScc3: Long = 0
      var pdschSumScc4: Long = 0
      var pdschSumScc5: Long = 0
      var pdschSumScc6: Long = 0
      var pdschSumScc7: Long = 0
      var durationMs: Double = 0

      var pdschSum_15kHz: Int = 0
      var pdschSum_30kHz: Int = 0
      var pdschSum_60kHz: Int = 0
      var pdschSum_120kHz: Int = 0
      var pdschSum_240kHz: Int = 0
      var pdschTput_15kHz: lang.Double = null
      var pdschTput_30kHz: lang.Double = null
      var pdschTput_60kHz: lang.Double = null
      var pdschTput_120kHz: lang.Double = null
      var pdschTput_240kHz: lang.Double = null
      var pdschTputVal: lang.Double = null

      var firstSfn_15kHz: Int = 0
      var firstSfnSlot_15kHz: Int = 0
      var lastSfn_15kHz: Int = 0
      var lastSfnSlot_15kHz: Int = 0
      var firstRecord_15kHz: Boolean = true
      var crcReceived_15kHz: Int = 0
      var crcFail_15kHz: Int = 0
      //var pdschSumScc1_15kHz: Int = 0
      //var numRbsScc1_15kHz: Int = 0
      //var modulationTypeScc1_15kHz: String = ""
      //var mcsScc1_15kHz: String = ""
      //var numLayersScc1_15kHz: Int = 0

      var firstSfn_30kHz: Int = 0
      var firstSfnSlot_30kHz: Int = 0
      var lastSfn_30kHz: Int = 0
      var lastSfnSlot_30kHz: Int = 0
      var firstRecord_30kHz: Boolean = true
      var crcReceived_30kHz: Int = 0
      var crcFail_30kHz: Int = 0
      //var pdschSumScc1_30kHz: Int = 0
      //var numRbsScc1_30kHz: Int = 0
      //var modulationTypeScc1_30kHz: String = ""
      //var mcsScc1_30kHz: String = ""
      //var numLayersScc1_30kHz: Int = 0

      var firstSfn_60kHz: Int = 0
      var firstSfnSlot_60kHz: Int = 0
      var lastSfn_60kHz: Int = 0
      var lastSfnSlot_60kHz: Int = 0
      var firstRecord_60kHz: Boolean = true
      var crcReceived_60kHz: Int = 0
      var crcFail_60kHz: Int = 0
      //var pdschSumScc1_60kHz: Int = 0
      //var numRbsScc1_60kHz: Int = 0
      //var modulationTypeScc1_60kHz: String = ""
      //var mcsScc1_60kHz: String = ""
      //var numLayersScc1_60kHz: Int = 0

      var firstSfn_120kHz: Int = 0
      var firstSfnSlot_120kHz: Int = 0
      var lastSfn_120kHz: Int = 0
      var lastSfnSlot_120kHz: Int = 0
      var firstRecord_120kHz: Boolean = true
      var crcReceived_120kHz: Int = 0
      var crcFail_120kHz: Int = 0
      //var pdschSumScc1_120kHz: Int = 0
      //var numRbsScc1_120kHz: Int = 0
      //var modulationTypeScc1_120kHz: String = ""
      //var mcsScc1_120kHz: String = ""
      //var numLayersScc1_120kHz: Int = 0

      var firstSfn_240kHz: Int = 0
      var firstSfnSlot_240kHz: Int = 0
      var lastSfn_240kHz: Int = 0
      var lastSfnSlot_240kHz: Int = 0
      var firstRecord_240kHz: Boolean = true
      var crcReceived_240kHz: Int = 0
      var crcFail_240kHz: Int = 0
      //var pdschSumScc1_240kHz: Int = 0
      //var numRbsScc1_240kHz: Int = 0
      //var modulationTypeScc1_240kHz: String = ""
      //var mcsScc1_240kHz: String = ""
      //var numLayersScc1_240kHz: Int = 0
      var numerologyListBuffer = new ListBuffer[String]()
      //logger.info("Numerology List Size>>>>>>>>>>>> "+bean47239_65545.getSr_16612_inst_list.size())
      for (i <- 0 to bean47239_65545.getSr_16612_inst_list.size() - 1) {
        val record: Dynamic_47239_65545_SR_16612_DTO = bean47239_65545.getSr_16612_inst_list.get(i)
        numerologyListBuffer += record.getSr_16626_inst_list.get(0).getNumerology

        //logger.info("pdschTput5G numerology for : "+ i +" >>>>> "+ numerologyListBuffer)

        record.getSr_16626_inst_list().get(0).getNumerology() match {
          case "15kHz" =>
            if (firstRecord_15kHz) {
              firstSfn_15kHz = record.getSr_16626_inst_list.get(0).getFrame.toInt
              firstSfnSlot_15kHz = record.getSr_16626_inst_list.get(0).getSlot.toInt
              firstRecord_15kHz = false
            }
            lastSfn_15kHz = record.getSr_16626_inst_list.get(0).getFrame.toInt
            lastSfnSlot_15kHz = record.getSr_16626_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_16646_inst_list.size() - 1) {
              val tb: Dynamic_47239_65545_SR_16646_DTO = record.getSr_16646_inst_list.get(j)

              crcReceived_15kHz += 1
              if (tb.getCRC_status.equalsIgnoreCase("crc_pass") || tb.getCRC_status.equalsIgnoreCase("pass")) {
                pdschSum_15kHz += tb.getTB_Size.toInt
                tb.getCarrier_ID match {
                  case "0" =>
                    pdschSumPcc += tb.getTB_Size.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTB_Size.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTB_Size.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTB_Size.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTB_Size.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTB_Size.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTB_Size.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTB_Size.toInt
                  case _ =>
                }
              } else {
                crcFail_15kHz += 1
              }
              //numRbsScc1_15kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_15kHz = tb.getMod_Type
              //mcsScc1_15kHz = tb.getMCS
              //numLayersScc1_15kHz = tb.getNum_Layers.toInt
            }

          case "30kHz" =>
            if (firstRecord_30kHz) {
              firstSfn_30kHz = record.getSr_16626_inst_list.get(0).getFrame.toInt
              firstSfnSlot_30kHz = record.getSr_16626_inst_list.get(0).getSlot.toInt
              firstRecord_30kHz = false
            }
            lastSfn_30kHz = record.getSr_16626_inst_list.get(0).getFrame.toInt
            lastSfnSlot_30kHz = record.getSr_16626_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_16646_inst_list.size() - 1) {
              val tb: Dynamic_47239_65545_SR_16646_DTO = record.getSr_16646_inst_list.get(j)

              crcReceived_30kHz += 1
              if (tb.getCRC_status.equalsIgnoreCase("crc_pass") || tb.getCRC_status.equalsIgnoreCase("pass")) {
                pdschSum_30kHz += tb.getTB_Size.toInt
                tb.getCarrier_ID match {
                  case "0" =>
                    pdschSumPcc += tb.getTB_Size.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTB_Size.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTB_Size.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTB_Size.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTB_Size.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTB_Size.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTB_Size.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTB_Size.toInt
                  case _ =>
                }
              } else {
                crcFail_30kHz += 1
              }
              //numRbsScc1_30kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_30kHz = tb.getMod_Type
              //mcsScc1_30kHz = tb.getMCS
              //numLayersScc1_30kHz = tb.getNum_Layers.toInt
            }

          case "60kHz" =>
            if (firstRecord_60kHz) {
              firstSfn_60kHz = record.getSr_16626_inst_list.get(0).getFrame.toInt
              firstSfnSlot_60kHz = record.getSr_16626_inst_list.get(0).getSlot.toInt
              firstRecord_60kHz = false
            }
            lastSfn_60kHz = record.getSr_16626_inst_list.get(0).getFrame.toInt
            lastSfnSlot_60kHz = record.getSr_16626_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_16646_inst_list.size() - 1) {
              val tb: Dynamic_47239_65545_SR_16646_DTO = record.getSr_16646_inst_list.get(j)

              crcReceived_60kHz += 1
              if (tb.getCRC_status.equalsIgnoreCase("crc_pass") || tb.getCRC_status.equalsIgnoreCase("pass")) {
                pdschSum_60kHz += tb.getTB_Size.toInt
                tb.getCarrier_ID match {
                  case "0" =>
                    pdschSumPcc += tb.getTB_Size.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTB_Size.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTB_Size.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTB_Size.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTB_Size.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTB_Size.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTB_Size.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTB_Size.toInt
                  case _ =>
                }
              } else {
                crcFail_60kHz += 1
              }
              //numRbsScc1_60kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_60kHz = tb.getMod_Type
              //mcsScc1_60kHz = tb.getMCS
              //numLayersScc1_60kHz = tb.getNum_Layers.toInt
            }

          case "120kHz" =>
            //logger.info("pdschTput5G 120kHz > firstRecord_120kHz = " + firstRecord_120kHz)
            if (firstRecord_120kHz) {
              firstSfn_120kHz = record.getSr_16626_inst_list.get(0).getFrame.toInt
              firstSfnSlot_120kHz = record.getSr_16626_inst_list.get(0).getSlot.toInt
              firstRecord_120kHz = false
            }
            lastSfn_120kHz = record.getSr_16626_inst_list.get(0).getFrame.toInt
            lastSfnSlot_120kHz = record.getSr_16626_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_16646_inst_list.size() - 1) {
              val tb: Dynamic_47239_65545_SR_16646_DTO = record.getSr_16646_inst_list.get(j)

              crcReceived_120kHz += 1
              if (tb.getCRC_status.equalsIgnoreCase("crc_pass") || tb.getCRC_status.equalsIgnoreCase("pass")) {
                pdschSum_120kHz += tb.getTB_Size.toInt
                tb.getCarrier_ID match {
                  case "0" =>
                    pdschSumPcc += tb.getTB_Size.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTB_Size.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTB_Size.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTB_Size.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTB_Size.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTB_Size.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTB_Size.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTB_Size.toInt
                  case _ =>
                }
              } else {
                crcFail_120kHz += 1
              }
              //numRbsScc1_120kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_120kHz = tb.getMod_Type
              //mcsScc1_120kHz = tb.getMCS
              //numLayersScc1_120kHz = tb.getNum_Layers.toInt
            }
          //logger.info("pdschTput5G 120kHz > TS: " + logRecord.dmTimeStamp + "; firstSfn= " + firstSfn_120kHz + "; firstSfnSlot= " + firstSfnSlot_120kHz + "; lastSfn= " + lastSfn_120kHz + "; lastSfnSlot= " + lastSfnSlot_120kHz + "; pdschSum= " + pdschSum_120kHz)

          case "240kHz" =>
            if (firstRecord_240kHz) {
              firstSfn_240kHz = record.getSr_16626_inst_list.get(0).getFrame.toInt
              firstSfnSlot_240kHz = record.getSr_16626_inst_list.get(0).getSlot.toInt
              firstRecord_240kHz = false
            }
            lastSfn_240kHz = record.getSr_16626_inst_list.get(0).getFrame.toInt
            lastSfnSlot_240kHz = record.getSr_16626_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_16646_inst_list.size() - 1) {
              val tb: Dynamic_47239_65545_SR_16646_DTO = record.getSr_16646_inst_list.get(j)

              crcReceived_240kHz += 1
              if (tb.getCRC_status.equalsIgnoreCase("crc_pass") || tb.getCRC_status.equalsIgnoreCase("pass")) {
                pdschSum_240kHz += tb.getTB_Size.toInt
                tb.getCarrier_ID match {
                  case "0" =>
                    pdschSumPcc += tb.getTB_Size.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTB_Size.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTB_Size.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTB_Size.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTB_Size.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTB_Size.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTB_Size.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTB_Size.toInt
                  case _ =>
                }
              } else {
                crcFail_240kHz += 1
              }
              //numRbsScc1_240kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_240kHz = tb.getMod_Type
              //mcsScc1_240kHz = tb.getMCS
              //numLayersScc1_240kHz = tb.getNum_Layers.toInt
            }
        }
      }

      if (!firstRecord_15kHz) {
        val durationMs_15kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("15kHz", firstSfn_15kHz, firstSfnSlot_15kHz, lastSfn_15kHz, lastSfnSlot_15kHz)
        if (durationMs_15kHz > 0) if (pdschSum_15kHz > 0) {
          durationMs = durationMs_15kHz
          pdschTput_15kHz = (pdschSum_15kHz * 8 * (1000 / durationMs_15kHz.asInstanceOf[Double])) / Constants.Mbps

          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_15kHz
          } else {
            pdschTputVal += pdschTput_15kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput15kHz = pdschTput_15kHz,
            firstSfn_15kHz_0xB887 = firstSfn_15kHz,
            firstSfnSlot_15kHz_0xB887 = firstSfnSlot_15kHz,
            lastSfn_15kHz_0xB887 = lastSfn_15kHz,
            lastSfnSlot_15kHz_0xB887 = lastSfnSlot_15kHz)
          //logger.info("pdschTput5G 15kHz >" + pdschTput_15kHz)
        }
      }

      if (!firstRecord_30kHz) {
        val durationMs_30kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("30kHz", firstSfn_30kHz, firstSfnSlot_30kHz, lastSfn_30kHz, lastSfnSlot_30kHz)
        if (durationMs_30kHz > 0) if (pdschSum_30kHz > 0) {
          if (durationMs < durationMs_30kHz) {
            durationMs = durationMs_30kHz
          }
          pdschTput_30kHz = (pdschSum_30kHz * 8 * (1000 / durationMs_30kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_30kHz
          } else {
            pdschTputVal += pdschTput_30kHz
          }

          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput30kHz = pdschTput_30kHz,
            firstSfn_30kHz_0xB887 = firstSfn_30kHz,
            firstSfnSlot_30kHz_0xB887 = firstSfnSlot_30kHz,
            lastSfn_30kHz_0xB887 = lastSfn_30kHz,
            lastSfnSlot_30kHz_0xB887 = lastSfnSlot_30kHz)

          //logger.info("pdschTput5G 30kHz >" + pdschTput_30kHz)
        }
      }

      if (!firstRecord_60kHz) {
        val durationMs_60kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("60kHz", firstSfn_60kHz, firstSfnSlot_60kHz, lastSfn_60kHz, lastSfnSlot_60kHz)
        if (durationMs_60kHz > 0) if (pdschSum_60kHz > 0) {
          if (durationMs < durationMs_60kHz) {
            durationMs = durationMs_60kHz
          }
          pdschTput_60kHz = (pdschSum_60kHz * 8 * (1000 / durationMs_60kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_60kHz
          } else {
            pdschTputVal += pdschTput_60kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput60kHz = pdschTput_60kHz,
            firstSfn_60kHz_0xB887 = firstSfn_60kHz,
            firstSfnSlot_60kHz_0xB887 = firstSfnSlot_60kHz,
            lastSfn_60kHz_0xB887 = lastSfn_60kHz,
            lastSfnSlot_60kHz_0xB887 = lastSfnSlot_60kHz)

          //logger.info("pdschTput5G 60kHz >" + pdschTput_60kHz)
        }
      }

      if (!firstRecord_120kHz) {
        val durationMs_120kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("120kHz", firstSfn_120kHz, firstSfnSlot_120kHz, lastSfn_120kHz, lastSfnSlot_120kHz)
        if (durationMs_120kHz > 0) if (pdschSum_120kHz > 0) {
          if (durationMs < durationMs_120kHz) {
            durationMs = durationMs_120kHz
          }
          pdschTput_120kHz = (pdschSum_120kHz * 8 * (1000 / durationMs_120kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_120kHz
          } else {
            pdschTputVal += pdschTput_120kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput120kHz = pdschTput_120kHz,
            firstSfn_120kHz_0xB887 = firstSfn_120kHz,
            firstSfnSlot_120kHz_0xB887 = firstSfnSlot_120kHz,
            lastSfn_120kHz_0xB887 = lastSfn_120kHz,
            lastSfnSlot_120kHz_0xB887 = lastSfnSlot_120kHz)

          // logger.info("pdschTput5G 120kHz Param> TS: " + logRecord.dmTimeStamp + "; firstSfn= " + firstSfn_120kHz + "; firstSlot= " + firstSfnSlot_120kHz + "; lastSfn= " + lastSfn_120kHz + "; lastSlot= " + lastSfnSlot_120kHz + "; TB Size= " + pdschSum_120kHz)
          // logger.info("pdschTput5G 120kHz Tput > " + "; TS=" + logRecord.dmTimeStamp + "; durationMs= " + durationMs_120kHz + "; TB Size= " + pdschSum_120kHz + "; Tput=" + pdschTput_120kHz + " Mbps")
        }
      }

      if (!firstRecord_240kHz) {
        val durationMs_240kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("240kHz", firstSfn_240kHz, firstSfnSlot_240kHz, lastSfn_240kHz, lastSfnSlot_240kHz)
        if (durationMs_240kHz > 0) if (pdschSum_240kHz > 0) {
          if (durationMs < durationMs_240kHz) {
            durationMs = durationMs_240kHz
          }
          pdschTput_240kHz = (pdschSum_240kHz * 8 * (1000 / durationMs_240kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_240kHz
            pdschTputVal += pdschTput_240kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput240kHz = pdschTput_240kHz,
            firstSfn_240kHz_0xB887 = firstSfn_240kHz,
            firstSfnSlot_240kHz_0xB887 = firstSfnSlot_240kHz,
            lastSfn_240kHz_0xB887 = lastSfn_240kHz,
            lastSfnSlot_240kHz_0xB887 = lastSfnSlot_240kHz)

          //logger.info("pdschTput5G 240kHz >" + pdschTput_240kHz)
        }
      }
      pdschSum = pdschSum_15kHz + pdschSum_30kHz + pdschSum_60kHz + pdschSum_120kHz + pdschSum_240kHz
      logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput = pdschTputVal,
        pdschSum_0xB887 = pdschSum,
        pdschSumPcc_0xB887 = pdschSumPcc,
        pdschSumScc1_0xB887 = pdschSumScc1,
        pdschSumScc2_0xB887 = pdschSumScc2,
        pdschSumScc3_0xB887 = pdschSumScc3,
        pdschSumScc4_0xB887 = pdschSumScc4,
        pdschSumScc5_0xB887 = pdschSumScc5,
        pdschSumScc6_0xB887 = pdschSumScc6,
        pdschSumScc7_0xB887 = pdschSumScc7,
        durationMs_0xB887 = durationMs, status0xB887 = true, numerology_0xB887 = if (numerologyListBuffer.size > 0) numerologyListBuffer.toList else null)
      logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47239", hexLogCode = "0xB887", logRecordQComm5G = logRecordQComm5GObj)
    }

    def parse65547 = {
      val bean47239_65547 = process47239_65547(x, exceptionUseArray)
      //logger.info("bean47239_65547 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47239_65547))
      val NRDLMCSVal = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16425_inst_list.asScala.toList.map(_.MCS)).flatten
      val NRDLModTypeVal = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16425_inst_list.asScala.toList.map(_.Mod_Type)).flatten
      val NumRecordsVal = bean47239_65547.Num_Records
      val SystemTimeVal = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16385_inst_list.asScala.toList.map(_.System_Time)).flatten
      val FrameVal = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16385_inst_list.asScala.toList.map(_.Frame)).flatten
      val NumPDSCHStatusVal = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.Num_PDSCH_Status)
      val PDSCHStatusInfoVal = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16425_inst_list.asScala.toList.map(_.PDSCH_Status_Info)).flatten
      val CarrierID_0xB887Val = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16425_inst_list.asScala.toList.map(_.Carrier_ID)).flatten
      val CRCstatus_0xB887Val = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16425_inst_list.asScala.toList.map(_.CRC_status)).flatten
      val CRCstate_0xB887Val = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16425_inst_list.asScala.toList.map(_.CRC_State)).flatten
      val TBSize_0xB887Val = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16425_inst_list.asScala.toList.map(_.TB_Size)).flatten
      val CellId_0xB887Val = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16425_inst_list.asScala.toList.map(_.Physical_cell_ID)).flatten
      val NumRbs_0xB887Val = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16425_inst_list.asScala.toList.map(_.Num_Rbs)).flatten
      val NumLayers_0xB887Val = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16425_inst_list.asScala.toList.map(_.Num_Layers)).flatten
      val RNTIType_0xB887Val = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16425_inst_list.asScala.toList.map(_.RNTI_Type)).flatten
      val TXMode_0xB887Val = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16425_inst_list.asScala.toList.map(_.TX_Mode)).flatten
      val NumReTx_0xB887Val = bean47239_65547.sr_16370_inst_list.asScala.toList.map(_.sr_16425_inst_list.asScala.toList.map(_.Num_ReTx)).flatten
      val numRecords = if (CRCstate_0xB887Val != null && CRCstate_0xB887Val.size > 0) CRCstate_0xB887Val.size else 0
      var NumReTx_cnt0 = 0
      var NumReTx_cnt1 = 0
      var NumReTx_cnt2 = 0
      var NumReTx_cnt3 = 0
      var NumReTx_cnt4 = 0
      var CRCFail_cnt0 = 0
      var CRCFail_cnt1 = 0
      var CRCFail_cnt2 = 0
      var CRCFail_cnt3 = 0
      var CRCFail_cnt4 = 0
      //logger.info("0xB887(47239) -> Timestamp : "+logRecord.dmTimeStamp + " NumRecords : "+numRecords)
      if (numRecords > 0) {
        for (i <- 0 to numRecords - 1) {
          //logger.info("0xB887(47239) -> Timestamp : "+logRecord.dmTimeStamp + " NumReTx_0xB887Val : "+NumReTx_0xB887Val)
          if (NumReTx_0xB887Val != null && !NumReTx_0xB887Val.isEmpty && NumReTx_0xB887Val(i) != null) {
            //logger.info("0xB887(47239) -> Timestamp : "+logRecord.dmTimeStamp + " I Val : "+i+ " Inside Condition........."+NumReTx_0xB887Val(i))
            NumReTx_0xB887Val(i).toInt match {
              case 0 =>
                NumReTx_cnt0 += 1
                //logger.info("0xB887(47239) -> Timestamp : "+logRecord.dmTimeStamp + " I Val : "+i+ " Case 0........."+NumReTx_cnt0 + " CRCState("+i+") value : "+CRCstate_0xB887Val(i))
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt0 += 1
                }
              case 1 =>
                NumReTx_cnt1 += 1
                //logger.info("0xB887(47239) -> Timestamp : "+logRecord.dmTimeStamp + " I Val : "+i+ " Case 0........."+NumReTx_cnt1 + " CRCState("+i+") value : "+CRCstate_0xB887Val(i))
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt1 += 1
                }
              case 2 =>
                NumReTx_cnt2 += 1
                //logger.info("0xB887(47239) -> Timestamp : "+logRecord.dmTimeStamp + " I Val : "+i+ " Case 0........."+NumReTx_cnt2 + " CRCState("+i+") value : "+CRCstate_0xB887Val(i))
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt2 += 1
                }
              case 3 =>
                NumReTx_cnt3 += 1
                //logger.info("0xB887(47239) -> Timestamp : "+logRecord.dmTimeStamp + " I Val : "+i+ " Case 0........."+NumReTx_cnt3 + " CRCState("+i+") value : "+CRCstate_0xB887Val(i))
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt3 += 1
                }
              case 4 =>
                NumReTx_cnt4 += 1
                //logger.info("0xB887(47239) -> Timestamp : "+logRecord.dmTimeStamp + " I Val : "+i+ " Case 0........."+NumReTx_cnt4 + " CRCState("+i+") value : "+CRCstate_0xB887Val(i))
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt4 += 1
                }
              case _ =>
            }
          }
        }
      }
      logRecordQComm5GObj = logRecordQComm5GObj.copy(NRDLMCS = NRDLMCSVal, NRDLModType = NRDLModTypeVal,
        NumRecords_0xB887 = NumRecordsVal, SystemTime = SystemTimeVal, Frame = FrameVal,
        NumPDSCHStatus = NumPDSCHStatusVal, PDSCHStatusInfo = PDSCHStatusInfoVal, CarrierID_0xB887 = CarrierID_0xB887Val,
        CRCstatus_0xB887 = CRCstatus_0xB887Val, CRCstate_0xB887 = CRCstate_0xB887Val, TBSize_0xB887 = TBSize_0xB887Val,
        CellId_0xB887 = CellId_0xB887Val, NumRbs_0xB887 = NumRbs_0xB887Val, NumLayers_0xB887 = NumLayers_0xB887Val,
        RNTIType_0xB887 = RNTIType_0xB887Val, TXMode_0xB887 = TXMode_0xB887Val, status0xB887 = true,
        NumReTx_0xB887 = NumReTx_0xB887Val, NumReTx_cnt0_0xB887 = NumReTx_cnt0, NumReTx_cnt1_0xB887 = NumReTx_cnt1, NumReTx_cnt2_0xB887 = NumReTx_cnt2,
        NumReTx_cnt3_0xB887 = NumReTx_cnt3, NumReTx_cnt4_0xB887 = NumReTx_cnt4, CRCFail_cnt0_0xB887 = CRCFail_cnt0, CRCFail_cnt1_0xB887 = CRCFail_cnt1,
        CRCFail_cnt2_0xB887 = CRCFail_cnt2, CRCFail_cnt3_0xB887 = CRCFail_cnt3, CRCFail_cnt4_0xB887 = CRCFail_cnt4)

      //Throughput
      var pdschSum: Long = 0
      var pdschSumPcc: Long = 0
      var pdschSumScc1: Long = 0
      var pdschSumScc2: Long = 0
      var pdschSumScc3: Long = 0
      var pdschSumScc4: Long = 0
      var pdschSumScc5: Long = 0
      var pdschSumScc6: Long = 0
      var pdschSumScc7: Long = 0
      var durationMs: Double = 0

      var pdschSum_15kHz: Int = 0
      var pdschSum_30kHz: Int = 0
      var pdschSum_60kHz: Int = 0
      var pdschSum_120kHz: Int = 0
      var pdschSum_240kHz: Int = 0
      var pdschTput_15kHz: lang.Double = null
      var pdschTput_30kHz: lang.Double = null
      var pdschTput_60kHz: lang.Double = null
      var pdschTput_120kHz: lang.Double = null
      var pdschTput_240kHz: lang.Double = null
      var pdschTputVal: lang.Double = null

      var firstSfn_15kHz: Int = 0
      var firstSfnSlot_15kHz: Int = 0
      var lastSfn_15kHz: Int = 0
      var lastSfnSlot_15kHz: Int = 0
      var firstRecord_15kHz: Boolean = true
      var crcReceived_15kHz: Int = 0
      var crcFail_15kHz: Int = 0


      var firstSfn_30kHz: Int = 0
      var firstSfnSlot_30kHz: Int = 0
      var lastSfn_30kHz: Int = 0
      var lastSfnSlot_30kHz: Int = 0
      var firstRecord_30kHz: Boolean = true
      var crcReceived_30kHz: Int = 0
      var crcFail_30kHz: Int = 0


      var firstSfn_60kHz: Int = 0
      var firstSfnSlot_60kHz: Int = 0
      var lastSfn_60kHz: Int = 0
      var lastSfnSlot_60kHz: Int = 0
      var firstRecord_60kHz: Boolean = true
      var crcReceived_60kHz: Int = 0
      var crcFail_60kHz: Int = 0


      var firstSfn_120kHz: Int = 0
      var firstSfnSlot_120kHz: Int = 0
      var lastSfn_120kHz: Int = 0
      var lastSfnSlot_120kHz: Int = 0
      var firstRecord_120kHz: Boolean = true
      var crcReceived_120kHz: Int = 0
      var crcFail_120kHz: Int = 0


      var firstSfn_240kHz: Int = 0
      var firstSfnSlot_240kHz: Int = 0
      var lastSfn_240kHz: Int = 0
      var lastSfnSlot_240kHz: Int = 0
      var firstRecord_240kHz: Boolean = true
      var crcReceived_240kHz: Int = 0
      var crcFail_240kHz: Int = 0

      var numerologyListBuffer = new ListBuffer[String]()
      for (i <- 0 to bean47239_65547.getSr_16370_inst_list.size() - 1) {
        val record: Dynamic_47239_65547_SR_16370_DTO = bean47239_65547.getSr_16370_inst_list.get(i)
        numerologyListBuffer += record.getSr_16385_inst_list.get(0).getNumerology

        //logger.info("pdschTput5G numerology >" + numerology)

        record.getSr_16385_inst_list().get(0).getNumerology() match {
          case "15kHz" =>
            if (firstRecord_15kHz) {
              firstSfn_15kHz = record.getSr_16385_inst_list.get(0).getFrame.toInt
              firstSfnSlot_15kHz = record.getSr_16385_inst_list.get(0).getSlot.toInt
              firstRecord_15kHz = false
            }
            lastSfn_15kHz = record.getSr_16385_inst_list.get(0).getFrame.toInt
            lastSfnSlot_15kHz = record.getSr_16385_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_16385_inst_list.size() - 1) {
              val tb: Dynamic_47239_65547_SR_16425_DTO = record.getSr_16425_inst_list.get(j)

              crcReceived_15kHz += 1
              if (tb.getCRC_status.equalsIgnoreCase("crc_pass") || tb.getCRC_status.equalsIgnoreCase("pass")) {
                pdschSum_15kHz += tb.getTB_Size.toInt
                tb.getCarrier_ID match {
                  case "0" =>
                    pdschSumPcc += tb.getTB_Size.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTB_Size.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTB_Size.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTB_Size.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTB_Size.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTB_Size.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTB_Size.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTB_Size.toInt
                  case _ =>
                }
              } else {
                crcFail_15kHz += 1
              }
              //numRbsScc1_15kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_15kHz = tb.getMod_Type
              //mcsScc1_15kHz = tb.getMCS
              //numLayersScc1_15kHz = tb.getNum_Layers.toInt
            }

          case "30kHz" =>
            if (firstRecord_30kHz) {
              firstSfn_30kHz = record.getSr_16385_inst_list.get(0).getFrame.toInt
              firstSfnSlot_30kHz = record.getSr_16385_inst_list.get(0).getSlot.toInt
              firstRecord_30kHz = false
            }
            lastSfn_30kHz = record.getSr_16385_inst_list.get(0).getFrame.toInt
            lastSfnSlot_30kHz = record.getSr_16385_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_16425_inst_list.size() - 1) {
              val tb: Dynamic_47239_65547_SR_16425_DTO = record.getSr_16425_inst_list.get(j)

              crcReceived_30kHz += 1
              if (tb.getCRC_status.equalsIgnoreCase("crc_pass") || tb.getCRC_status.equalsIgnoreCase("pass")) {
                pdschSum_30kHz += tb.getTB_Size.toInt
                tb.getCarrier_ID match {
                  case "0" =>
                    pdschSumPcc += tb.getTB_Size.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTB_Size.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTB_Size.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTB_Size.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTB_Size.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTB_Size.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTB_Size.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTB_Size.toInt
                  case _ =>
                }
              } else {
                crcFail_30kHz += 1
              }
            }

          case "60kHz" =>
            if (firstRecord_60kHz) {
              firstSfn_60kHz = record.getSr_16385_inst_list.get(0).getFrame.toInt
              firstSfnSlot_60kHz = record.getSr_16385_inst_list.get(0).getSlot.toInt
              firstRecord_60kHz = false
            }
            lastSfn_60kHz = record.getSr_16385_inst_list.get(0).getFrame.toInt
            lastSfnSlot_60kHz = record.getSr_16385_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_16425_inst_list.size() - 1) {
              val tb: Dynamic_47239_65547_SR_16425_DTO = record.getSr_16425_inst_list.get(j)

              crcReceived_60kHz += 1
              if (tb.getCRC_status.equalsIgnoreCase("crc_pass") || tb.getCRC_status.equalsIgnoreCase("pass")) {
                pdschSum_60kHz += tb.getTB_Size.toInt
                tb.getCarrier_ID match {
                  case "0" =>
                    pdschSumPcc += tb.getTB_Size.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTB_Size.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTB_Size.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTB_Size.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTB_Size.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTB_Size.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTB_Size.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTB_Size.toInt
                  case _ =>
                }
              } else {
                crcFail_60kHz += 1
              }
              //numRbsScc1_60kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_60kHz = tb.getMod_Type
              //mcsScc1_60kHz = tb.getMCS
              //numLayersScc1_60kHz = tb.getNum_Layers.toInt
            }

          case "120kHz" =>
            //logger.info("pdschTput5G 120kHz > firstRecord_120kHz = " + firstRecord_120kHz)
            if (firstRecord_120kHz) {
              firstSfn_120kHz = record.getSr_16385_inst_list.get(0).getFrame.toInt
              firstSfnSlot_120kHz = record.getSr_16385_inst_list.get(0).getSlot.toInt
              firstRecord_120kHz = false
            }
            lastSfn_120kHz = record.getSr_16385_inst_list.get(0).getFrame.toInt
            lastSfnSlot_120kHz = record.getSr_16385_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_16425_inst_list.size() - 1) {
              val tb: Dynamic_47239_65547_SR_16425_DTO = record.getSr_16425_inst_list.get(j)

              crcReceived_120kHz += 1
              if (tb.getCRC_status.equalsIgnoreCase("crc_pass") || tb.getCRC_status.equalsIgnoreCase("pass")) {
                pdschSum_120kHz += tb.getTB_Size.toInt
                tb.getCarrier_ID match {
                  case "0" =>
                    pdschSumPcc += tb.getTB_Size.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTB_Size.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTB_Size.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTB_Size.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTB_Size.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTB_Size.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTB_Size.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTB_Size.toInt
                  case _ =>
                }
              } else {
                crcFail_120kHz += 1
              }
              //numRbsScc1_120kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_120kHz = tb.getMod_Type
              //mcsScc1_120kHz = tb.getMCS
              //numLayersScc1_120kHz = tb.getNum_Layers.toInt
            }
          //logger.info("pdschTput5G 120kHz > TS: " + logRecord.dmTimeStamp + "; firstSfn= " + firstSfn_120kHz + "; firstSfnSlot= " + firstSfnSlot_120kHz + "; lastSfn= " + lastSfn_120kHz + "; lastSfnSlot= " + lastSfnSlot_120kHz + "; pdschSum= " + pdschSum_120kHz)

          case "240kHz" =>
            if (firstRecord_240kHz) {
              firstSfn_240kHz = record.getSr_16385_inst_list.get(0).getFrame.toInt
              firstSfnSlot_240kHz = record.getSr_16385_inst_list.get(0).getSlot.toInt
              firstRecord_240kHz = false
            }
            lastSfn_240kHz = record.getSr_16385_inst_list.get(0).getFrame.toInt
            lastSfnSlot_240kHz = record.getSr_16385_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_16425_inst_list.size() - 1) {
              val tb: Dynamic_47239_65547_SR_16425_DTO = record.getSr_16425_inst_list.get(j)

              crcReceived_240kHz += 1
              if (tb.getCRC_status.equalsIgnoreCase("crc_pass") || tb.getCRC_status.equalsIgnoreCase("pass")) {
                pdschSum_240kHz += tb.getTB_Size.toInt

                tb.getCarrier_ID match {
                  case "0" =>
                    pdschSumPcc += tb.getTB_Size.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTB_Size.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTB_Size.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTB_Size.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTB_Size.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTB_Size.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTB_Size.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTB_Size.toInt
                  case _ =>
                }
              } else {
                crcFail_240kHz += 1
              }
              //numRbsScc1_240kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_240kHz = tb.getMod_Type
              //mcsScc1_240kHz = tb.getMCS
              //numLayersScc1_240kHz = tb.getNum_Layers.toInt
            }
        }
      }

      if (!firstRecord_15kHz) {
        val durationMs_15kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("15kHz", firstSfn_15kHz, firstSfnSlot_15kHz, lastSfn_15kHz, lastSfnSlot_15kHz)
        if (durationMs_15kHz > 0) if (pdschSum_15kHz > 0) {
          durationMs = durationMs_15kHz
          pdschTput_15kHz = (pdschSum_15kHz * 8 * (1000 / durationMs_15kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_15kHz
          } else {
            pdschTputVal += pdschTput_15kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput15kHz = pdschTput_15kHz,
            firstSfn_15kHz_0xB887 = firstSfn_15kHz,
            firstSfnSlot_15kHz_0xB887 = firstSfnSlot_15kHz,
            lastSfn_15kHz_0xB887 = lastSfn_15kHz,
            lastSfnSlot_15kHz_0xB887 = lastSfnSlot_15kHz)
          //logger.info("pdschTput5G 15kHz >" + pdschTput_15kHz)
        }
      }

      if (!firstRecord_30kHz) {
        val durationMs_30kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("30kHz", firstSfn_30kHz, firstSfnSlot_30kHz, lastSfn_30kHz, lastSfnSlot_30kHz)
        if (durationMs_30kHz > 0) if (pdschSum_30kHz > 0) {
          if (durationMs < durationMs_30kHz) {
            durationMs = durationMs_30kHz
          }
          pdschTput_30kHz = (pdschSum_30kHz * 8 * (1000 / durationMs_30kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_30kHz
          } else {
            pdschTputVal += pdschTput_30kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput30kHz = pdschTput_30kHz,
            firstSfn_30kHz_0xB887 = firstSfn_30kHz,
            firstSfnSlot_30kHz_0xB887 = firstSfnSlot_30kHz,
            lastSfn_30kHz_0xB887 = lastSfn_30kHz,
            lastSfnSlot_30kHz_0xB887 = lastSfnSlot_30kHz)
          //logger.info("pdschTput5G 30kHz >" + pdschTput_30kHz)

        }
      }

      if (!firstRecord_60kHz) {
        val durationMs_60kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("60kHz", firstSfn_60kHz, firstSfnSlot_60kHz, lastSfn_60kHz, lastSfnSlot_60kHz)
        if (durationMs_60kHz > 0) if (pdschSum_60kHz > 0) {
          if (durationMs < durationMs_60kHz) {
            durationMs = durationMs_60kHz
          }
          pdschTput_60kHz = (pdschSum_60kHz * 8 * (1000 / durationMs_60kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_60kHz
          } else {
            pdschTputVal += pdschTput_60kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput60kHz = pdschTput_60kHz,
            firstSfn_60kHz_0xB887 = firstSfn_60kHz,
            firstSfnSlot_60kHz_0xB887 = firstSfnSlot_60kHz,
            lastSfn_60kHz_0xB887 = lastSfn_60kHz,
            lastSfnSlot_60kHz_0xB887 = lastSfnSlot_60kHz)
          //logger.info("pdschTput5G 60kHz >" + pdschTput_60kHz)
        }
      }

      if (!firstRecord_120kHz) {
        val durationMs_120kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("120kHz", firstSfn_120kHz, firstSfnSlot_120kHz, lastSfn_120kHz, lastSfnSlot_120kHz)
        if (durationMs_120kHz > 0) if (pdschSum_120kHz > 0) {
          if (durationMs < durationMs_120kHz) {
            durationMs = durationMs_120kHz
          }
          pdschTput_120kHz = (pdschSum_120kHz * 8 * (1000 / durationMs_120kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_120kHz
          } else {
            pdschTputVal += pdschTput_120kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput120kHz = pdschTput_120kHz,
            firstSfn_120kHz_0xB887 = firstSfn_120kHz,
            firstSfnSlot_120kHz_0xB887 = firstSfnSlot_120kHz,
            lastSfn_120kHz_0xB887 = lastSfn_120kHz,
            lastSfnSlot_120kHz_0xB887 = lastSfnSlot_120kHz)
          //logger.info("pdschTput5G 120kHz Param> TS: " + logRecord.dmTimeStamp + "; firstSfn= " + firstSfn_120kHz + "; firstSlot= " + firstSfnSlot_120kHz + "; lastSfn= " + lastSfn_120kHz + "; lastSlot= " + lastSfnSlot_120kHz + "; TB Size= " + pdschSum_120kHz)
          //logger.info("pdschTput5G 120kHz Tput > " + "; TS=" + logRecord.dmTimeStamp + "; durationMs= " + durationMs_120kHz + "; TB Size= " + pdschSum_120kHz + "; Tput=" + pdschTput_120kHz + " Mbps")
          //logger.info("pdschTput5G 120kHz Tput > " + "; TS=" + logRecord.dmTimeStamp + "; durationMs= " + durationMs_120kHz + "; pdschSum= " + pdschSum_120kHz + "; pdschTput=" + pdschTput_120kHz + " Mbps")
        }
      }

      if (!firstRecord_240kHz) {
        val durationMs_240kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("240kHz", firstSfn_240kHz, firstSfnSlot_240kHz, lastSfn_240kHz, lastSfnSlot_240kHz)
        if (durationMs_240kHz > 0) if (pdschSum_240kHz > 0) {
          if (durationMs < durationMs_240kHz) {
            durationMs = durationMs_240kHz
          }
          pdschTput_240kHz = (pdschSum_240kHz * 8 * (1000 / durationMs_240kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_240kHz
          } else {
            pdschTputVal += pdschTput_240kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput240kHz = pdschTput_240kHz,
            firstSfn_240kHz_0xB887 = firstSfn_240kHz,
            firstSfnSlot_240kHz_0xB887 = firstSfnSlot_240kHz,
            lastSfn_240kHz_0xB887 = lastSfn_240kHz,
            lastSfnSlot_240kHz_0xB887 = lastSfnSlot_240kHz)

          //logger.info("pdschTput5G 240kHz >" + pdschTput_240kHz)
        }
      }

      pdschSum = pdschSum_15kHz + pdschSum_30kHz + pdschSum_60kHz + pdschSum_120kHz + pdschSum_240kHz
      logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput = pdschTputVal,
        pdschSum_0xB887 = pdschSum,
        pdschSumPcc_0xB887 = pdschSumPcc,
        pdschSumScc1_0xB887 = pdschSumScc1,
        pdschSumScc2_0xB887 = pdschSumScc2,
        pdschSumScc3_0xB887 = pdschSumScc3,
        pdschSumScc4_0xB887 = pdschSumScc4,
        pdschSumScc5_0xB887 = pdschSumScc5,
        pdschSumScc6_0xB887 = pdschSumScc6,
        pdschSumScc7_0xB887 = pdschSumScc7,
        durationMs_0xB887 = durationMs, status0xB887 = true, numerology_0xB887 = if (numerologyListBuffer.size > 0) numerologyListBuffer.toList else null)
      logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47239", hexLogCode = "0xB887", logRecordQComm5G = logRecordQComm5GObj)
    }

    def parse131076or5 = {
      val bean47239_131076 = process47239_131076(x, exceptionUseArray)
      val NRDLMCSVal = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19776_inst_list.asScala.toList.map(_.MCS)).flatten
      val NRDLModTypeVal = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19776_inst_list.asScala.toList.map(_.ModType)).flatten
      val NumRecordsVal = bean47239_131076.NumRecords
      val SystemTimeVal = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19735_inst_list.asScala.toList.map(_.SystemTime)).flatten
      val FrameVal = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19735_inst_list.asScala.toList.map(_.Frame)).flatten
      val NumPDSCHStatusVal = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.NumPDSCHStatus)
      val PDSCHStatusInfoVal = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19776_inst_list.asScala.toList.map(_.PDSCHStatusInfo)).flatten
      val CarrierID_0xB887Val = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19776_inst_list.asScala.toList.map(_.CarrierID)).flatten
      val CRCstatus_0xB887Val = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19776_inst_list.asScala.toList.map(_.CRCStatus)).flatten
      val CRCstate_0xB887Val = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19776_inst_list.asScala.toList.map(_.CRCState)).flatten
      val TBSize_0xB887Val = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19776_inst_list.asScala.toList.map(_.TBSize)).flatten
      val CellId_0xB887Val = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19776_inst_list.asScala.toList.map(_.PhysicalcellID)).flatten
      val NumRbs_0xB887Val = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19776_inst_list.asScala.toList.map(_.NumRbs)).flatten
      val NumLayers_0xB887Val = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19776_inst_list.asScala.toList.map(_.NumLayers)).flatten
      val RNTIType_0xB887Val = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19776_inst_list.asScala.toList.map(_.RNTIType)).flatten
      val TXMode_0xB887Val = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19776_inst_list.asScala.toList.map(_.NumRX)).flatten
      val NRMIMOType_0xB887Val: List[String] = bean47239_131076.sr_19721_inst_list.asScala.toList.flatMap(_.sr_19776_inst_list.asScala.toList.filter(_.CarrierID.equals("0")).map(_.NumRX))
      val NumReTx_0xB887Val = bean47239_131076.sr_19721_inst_list.asScala.toList.map(_.sr_19776_inst_list.asScala.toList.map(_.NumReTx)).flatten
      val numRecords = if (CRCstate_0xB887Val != null && CRCstate_0xB887Val.size > 0) CRCstate_0xB887Val.size else 0
      var NumReTx_cnt0 = 0
      var NumReTx_cnt1 = 0
      var NumReTx_cnt2 = 0
      var NumReTx_cnt3 = 0
      var NumReTx_cnt4 = 0
      var CRCFail_cnt0 = 0
      var CRCFail_cnt1 = 0
      var CRCFail_cnt2 = 0
      var CRCFail_cnt3 = 0
      var CRCFail_cnt4 = 0
      if (numRecords > 0) {
        for (i <- 0 to numRecords - 1) {
          if (NumReTx_0xB887Val != null && !NumReTx_0xB887Val.isEmpty && NumReTx_0xB887Val(i) != null) {
            NumReTx_0xB887Val(i).toInt match {
              case 0 =>
                NumReTx_cnt0 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt0 += 1
                }
              case 1 =>
                NumReTx_cnt1 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt1 += 1
                }
              case 2 =>
                NumReTx_cnt2 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt2 += 1
                }
              case 3 =>
                NumReTx_cnt3 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt3 += 1
                }
              case 4 =>
                NumReTx_cnt4 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt4 += 1
                }
              case _ =>
            }
          }
        }
      }
      logRecordQComm5GObj = logRecordQComm5GObj.copy(NRDLMCS = NRDLMCSVal, NRDLModType = NRDLModTypeVal,
        NumRecords_0xB887 = NumRecordsVal, SystemTime = SystemTimeVal, Frame = FrameVal,
        NumPDSCHStatus = NumPDSCHStatusVal, PDSCHStatusInfo = PDSCHStatusInfoVal, CarrierID_0xB887 = CarrierID_0xB887Val,
        CRCstatus_0xB887 = CRCstatus_0xB887Val, CRCstate_0xB887 = CRCstate_0xB887Val, TBSize_0xB887 = TBSize_0xB887Val,
        CellId_0xB887 = CellId_0xB887Val, NumRbs_0xB887 = NumRbs_0xB887Val, NumLayers_0xB887 = NumLayers_0xB887Val,
        RNTIType_0xB887 = RNTIType_0xB887Val, TXMode_0xB887 = TXMode_0xB887Val, NRMIMOType_0xB887 = NRMIMOType_0xB887Val, status0xB887 = true,
        NumReTx_0xB887 = NumReTx_0xB887Val, NumReTx_cnt0_0xB887 = NumReTx_cnt0, NumReTx_cnt1_0xB887 = NumReTx_cnt1, NumReTx_cnt2_0xB887 = NumReTx_cnt2,
        NumReTx_cnt3_0xB887 = NumReTx_cnt3, NumReTx_cnt4_0xB887 = NumReTx_cnt4, CRCFail_cnt0_0xB887 = CRCFail_cnt0, CRCFail_cnt1_0xB887 = CRCFail_cnt1,
        CRCFail_cnt2_0xB887 = CRCFail_cnt2, CRCFail_cnt3_0xB887 = CRCFail_cnt3, CRCFail_cnt4_0xB887 = CRCFail_cnt4)

      //Throughput
      var pdschSum: Long = 0
      var pdschSumPcc: Long = 0
      var pdschSumScc1: Long = 0
      var pdschSumScc2: Long = 0
      var pdschSumScc3: Long = 0
      var pdschSumScc4: Long = 0
      var pdschSumScc5: Long = 0
      var pdschSumScc6: Long = 0
      var pdschSumScc7: Long = 0
      var durationMs: Double = 0

      var pdschSum_15kHz: Int = 0
      var pdschSum_30kHz: Int = 0
      var pdschSum_60kHz: Int = 0
      var pdschSum_120kHz: Int = 0
      var pdschSum_240kHz: Int = 0
      var pdschTput_15kHz: lang.Double = null
      var pdschTput_30kHz: lang.Double = null
      var pdschTput_60kHz: lang.Double = null
      var pdschTput_120kHz: lang.Double = null
      var pdschTput_240kHz: lang.Double = null
      var pdschTputVal: lang.Double = null

      var firstSfn_15kHz: Int = 0
      var firstSfnSlot_15kHz: Int = 0
      var lastSfn_15kHz: Int = 0
      var lastSfnSlot_15kHz: Int = 0
      var firstRecord_15kHz: Boolean = true
      var crcReceived_15kHz: Int = 0
      var crcFail_15kHz: Int = 0

      var firstSfn_30kHz: Int = 0
      var firstSfnSlot_30kHz: Int = 0
      var lastSfn_30kHz: Int = 0
      var lastSfnSlot_30kHz: Int = 0
      var firstRecord_30kHz: Boolean = true
      var crcReceived_30kHz: Int = 0
      var crcFail_30kHz: Int = 0

      var firstSfn_60kHz: Int = 0
      var firstSfnSlot_60kHz: Int = 0
      var lastSfn_60kHz: Int = 0
      var lastSfnSlot_60kHz: Int = 0
      var firstRecord_60kHz: Boolean = true
      var crcReceived_60kHz: Int = 0
      var crcFail_60kHz: Int = 0

      var firstSfn_120kHz: Int = 0
      var firstSfnSlot_120kHz: Int = 0
      var lastSfn_120kHz: Int = 0
      var lastSfnSlot_120kHz: Int = 0
      var firstRecord_120kHz: Boolean = true
      var crcReceived_120kHz: Int = 0
      var crcFail_120kHz: Int = 0

      var firstSfn_240kHz: Int = 0
      var firstSfnSlot_240kHz: Int = 0
      var lastSfn_240kHz: Int = 0
      var lastSfnSlot_240kHz: Int = 0
      var firstRecord_240kHz: Boolean = true
      var crcReceived_240kHz: Int = 0
      var crcFail_240kHz: Int = 0
      var numerologyListBuffer = new ListBuffer[String]()
      for (i <- 0 to bean47239_131076.getSr_19721_inst_list.size() - 1) {
        val record: Dynamic_47239_131076_SR_19721_DTO = bean47239_131076.getSr_19721_inst_list.get(i)
        numerologyListBuffer += record.getSr_19735_inst_list.get(0).getNumerology

        //logger.info("pdschTput5G numerology >" + numerology)

        record.getSr_19735_inst_list().get(0).getNumerology() match {
          case "15kHz" =>
            if (firstRecord_15kHz) {
              firstSfn_15kHz = record.getSr_19735_inst_list.get(0).getFrame.toInt
              firstSfnSlot_15kHz = record.getSr_19735_inst_list.get(0).getSlot.toInt
              firstRecord_15kHz = false
            }
            lastSfn_15kHz = record.getSr_19735_inst_list.get(0).getFrame.toInt
            lastSfnSlot_15kHz = record.getSr_19735_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_19735_inst_list.size() - 1) {
              val tb: Dynamic_47239_131076_SR_19776_DTO = record.getSr_19776_inst_list.get(j)

              crcReceived_15kHz += 1
              if (tb.getCRCStatus.equalsIgnoreCase("crc_pass") || tb.getCRCStatus.equalsIgnoreCase("pass")) {
                pdschSum_15kHz += tb.getTBSize.toInt
                tb.getCarrierID match {
                  case "0" =>
                    pdschSumPcc += tb.getTBSize.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTBSize.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTBSize.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTBSize.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTBSize.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTBSize.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTBSize.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTBSize.toInt
                  case _ =>
                }
              } else {
                crcFail_15kHz += 1
              }
              //numRbsScc1_15kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_15kHz = tb.getMod_Type
              //mcsScc1_15kHz = tb.getMCS
              //numLayersScc1_15kHz = tb.getNum_Layers.toInt
            }

          case "30kHz" =>
            if (firstRecord_30kHz) {
              firstSfn_30kHz = record.getSr_19735_inst_list.get(0).getFrame.toInt
              firstSfnSlot_30kHz = record.getSr_19735_inst_list.get(0).getSlot.toInt
              firstRecord_30kHz = false
            }
            lastSfn_30kHz = record.getSr_19735_inst_list.get(0).getFrame.toInt
            lastSfnSlot_30kHz = record.getSr_19735_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_19776_inst_list.size() - 1) {
              val tb: Dynamic_47239_131076_SR_19776_DTO = record.getSr_19776_inst_list.get(j)

              crcReceived_30kHz += 1
              if (tb.getCRCStatus.equalsIgnoreCase("crc_pass") || tb.getCRCStatus.equalsIgnoreCase("pass")) {
                pdschSum_30kHz += tb.getTBSize.toInt
                tb.getCarrierID match {
                  case "0" =>
                    pdschSumPcc += tb.getTBSize.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTBSize.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTBSize.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTBSize.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTBSize.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTBSize.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTBSize.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTBSize.toInt
                  case _ =>
                }
              } else {
                crcFail_30kHz += 1
              }
              //numRbsScc1_30kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_30kHz = tb.getMod_Type
              //mcsScc1_30kHz = tb.getMCS
              //numLayersScc1_30kHz = tb.getNum_Layers.toInt
            }

          case "60kHz" =>
            if (firstRecord_60kHz) {
              firstSfn_60kHz = record.getSr_19735_inst_list.get(0).getFrame.toInt
              firstSfnSlot_60kHz = record.getSr_19735_inst_list.get(0).getSlot.toInt
              firstRecord_60kHz = false
            }
            lastSfn_60kHz = record.getSr_19735_inst_list.get(0).getFrame.toInt
            lastSfnSlot_60kHz = record.getSr_19735_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_19776_inst_list.size() - 1) {
              val tb: Dynamic_47239_131076_SR_19776_DTO = record.getSr_19776_inst_list.get(j)

              crcReceived_60kHz += 1
              if (tb.getCRCStatus.equalsIgnoreCase("crc_pass") || tb.getCRCStatus.equalsIgnoreCase("pass")) {
                pdschSum_60kHz += tb.getTBSize.toInt
                tb.getCarrierID match {
                  case "0" =>
                    pdschSumPcc += tb.getTBSize.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTBSize.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTBSize.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTBSize.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTBSize.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTBSize.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTBSize.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTBSize.toInt
                  case _ =>
                }
              } else {
                crcFail_60kHz += 1
              }
              //numRbsScc1_60kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_60kHz = tb.getMod_Type
              //mcsScc1_60kHz = tb.getMCS
              //numLayersScc1_60kHz = tb.getNum_Layers.toInt
            }

          case "120kHz" =>
            //logger.info("pdschTput5G 120kHz > firstRecord_120kHz = " + firstRecord_120kHz)
            if (firstRecord_120kHz) {
              firstSfn_120kHz = record.getSr_19735_inst_list.get(0).getFrame.toInt
              firstSfnSlot_120kHz = record.getSr_19735_inst_list.get(0).getSlot.toInt
              firstRecord_120kHz = false
            }
            lastSfn_120kHz = record.getSr_19735_inst_list.get(0).getFrame.toInt
            lastSfnSlot_120kHz = record.getSr_19735_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_19776_inst_list.size() - 1) {
              val tb: Dynamic_47239_131076_SR_19776_DTO = record.getSr_19776_inst_list.get(j)

              crcReceived_120kHz += 1
              if (tb.getCRCStatus.equalsIgnoreCase("crc_pass") || tb.getCRCStatus.equalsIgnoreCase("pass")) {
                pdschSum_120kHz += tb.getTBSize.toInt
                tb.getCarrierID match {
                  case "0" =>
                    pdschSumPcc += tb.getTBSize.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTBSize.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTBSize.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTBSize.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTBSize.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTBSize.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTBSize.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTBSize.toInt
                  case _ =>
                }
              } else {
                crcFail_120kHz += 1
              }
              //numRbsScc1_120kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_120kHz = tb.getMod_Type
              //mcsScc1_120kHz = tb.getMCS
              //numLayersScc1_120kHz = tb.getNum_Layers.toInt
            }
          //logger.info("pdschTput5G 120kHz > TS: " + logRecord.dmTimeStamp + "; firstSfn= " + firstSfn_120kHz + "; firstSfnSlot= " + firstSfnSlot_120kHz + "; lastSfn= " + lastSfn_120kHz + "; lastSfnSlot= " + lastSfnSlot_120kHz + "; pdschSum= " + pdschSum_120kHz)

          case "240kHz" =>
            if (firstRecord_240kHz) {
              firstSfn_240kHz = record.getSr_19735_inst_list.get(0).getFrame.toInt
              firstSfnSlot_240kHz = record.getSr_19735_inst_list.get(0).getSlot.toInt
              firstRecord_240kHz = false
            }
            lastSfn_240kHz = record.getSr_19735_inst_list.get(0).getFrame.toInt
            lastSfnSlot_240kHz = record.getSr_19735_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_19776_inst_list.size() - 1) {
              val tb: Dynamic_47239_131076_SR_19776_DTO = record.getSr_19776_inst_list.get(j)

              crcReceived_240kHz += 1
              if (tb.getCRCStatus.equalsIgnoreCase("crc_pass") || tb.getCRCStatus.equalsIgnoreCase("pass")) {
                pdschSum_240kHz += tb.getTBSize.toInt

                tb.getCarrierID match {
                  case "0" =>
                    pdschSumPcc += tb.getTBSize.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTBSize.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTBSize.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTBSize.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTBSize.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTBSize.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTBSize.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTBSize.toInt
                  case _ =>
                }
              } else {
                crcFail_240kHz += 1
              }
              //numRbsScc1_240kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_240kHz = tb.getMod_Type
              //mcsScc1_240kHz = tb.getMCS
              //numLayersScc1_240kHz = tb.getNum_Layers.toInt
            }
        }
      }

      if (!firstRecord_15kHz) {
        val durationMs_15kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("15kHz", firstSfn_15kHz, firstSfnSlot_15kHz, lastSfn_15kHz, lastSfnSlot_15kHz)
        if (durationMs_15kHz > 0) if (pdschSum_15kHz > 0) {
          durationMs = durationMs_15kHz
          pdschTput_15kHz = (pdschSum_15kHz * 8 * (1000 / durationMs_15kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_15kHz
          } else {
            pdschTputVal += pdschTput_15kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput15kHz = pdschTput_15kHz,
            firstSfn_15kHz_0xB887 = firstSfn_15kHz,
            firstSfnSlot_15kHz_0xB887 = firstSfnSlot_15kHz,
            lastSfn_15kHz_0xB887 = lastSfn_15kHz,
            lastSfnSlot_15kHz_0xB887 = lastSfnSlot_15kHz)
          //logger.info("pdschTput5G 15kHz >" + pdschTput_15kHz)
        }
      }

      if (!firstRecord_30kHz) {
        val durationMs_30kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("30kHz", firstSfn_30kHz, firstSfnSlot_30kHz, lastSfn_30kHz, lastSfnSlot_30kHz)
        if (durationMs_30kHz > 0) if (pdschSum_30kHz > 0) {
          if (durationMs < durationMs_30kHz) {
            durationMs = durationMs_30kHz
          }
          pdschTput_30kHz = (pdschSum_30kHz * 8 * (1000 / durationMs_30kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_30kHz
          } else {
            pdschTputVal += pdschTput_30kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput30kHz = pdschTput_30kHz,
            firstSfn_30kHz_0xB887 = firstSfn_30kHz,
            firstSfnSlot_30kHz_0xB887 = firstSfnSlot_30kHz,
            lastSfn_30kHz_0xB887 = lastSfn_30kHz,
            lastSfnSlot_30kHz_0xB887 = lastSfnSlot_30kHz)
          //logger.info("pdschTput5G 30kHz >" + pdschTput_30kHz)

        }
      }

      if (!firstRecord_60kHz) {
        val durationMs_60kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("60kHz", firstSfn_60kHz, firstSfnSlot_60kHz, lastSfn_60kHz, lastSfnSlot_60kHz)
        if (durationMs_60kHz > 0) if (pdschSum_60kHz > 0) {
          if (durationMs < durationMs_60kHz) {
            durationMs = durationMs_60kHz
          }
          pdschTput_60kHz = (pdschSum_60kHz * 8 * (1000 / durationMs_60kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_60kHz
          } else {
            pdschTputVal += pdschTput_60kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput60kHz = pdschTput_60kHz,
            firstSfn_60kHz_0xB887 = firstSfn_60kHz,
            firstSfnSlot_60kHz_0xB887 = firstSfnSlot_60kHz,
            lastSfn_60kHz_0xB887 = lastSfn_60kHz,
            lastSfnSlot_60kHz_0xB887 = lastSfnSlot_60kHz)
          //logger.info("pdschTput5G 60kHz >" + pdschTput_60kHz)
        }
      }

      if (!firstRecord_120kHz) {
        val durationMs_120kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("120kHz", firstSfn_120kHz, firstSfnSlot_120kHz, lastSfn_120kHz, lastSfnSlot_120kHz)
        if (durationMs_120kHz > 0) if (pdschSum_120kHz > 0) {
          if (durationMs < durationMs_120kHz) {
            durationMs = durationMs_120kHz
          }
          pdschTput_120kHz = (pdschSum_120kHz * 8 * (1000 / durationMs_120kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_120kHz
          } else {
            pdschTputVal += pdschTput_120kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput120kHz = pdschTput_120kHz,
            firstSfn_120kHz_0xB887 = firstSfn_120kHz,
            firstSfnSlot_120kHz_0xB887 = firstSfnSlot_120kHz,
            lastSfn_120kHz_0xB887 = lastSfn_120kHz,
            lastSfnSlot_120kHz_0xB887 = lastSfnSlot_120kHz)
          //logger.info("pdschTput5G 120kHz Param> TS: " + logRecord.dmTimeStamp + "; firstSfn= " + firstSfn_120kHz + "; firstSlot= " + firstSfnSlot_120kHz + "; lastSfn= " + lastSfn_120kHz + "; lastSlot= " + lastSfnSlot_120kHz + "; TB Size= " + pdschSum_120kHz)
          //logger.info("pdschTput5G 120kHz Tput > " + "; TS=" + logRecord.dmTimeStamp + "; durationMs= " + durationMs_120kHz + "; TB Size= " + pdschSum_120kHz + "; Tput=" + pdschTput_120kHz + " Mbps")
          //logger.info("pdschTput5G 120kHz Tput > " + "; TS=" + logRecord.dmTimeStamp + "; durationMs= " + durationMs_120kHz + "; pdschSum= " + pdschSum_120kHz + "; pdschTput=" + pdschTput_120kHz + " Mbps")
        }
      }

      if (!firstRecord_240kHz) {
        val durationMs_240kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("240kHz", firstSfn_240kHz, firstSfnSlot_240kHz, lastSfn_240kHz, lastSfnSlot_240kHz)
        if (durationMs_240kHz > 0) if (pdschSum_240kHz > 0) {
          if (durationMs < durationMs_240kHz) {
            durationMs = durationMs_240kHz
          }
          pdschTput_240kHz = (pdschSum_240kHz * 8 * (1000 / durationMs_240kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_240kHz
          } else {
            pdschTputVal += pdschTput_240kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput240kHz = pdschTput_240kHz,
            firstSfn_240kHz_0xB887 = firstSfn_240kHz,
            firstSfnSlot_240kHz_0xB887 = firstSfnSlot_240kHz,
            lastSfn_240kHz_0xB887 = lastSfn_240kHz,
            lastSfnSlot_240kHz_0xB887 = lastSfnSlot_240kHz)

          //logger.info("pdschTput5G 240kHz >" + pdschTput_240kHz)
        }
      }

      pdschSum = pdschSum_15kHz + pdschSum_30kHz + pdschSum_60kHz + pdschSum_120kHz + pdschSum_240kHz
      logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput = pdschTputVal,
        pdschSum_0xB887 = pdschSum,
        pdschSumPcc_0xB887 = pdschSumPcc,
        pdschSumScc1_0xB887 = pdschSumScc1,
        pdschSumScc2_0xB887 = pdschSumScc2,
        pdschSumScc3_0xB887 = pdschSumScc3,
        pdschSumScc4_0xB887 = pdschSumScc4,
        pdschSumScc5_0xB887 = pdschSumScc5,
        pdschSumScc6_0xB887 = pdschSumScc6,
        pdschSumScc7_0xB887 = pdschSumScc7,
        durationMs_0xB887 = durationMs, status0xB887 = true, numerology_0xB887 = if (numerologyListBuffer.size > 0) numerologyListBuffer.toList else null)
      logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47239", hexLogCode = "0xB887", logRecordQComm5G = logRecordQComm5GObj)

    }

    def parse131077 = {
      val bean47239_131077 = process47239_131077(x, exceptionUseArray)
      val NRDLMCSVal = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27701_inst_list.asScala.toList.map(_.MCS)).flatten
      val NRDLModTypeVal = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27701_inst_list.asScala.toList.map(_.ModType)).flatten
      val NumRecordsVal = bean47239_131077.NumRecords
      val SystemTimeVal = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27694_inst_list.asScala.toList.map(_.SystemTime)).flatten
      val FrameVal = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27694_inst_list.asScala.toList.map(_.Frame)).flatten
      val NumPDSCHStatusVal = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.NumPDSCHStatus)
      val PDSCHStatusInfoVal = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27701_inst_list.asScala.toList.map(_.PDSCHStatusInfo)).flatten
      val CarrierID_0xB887Val = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27701_inst_list.asScala.toList.map(_.CarrierID)).flatten
      val CRCstatus_0xB887Val = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27701_inst_list.asScala.toList.map(_.CRCStatus)).flatten
      val CRCstate_0xB887Val = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27701_inst_list.asScala.toList.map(_.CRCState)).flatten
      val TBSize_0xB887Val = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27701_inst_list.asScala.toList.map(_.TBSize)).flatten
      val CellId_0xB887Val = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27701_inst_list.asScala.toList.map(_.PhysicalcellID)).flatten
      val NumRbs_0xB887Val = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27701_inst_list.asScala.toList.map(_.NumRbs)).flatten
      val NumLayers_0xB887Val = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27701_inst_list.asScala.toList.map(_.NumLayers)).flatten
      val RNTIType_0xB887Val = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27701_inst_list.asScala.toList.map(_.RNTIType)).flatten
      val TXMode_0xB887Val = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27701_inst_list.asScala.toList.map(_.NumRX)).flatten
      val NRMIMOType_0xB887Val: List[String] = bean47239_131077.sr_27681_inst_list.asScala.toList.flatMap(_.sr_27701_inst_list.asScala.toList.filter(_.CarrierID.equals("0")).map(_.NumRX))
      val NumReTx_0xB887Val = bean47239_131077.sr_27681_inst_list.asScala.toList.map(_.sr_27701_inst_list.asScala.toList.map(_.NumReTx)).flatten
      val numRecords = if (CRCstate_0xB887Val != null && CRCstate_0xB887Val.size > 0) CRCstate_0xB887Val.size else 0
      var NumReTx_cnt0 = 0
      var NumReTx_cnt1 = 0
      var NumReTx_cnt2 = 0
      var NumReTx_cnt3 = 0
      var NumReTx_cnt4 = 0
      var CRCFail_cnt0 = 0
      var CRCFail_cnt1 = 0
      var CRCFail_cnt2 = 0
      var CRCFail_cnt3 = 0
      var CRCFail_cnt4 = 0
      if (numRecords > 0) {
        for (i <- 0 to numRecords - 1) {
          if (NumReTx_0xB887Val != null && !NumReTx_0xB887Val.isEmpty && NumReTx_0xB887Val(i) != null) {
            NumReTx_0xB887Val(i).toInt match {
              case 0 =>
                NumReTx_cnt0 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt0 += 1
                }
              case 1 =>
                NumReTx_cnt1 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt1 += 1
                }
              case 2 =>
                NumReTx_cnt2 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt2 += 1
                }
              case 3 =>
                NumReTx_cnt3 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt3 += 1
                }
              case 4 =>
                NumReTx_cnt4 += 1
                if (CRCstate_0xB887Val != null && !CRCstate_0xB887Val.isEmpty && CRCstate_0xB887Val(i) != null && CRCstate_0xB887Val(i).equalsIgnoreCase("fail")) {
                  CRCFail_cnt4 += 1
                }
              case _ =>
            }
          }
        }
      }
      logRecordQComm5GObj = logRecordQComm5GObj.copy(NRDLMCS = NRDLMCSVal, NRDLModType = NRDLModTypeVal,
        NumRecords_0xB887 = NumRecordsVal, SystemTime = SystemTimeVal, Frame = FrameVal,
        NumPDSCHStatus = NumPDSCHStatusVal, PDSCHStatusInfo = PDSCHStatusInfoVal, CarrierID_0xB887 = CarrierID_0xB887Val,
        CRCstatus_0xB887 = CRCstatus_0xB887Val, CRCstate_0xB887 = CRCstate_0xB887Val, TBSize_0xB887 = TBSize_0xB887Val,
        CellId_0xB887 = CellId_0xB887Val, NumRbs_0xB887 = NumRbs_0xB887Val, NumLayers_0xB887 = NumLayers_0xB887Val,
        RNTIType_0xB887 = RNTIType_0xB887Val, TXMode_0xB887 = TXMode_0xB887Val, NRMIMOType_0xB887 = NRMIMOType_0xB887Val, status0xB887 = true,
        NumReTx_0xB887 = NumReTx_0xB887Val, NumReTx_cnt0_0xB887 = NumReTx_cnt0, NumReTx_cnt1_0xB887 = NumReTx_cnt1, NumReTx_cnt2_0xB887 = NumReTx_cnt2,
        NumReTx_cnt3_0xB887 = NumReTx_cnt3, NumReTx_cnt4_0xB887 = NumReTx_cnt4, CRCFail_cnt0_0xB887 = CRCFail_cnt0, CRCFail_cnt1_0xB887 = CRCFail_cnt1,
        CRCFail_cnt2_0xB887 = CRCFail_cnt2, CRCFail_cnt3_0xB887 = CRCFail_cnt3, CRCFail_cnt4_0xB887 = CRCFail_cnt4)

      //Throughput
      var pdschSum: Long = 0
      var pdschSumPcc: Long = 0
      var pdschSumScc1: Long = 0
      var pdschSumScc2: Long = 0
      var pdschSumScc3: Long = 0
      var pdschSumScc4: Long = 0
      var pdschSumScc5: Long = 0
      var pdschSumScc6: Long = 0
      var pdschSumScc7: Long = 0
      var durationMs: Double = 0

      var pdschSum_15kHz: Int = 0
      var pdschSum_30kHz: Int = 0
      var pdschSum_60kHz: Int = 0
      var pdschSum_120kHz: Int = 0
      var pdschSum_240kHz: Int = 0
      var pdschTput_15kHz: lang.Double = null
      var pdschTput_30kHz: lang.Double = null
      var pdschTput_60kHz: lang.Double = null
      var pdschTput_120kHz: lang.Double = null
      var pdschTput_240kHz: lang.Double = null
      var pdschTputVal: lang.Double = null

      var firstSfn_15kHz: Int = 0
      var firstSfnSlot_15kHz: Int = 0
      var lastSfn_15kHz: Int = 0
      var lastSfnSlot_15kHz: Int = 0
      var firstRecord_15kHz: Boolean = true
      var crcReceived_15kHz: Int = 0
      var crcFail_15kHz: Int = 0

      var firstSfn_30kHz: Int = 0
      var firstSfnSlot_30kHz: Int = 0
      var lastSfn_30kHz: Int = 0
      var lastSfnSlot_30kHz: Int = 0
      var firstRecord_30kHz: Boolean = true
      var crcReceived_30kHz: Int = 0
      var crcFail_30kHz: Int = 0

      var firstSfn_60kHz: Int = 0
      var firstSfnSlot_60kHz: Int = 0
      var lastSfn_60kHz: Int = 0
      var lastSfnSlot_60kHz: Int = 0
      var firstRecord_60kHz: Boolean = true
      var crcReceived_60kHz: Int = 0
      var crcFail_60kHz: Int = 0

      var firstSfn_120kHz: Int = 0
      var firstSfnSlot_120kHz: Int = 0
      var lastSfn_120kHz: Int = 0
      var lastSfnSlot_120kHz: Int = 0
      var firstRecord_120kHz: Boolean = true
      var crcReceived_120kHz: Int = 0
      var crcFail_120kHz: Int = 0

      var firstSfn_240kHz: Int = 0
      var firstSfnSlot_240kHz: Int = 0
      var lastSfn_240kHz: Int = 0
      var lastSfnSlot_240kHz: Int = 0
      var firstRecord_240kHz: Boolean = true
      var crcReceived_240kHz: Int = 0
      var crcFail_240kHz: Int = 0
      var numerologyListBuffer = new ListBuffer[String]()
      for (i <- 0 to bean47239_131077.getSr_27681_inst_list.size() - 1) {
        val record: Dynamic_47239_131077_SR_27681_DTO = bean47239_131077.getSr_27681_inst_list.get(i)
        numerologyListBuffer += record.getSr_27694_inst_list.get(0).getNumerology

        //logger.info("pdschTput5G numerology >" + numerology)

        record.getSr_27694_inst_list().get(0).getNumerology() match {
          case "15kHz" =>
            if (firstRecord_15kHz) {
              firstSfn_15kHz = record.getSr_27694_inst_list.get(0).getFrame.toInt
              firstSfnSlot_15kHz = record.getSr_27694_inst_list.get(0).getSlot.toInt
              firstRecord_15kHz = false
            }
            lastSfn_15kHz = record.getSr_27694_inst_list.get(0).getFrame.toInt
            lastSfnSlot_15kHz = record.getSr_27694_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_27694_inst_list.size() - 1) {
              val tb: Dynamic_47239_131077_SR_27701_DTO = record.getSr_27701_inst_list.get(j)

              crcReceived_15kHz += 1
              //logger.info("crc status........" + tb.getCRCStatus)
              if (tb.getCRCStatus.equalsIgnoreCase("crc_pass") || tb.getCRCStatus.equalsIgnoreCase("pass")) {
                pdschSum_15kHz += tb.getTBSize.toInt
                tb.getCarrierID match {
                  case "0" =>
                    pdschSumPcc += tb.getTBSize.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTBSize.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTBSize.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTBSize.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTBSize.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTBSize.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTBSize.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTBSize.toInt
                  case _ =>
                }
              } else {
                crcFail_15kHz += 1
              }
              //numRbsScc1_15kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_15kHz = tb.getMod_Type
              //mcsScc1_15kHz = tb.getMCS
              //numLayersScc1_15kHz = tb.getNum_Layers.toInt
            }

          case "30kHz" =>
            if (firstRecord_30kHz) {
              firstSfn_30kHz = record.getSr_27694_inst_list.get(0).getFrame.toInt
              firstSfnSlot_30kHz = record.getSr_27694_inst_list.get(0).getSlot.toInt
              firstRecord_30kHz = false
            }
            lastSfn_30kHz = record.getSr_27694_inst_list.get(0).getFrame.toInt
            lastSfnSlot_30kHz = record.getSr_27694_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_27694_inst_list.size() - 1) {
              val tb: Dynamic_47239_131077_SR_27701_DTO = record.getSr_27701_inst_list.get(j)

              crcReceived_30kHz += 1
              if (tb.getCRCStatus.equalsIgnoreCase("crc_pass") || tb.getCRCStatus.equalsIgnoreCase("pass")) {
                pdschSum_30kHz += tb.getTBSize.toInt
                tb.getCarrierID match {
                  case "0" =>
                    pdschSumPcc += tb.getTBSize.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTBSize.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTBSize.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTBSize.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTBSize.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTBSize.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTBSize.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTBSize.toInt
                  case _ =>
                }
              } else {
                crcFail_30kHz += 1
              }
              //numRbsScc1_30kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_30kHz = tb.getMod_Type
              //mcsScc1_30kHz = tb.getMCS
              //numLayersScc1_30kHz = tb.getNum_Layers.toInt
            }

          case "60kHz" =>
            if (firstRecord_60kHz) {
              firstSfn_60kHz = record.getSr_27694_inst_list.get(0).getFrame.toInt
              firstSfnSlot_60kHz = record.getSr_27694_inst_list.get(0).getSlot.toInt
              firstRecord_60kHz = false
            }
            lastSfn_60kHz = record.getSr_27694_inst_list.get(0).getFrame.toInt
            lastSfnSlot_60kHz = record.getSr_27694_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_27694_inst_list.size() - 1) {
              val tb: Dynamic_47239_131077_SR_27701_DTO = record.getSr_27701_inst_list.get(j)

              crcReceived_60kHz += 1
              if (tb.getCRCStatus.equalsIgnoreCase("crc_pass") || tb.getCRCStatus.equalsIgnoreCase("pass")) {
                pdschSum_60kHz += tb.getTBSize.toInt
                tb.getCarrierID match {
                  case "0" =>
                    pdschSumPcc += tb.getTBSize.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTBSize.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTBSize.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTBSize.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTBSize.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTBSize.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTBSize.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTBSize.toInt
                  case _ =>
                }
              } else {
                crcFail_60kHz += 1
              }
              //numRbsScc1_60kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_60kHz = tb.getMod_Type
              //mcsScc1_60kHz = tb.getMCS
              //numLayersScc1_60kHz = tb.getNum_Layers.toInt
            }

          case "120kHz" =>
            //logger.info("pdschTput5G 120kHz > firstRecord_120kHz = " + firstRecord_120kHz)
            if (firstRecord_120kHz) {
              firstSfn_120kHz = record.getSr_27694_inst_list.get(0).getFrame.toInt
              firstSfnSlot_120kHz = record.getSr_27694_inst_list.get(0).getSlot.toInt
              firstRecord_120kHz = false
            }
            lastSfn_120kHz = record.getSr_27694_inst_list.get(0).getFrame.toInt
            lastSfnSlot_120kHz = record.getSr_27694_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_27694_inst_list.size() - 1) {
              val tb: Dynamic_47239_131077_SR_27701_DTO = record.getSr_27701_inst_list.get(j)

              crcReceived_120kHz += 1
              if (tb.getCRCStatus.equalsIgnoreCase("crc_pass") || tb.getCRCStatus.equalsIgnoreCase("pass")) {
                pdschSum_120kHz += tb.getTBSize.toInt
                tb.getCarrierID match {
                  case "0" =>
                    pdschSumPcc += tb.getTBSize.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTBSize.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTBSize.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTBSize.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTBSize.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTBSize.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTBSize.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTBSize.toInt
                  case _ =>
                }
              } else {
                crcFail_120kHz += 1
              }
              //numRbsScc1_120kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_120kHz = tb.getMod_Type
              //mcsScc1_120kHz = tb.getMCS
              //numLayersScc1_120kHz = tb.getNum_Layers.toInt
            }
          //logger.info("pdschTput5G 120kHz > TS: " + logRecord.dmTimeStamp + "; firstSfn= " + firstSfn_120kHz + "; firstSfnSlot= " + firstSfnSlot_120kHz + "; lastSfn= " + lastSfn_120kHz + "; lastSfnSlot= " + lastSfnSlot_120kHz + "; pdschSum= " + pdschSum_120kHz)

          case "240kHz" =>
            if (firstRecord_240kHz) {
              firstSfn_240kHz = record.getSr_27694_inst_list.get(0).getFrame.toInt
              firstSfnSlot_240kHz = record.getSr_27694_inst_list.get(0).getSlot.toInt
              firstRecord_240kHz = false
            }
            lastSfn_240kHz = record.getSr_27694_inst_list.get(0).getFrame.toInt
            lastSfnSlot_240kHz = record.getSr_27694_inst_list.get(0).getSlot.toInt

            for (j <- 0 to record.getSr_27694_inst_list.size() - 1) {
              val tb: Dynamic_47239_131077_SR_27701_DTO = record.getSr_27701_inst_list.get(j)

              crcReceived_240kHz += 1
              if (tb.getCRCStatus.equalsIgnoreCase("crc_pass") || tb.getCRCStatus.equalsIgnoreCase("pass")) {
                pdschSum_240kHz += tb.getTBSize.toInt

                tb.getCarrierID match {
                  case "0" =>
                    pdschSumPcc += tb.getTBSize.toInt
                  case "1" =>
                    pdschSumScc1 += tb.getTBSize.toInt
                  case "2" =>
                    pdschSumScc2 += tb.getTBSize.toInt
                  case "3" =>
                    pdschSumScc3 += tb.getTBSize.toInt
                  case "4" =>
                    pdschSumScc4 += tb.getTBSize.toInt
                  case "5" =>
                    pdschSumScc5 += tb.getTBSize.toInt
                  case "6" =>
                    pdschSumScc6 += tb.getTBSize.toInt
                  case "7" =>
                    pdschSumScc7 += tb.getTBSize.toInt
                  case _ =>
                }
              } else {
                crcFail_240kHz += 1
              }
              //numRbsScc1_240kHz = tb.getNum_Rbs.toInt
              //modulationTypeScc1_240kHz = tb.getMod_Type
              //mcsScc1_240kHz = tb.getMCS
              //numLayersScc1_240kHz = tb.getNum_Layers.toInt
            }
        }
      }

      if (!firstRecord_15kHz) {
        val durationMs_15kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("15kHz", firstSfn_15kHz, firstSfnSlot_15kHz, lastSfn_15kHz, lastSfnSlot_15kHz)
        if (durationMs_15kHz > 0) if (pdschSum_15kHz > 0) {
          durationMs = durationMs_15kHz
          pdschTput_15kHz = (pdschSum_15kHz * 8 * (1000 / durationMs_15kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_15kHz
          } else {
            pdschTputVal += pdschTput_15kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput15kHz = pdschTput_15kHz,
            firstSfn_15kHz_0xB887 = firstSfn_15kHz,
            firstSfnSlot_15kHz_0xB887 = firstSfnSlot_15kHz,
            lastSfn_15kHz_0xB887 = lastSfn_15kHz,
            lastSfnSlot_15kHz_0xB887 = lastSfnSlot_15kHz)
          //logger.info("pdschTput5G 15kHz >" + pdschTput_15kHz)
        }
      }

      if (!firstRecord_30kHz) {
        val durationMs_30kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("30kHz", firstSfn_30kHz, firstSfnSlot_30kHz, lastSfn_30kHz, lastSfnSlot_30kHz)
        if (durationMs_30kHz > 0) if (pdschSum_30kHz > 0) {
          if (durationMs < durationMs_30kHz) {
            durationMs = durationMs_30kHz
          }
          pdschTput_30kHz = (pdschSum_30kHz * 8 * (1000 / durationMs_30kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_30kHz
          } else {
            pdschTputVal += pdschTput_30kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput30kHz = pdschTput_30kHz,
            firstSfn_30kHz_0xB887 = firstSfn_30kHz,
            firstSfnSlot_30kHz_0xB887 = firstSfnSlot_30kHz,
            lastSfn_30kHz_0xB887 = lastSfn_30kHz,
            lastSfnSlot_30kHz_0xB887 = lastSfnSlot_30kHz)
          //logger.info("pdschTput5G 30kHz >" + pdschTput_30kHz)

        }
      }

      if (!firstRecord_60kHz) {
        val durationMs_60kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("60kHz", firstSfn_60kHz, firstSfnSlot_60kHz, lastSfn_60kHz, lastSfnSlot_60kHz)
        if (durationMs_60kHz > 0) if (pdschSum_60kHz > 0) {
          if (durationMs < durationMs_60kHz) {
            durationMs = durationMs_60kHz
          }
          pdschTput_60kHz = (pdschSum_60kHz * 8 * (1000 / durationMs_60kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_60kHz
          } else {
            pdschTputVal += pdschTput_60kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput60kHz = pdschTput_60kHz,
            firstSfn_60kHz_0xB887 = firstSfn_60kHz,
            firstSfnSlot_60kHz_0xB887 = firstSfnSlot_60kHz,
            lastSfn_60kHz_0xB887 = lastSfn_60kHz,
            lastSfnSlot_60kHz_0xB887 = lastSfnSlot_60kHz)
          //logger.info("pdschTput5G 60kHz >" + pdschTput_60kHz)
        }
      }

      if (!firstRecord_120kHz) {
        val durationMs_120kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("120kHz", firstSfn_120kHz, firstSfnSlot_120kHz, lastSfn_120kHz, lastSfnSlot_120kHz)
        if (durationMs_120kHz > 0) if (pdschSum_120kHz > 0) {
          if (durationMs < durationMs_120kHz) {
            durationMs = durationMs_120kHz
          }
          pdschTput_120kHz = (pdschSum_120kHz * 8 * (1000 / durationMs_120kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_120kHz
          } else {
            pdschTputVal += pdschTput_120kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput120kHz = pdschTput_120kHz,
            firstSfn_120kHz_0xB887 = firstSfn_120kHz,
            firstSfnSlot_120kHz_0xB887 = firstSfnSlot_120kHz,
            lastSfn_120kHz_0xB887 = lastSfn_120kHz,
            lastSfnSlot_120kHz_0xB887 = lastSfnSlot_120kHz)
          //logger.info("pdschTput5G 120kHz Param> TS: " + logRecord.dmTimeStamp + "; firstSfn= " + firstSfn_120kHz + "; firstSlot= " + firstSfnSlot_120kHz + "; lastSfn= " + lastSfn_120kHz + "; lastSlot= " + lastSfnSlot_120kHz + "; TB Size= " + pdschSum_120kHz)
          //logger.info("pdschTput5G 120kHz Tput > " + "; TS=" + logRecord.dmTimeStamp + "; durationMs= " + durationMs_120kHz + "; TB Size= " + pdschSum_120kHz + "; Tput=" + pdschTput_120kHz + " Mbps")
          //logger.info("pdschTput5G 120kHz Tput > " + "; TS=" + logRecord.dmTimeStamp + "; durationMs= " + durationMs_120kHz + "; pdschSum= " + pdschSum_120kHz + "; pdschTput=" + pdschTput_120kHz + " Mbps")
        }
      }

      if (!firstRecord_240kHz) {
        val durationMs_240kHz: Double = Constants.CalculateMsFromSfnAndSubFrames5gNr("240kHz", firstSfn_240kHz, firstSfnSlot_240kHz, lastSfn_240kHz, lastSfnSlot_240kHz)
        if (durationMs_240kHz > 0) if (pdschSum_240kHz > 0) {
          if (durationMs < durationMs_240kHz) {
            durationMs = durationMs_240kHz
          }
          pdschTput_240kHz = (pdschSum_240kHz * 8 * (1000 / durationMs_240kHz.asInstanceOf[Double])) / Constants.Mbps
          if (pdschTputVal == null) {
            pdschTputVal = pdschTput_240kHz
          } else {
            pdschTputVal += pdschTput_240kHz
          }
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput240kHz = pdschTput_240kHz,
            firstSfn_240kHz_0xB887 = firstSfn_240kHz,
            firstSfnSlot_240kHz_0xB887 = firstSfnSlot_240kHz,
            lastSfn_240kHz_0xB887 = lastSfn_240kHz,
            lastSfnSlot_240kHz_0xB887 = lastSfnSlot_240kHz)

          //logger.info("pdschTput5G 240kHz >" + pdschTput_240kHz)
        }
      }

      pdschSum = pdschSum_15kHz + pdschSum_30kHz + pdschSum_60kHz + pdschSum_120kHz + pdschSum_240kHz
      logRecordQComm5GObj = logRecordQComm5GObj.copy(NrPdschTput = pdschTputVal,
        pdschSum_0xB887 = pdschSum,
        pdschSumPcc_0xB887 = pdschSumPcc,
        pdschSumScc1_0xB887 = pdschSumScc1,
        pdschSumScc2_0xB887 = pdschSumScc2,
        pdschSumScc3_0xB887 = pdschSumScc3,
        pdschSumScc4_0xB887 = pdschSumScc4,
        pdschSumScc5_0xB887 = pdschSumScc5,
        pdschSumScc6_0xB887 = pdschSumScc6,
        pdschSumScc7_0xB887 = pdschSumScc7,
        durationMs_0xB887 = durationMs, status0xB887 = true, numerology_0xB887 = if (numerologyListBuffer.size > 0) numerologyListBuffer.toList else null)
      logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47239", hexLogCode = "0xB887", logRecordQComm5G = logRecordQComm5GObj)

    }

    logCode match {
      case 47239 =>
        logVersion match {
          case 65540 =>
            parse65540
          case 65542 =>
            parse65542
          case 65543 =>
            parse65543
          case 65545 =>
            parse65545
          case 65547 =>
            parse65547
          case 131076 | 5 =>
            parse131076or5
          case 131077 =>
            parse131077
          case _ =>
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47239", hexLogCode = "0xB887", missingVersion = logVersion)
        }
        logRecord
    }
  }

  def parseLogRecordQCOMM5GSet5(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]): LogRecord = {
    import collection.JavaConverters._

    def parse47487(parentlogRecord: LogRecord): LogRecord = {
      var logRecord: LogRecord = parentlogRecord
      var logRecordQComm5GObj: LogRecordQComm5G = LogRecordQComm5G(is5GTechnology = true, isLogCode0xB97F = true)
      var logRecordQComm5G2Obj: LogRecordQComm5G2 = LogRecordQComm5G2()
      try {
        logVersion match {
          case 65537 =>
            val bean47487 = process47487_65537(x, exceptionUseArray)
            //logger.info("bean47487_131075 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47487_131075))
            val NRSBRSRPVal = bean47487.sr_8444_inst_list.asScala.toList.map(_.sr_8453_inst_list.asScala.toList.map(_.sr_8459_inst_list.asScala.toList.map(_.sr_8464_inst_list.asScala.toList.map(_.Filtered_Tx_Beam_RSRP)))).flatten.flatten.flatten
            val NRSBRSRQVal = bean47487.sr_8444_inst_list.asScala.toList.map(_.sr_8453_inst_list.asScala.toList.map(_.sr_8459_inst_list.asScala.toList.map(_.sr_8465_inst_list.asScala.toList.map(_.Filtered_Tx_Beam_RSRQ)))).flatten.flatten.flatten
            val NRPCIVal = bean47487.sr_8444_inst_list.asScala.toList.map(_.sr_8453_inst_list.asScala.toList.map(_.PCI)).flatten
            val NRBEAMCOUNTVal = bean47487.sr_8444_inst_list.asScala.toList.map(_.sr_8453_inst_list.asScala.toList.map(_.Num_Beams)).flatten
            val CCIndexVal = bean47487.sr_8444_inst_list.asScala.toList.map(_.Serving_Cell_Index)
            val NRServingBeamIndexVal = bean47487.sr_8444_inst_list.asScala.toList.map(_.sr_8453_inst_list.asScala.toList.map(_.Serving_Beam_Array_Index)).flatten
            val NRServingCellPCIVal = bean47487.sr_8444_inst_list.asScala.toList.map(_.Serving_Cell_Index)
            val NrARFCNVal = bean47487.sr_8444_inst_list.asScala.toList.map(_.Raster_Freq)

            var NrBandIndVal: Int = 0
            var NrBandType: String = null
            var servingCellIndx = Try(CCIndexVal.map(_.toInt)).getOrElse(null)
            var nrarfcn = Try(NrARFCNVal.map(_.toInt)).getOrElse(null)
            if (nrarfcn != null && servingCellIndx != null) {
              NrBandIndVal = ParseUtils.getNrBandIndicator(nrarfcn.head)
              if (NrBandIndVal != 0 && servingCellIndx.head != 255) {
                if (NrBandIndVal >= 257 && NrBandIndVal <= 261) {
                  NrBandType = "mmW (FR2)"
                } else if (NrBandIndVal == 77) {
                  NrBandType = "C-Band (FR1)"
                } else if (NrBandIndVal >= 1 && NrBandIndVal <= 95) {
                  NrBandType = "DSS (FR1)"
                }
              }
            }

            logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSBRSRP_0xB97F = NRSBRSRPVal, NRSBRSRQ_0xB97F = NRSBRSRQVal, NRBEAMCOUNT_0XB97F = Try(NRBEAMCOUNTVal.map(_.toInt)).getOrElse(null), CCIndex_0xB97F = servingCellIndx,
              NRPCI_0XB97F = Try(NRPCIVal.map(_.toInt)).getOrElse(null), NRServingBeamIndex_0xB97F = Try(NRServingBeamIndexVal.head.toInt.asInstanceOf[Integer]).getOrElse(null), NRServingCellPci_0xB97F = Try(NRServingCellPCIVal.map(_.toInt)).getOrElse(null),
              NRBandType_0xB97F = NrBandType, NRBandInd_0xB97F = if (NrBandIndVal > 0) NrBandIndVal else null)
            logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NrPCCARFCN_0XB97F = nrarfcn.head)
            logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47487", hexLogCode = "0xB97F", logRecordQComm5G = logRecordQComm5GObj, logRecordQComm5G2 = logRecordQComm5G2Obj)
          case 65538 =>
            val bean47487 = process47487_65538(x, exceptionUseArray)
            //logger.info("bean47487_131075 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47487_131075))
            val NRSBRSRPVal = bean47487.sr_19021_inst_list.asScala.toList.map(_.sr_19029_inst_list.asScala.toList.map(_.sr_19034_inst_list.asScala.toList.map(_.sr_19073_inst_list.asScala.toList.map(_.FilteredTxBeamRSRP)))).flatten.flatten.flatten
            val NRSBRSRQVal = bean47487.sr_19021_inst_list.asScala.toList.map(_.sr_19029_inst_list.asScala.toList.map(_.sr_19034_inst_list.asScala.toList.map(_.sr_19043_inst_list.asScala.toList.map(_.FilteredTxBeamRSRQ)))).flatten.flatten.flatten
            val NRPCIVal = bean47487.sr_19021_inst_list.asScala.toList.map(_.sr_19029_inst_list.asScala.toList.map(_.PCI)).flatten
            val NRBEAMCOUNTVal = bean47487.sr_19021_inst_list.asScala.toList.map(_.sr_19029_inst_list.asScala.toList.map(_.NumBeams)).flatten
            val CCIndexVal = bean47487.sr_19021_inst_list.asScala.toList.map(_.ServingCellIndex)
            val NRServingBeamIndexVal = bean47487.sr_19021_inst_list.asScala.toList.map(_.sr_19029_inst_list.asScala.toList.map(_.ServingBeamArrayIndex)).flatten
            val NRServingCellPCIVal = bean47487.sr_19021_inst_list.asScala.toList.map(_.ServingCellIndex)
            val NrARFCNVal = bean47487.sr_19021_inst_list.asScala.toList.map(_.RasterFreq)
            var NrBandIndVal: Int = 0
            var NrBandType: String = null
            var servingCellIndx = Try(CCIndexVal.map(_.toInt)).getOrElse(null)
            var nrarfcn = Try(NrARFCNVal.map(_.toInt)).getOrElse(null)
            if (nrarfcn != null && servingCellIndx != null) {
              NrBandIndVal = ParseUtils.getNrBandIndicator(nrarfcn.head)
              if (NrBandIndVal != 0 && servingCellIndx.head != 255) {
                if (NrBandIndVal >= 257 && NrBandIndVal <= 261) {
                  NrBandType = "mmW (FR2)"
                } else if (NrBandIndVal == 77) {
                  NrBandType = "C-Band (FR1)"
                } else if (NrBandIndVal >= 1 && NrBandIndVal <= 95) {
                  NrBandType = "DSS (FR1)"
                }
              }
            }
            logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSBRSRP_0xB97F = NRSBRSRPVal, NRSBRSRQ_0xB97F = NRSBRSRQVal, NRBEAMCOUNT_0XB97F = Try(NRBEAMCOUNTVal.map(_.toInt)).getOrElse(null), CCIndex_0xB97F = servingCellIndx,
              NRPCI_0XB97F = Try(NRPCIVal.map(_.toInt)).getOrElse(null), NRServingBeamIndex_0xB97F = Try(NRServingBeamIndexVal.head.toInt.asInstanceOf[Integer]).getOrElse(null), NRServingCellPci_0xB97F = Try(NRServingCellPCIVal.map(_.toInt)).getOrElse(null),
              NRBandType_0xB97F = NrBandType, NRBandInd_0xB97F = if (NrBandIndVal > 0) NrBandIndVal else null)
            logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NrPCCARFCN_0XB97F = nrarfcn.head)
            logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47487", hexLogCode = "0xB97F", logRecordQComm5G = logRecordQComm5GObj, logRecordQComm5G2 = logRecordQComm5G2Obj)
          case 131075 =>
            val bean47487_131075 = process47487_131075(x, exceptionUseArray)
            //logger.info("bean47487_131075 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47487_131075))
            val NRSBRSRPVal = bean47487_131075.sr_27567_inst_list.asScala.toList.map(_.sr_27574_inst_list.asScala.toList.map(_.sr_27604_inst_list.asScala.toList.map(_.CellQualityRSRP))).flatten.flatten
            val NRSBRSRQVal = bean47487_131075.sr_27567_inst_list.asScala.toList.map(_.sr_27574_inst_list.asScala.toList.map(_.sr_27605_inst_list.asScala.toList.map(_.CellQualityRSRQ))).flatten.flatten
            val NRPCIVal = bean47487_131075.sr_27567_inst_list.asScala.toList.map(_.sr_27574_inst_list.asScala.toList.map(_.PCI)).flatten
            val NRBEAMCOUNTVal = bean47487_131075.sr_27567_inst_list.asScala.toList.map(_.sr_27574_inst_list.asScala.toList.map(_.NumBeams)).flatten
            val CCIndexVal = bean47487_131075.sr_27567_inst_list.asScala.toList.map(_.ServingCellIndex)
            val NRServingBeamIndexVal = bean47487_131075.sr_27567_inst_list.asScala.toList.map(_.ServingSSB)
            val NRServingCellPCIVal = bean47487_131075.sr_27567_inst_list.asScala.toList.map(_.ServingCellPCI)
            val NRArfcnVal = bean47487_131075.sr_27567_inst_list.asScala.toList.map(_.RasterFreq)

            var nrServingPCCPCI_0xB97FVal: List[String] = null
            var nrServingSCC1PCI_0xB97FVal: List[String] = null
            var nrServingSCC2PCI_0xB97FVal: List[String] = null
            var nrServingSCC3PCI_0xB97FVal: List[String] = null
            var nrServingSCC4PCI_0xB97FVal: List[String] = null
            var nrServingSCC5PCI_0xB97FVal: List[String] = null

            var nrServingPCCRxBeamId0_0xB97FVal: Integer = null
            var nrServingPCCRxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC1RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC1RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC2RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC2RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC3RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC3RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC4RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC4RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC5RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC5RxBeamId1_0xB97FVal: Integer = null

            var nrServingPCCRxBeamRSRP0_0xB97FVal: List[String] = null
            var nrServingPCCRxBeamRSRP1_0xB97FVal: List[String] = null
            var nrServingSCC1RxBeamRSRP0_0xB97FVal: List[String] = null
            var nrServingSCC1RxBeamRSRP1_0xB97FVal: List[String] = null
            var nrServingSCC2RxBeamRSRP0_0xB97FVal: List[String] = null
            var nrServingSCC2RxBeamRSRP1_0xB97FVal: List[String] = null
            var nrServingSCC3RxBeamRSRP0_0xB97FVal: List[String] = null
            var nrServingSCC3RxBeamRSRP1_0xB97FVal: List[String] = null
            var nrServingSCC4RxBeamRSRP0_0xB97FVal: List[String] = null
            var nrServingSCC4RxBeamRSRP1_0xB97FVal: List[String] = null
            var nrServingSCC5RxBeamRSRP0_0xB97FVal: List[String] = null
            var nrServingSCC5RxBeamRSRP1_0xB97FVal: List[String] = null

            var nrServingPCCRSRP_0xB97FVal: List[String] = null
            var nrServingPCCRSRQ_0xB97FVal: List[String] = null
            var nrServingSCC1RSRP_0xB97FVal: List[String] = null
            var nrServingSCC1RSRQ_0xB97FVal: List[String] = null
            var nrServingSCC2RSRP_0xB97FVal: List[String] = null
            var nrServingSCC2RSRQ_0xB97FVal: List[String] = null
            var nrServingSCC3RSRP_0xB97FVal: List[String] = null
            var nrServingSCC3RSRQ_0xB97FVal: List[String] = null
            var nrServingSCC4RSRP_0xB97FVal: List[String] = null
            var nrServingSCC4RSRQ_0xB97FVal: List[String] = null
            var nrServingSCC5RSRP_0xB97FVal: List[String] = null
            var nrServingSCC5RSRQ_0xB97FVal: List[String] = null

            var nrArfcnPCC_0xB97FVal:Integer = null
            var nrArfcnSCC1_0xB97FVal:Integer = null
            var nrBandPCC_0xB97FVal:Integer = null
            var nrBandSCC1_0xB97FVal:Integer = null
            var nrBandTypePCC_0xB97FVal:String = null
            var nrBandTypeSCC1_0xB97FVal:String = null

            def getPCI(list: List[Dynamic_47487_131075_SR_27574_DTO]) = {
              list.map(_.PCI)
            }

            def getServingRxBeam(index: Int, list: List[Dynamic_47487_131075_ENUMERATION_27591_DTO]): Integer = {
              try {
                Integer.parseInt(list(index).ServingRXBeam)
              } catch {
                case _: NumberFormatException => null
              }
            }

            def getRSRP(index: Int, servingSSB: String, list: List[Dynamic_47487_131075_SR_27574_DTO]) = {
              list.flatMap(_.sr_27579_inst_list.asScala.toList.filter(_.SSBIndex.equals(servingSSB)).flatMap(_.sr_27584_inst_list.asScala.toList.map(_.sr_27626_inst_list.get(index).RSRP)))
            }

            def getCellQualityRSRP(list: List[Dynamic_47487_131075_SR_27574_DTO]) = {
              list.flatMap(_.sr_27604_inst_list.asScala.toList.map(_.CellQualityRSRP).filterNot(_.equals("0.000")))
            }

            def getCellQualityRSRQ(list: List[Dynamic_47487_131075_SR_27574_DTO]) = {
              list.flatMap(_.sr_27605_inst_list.asScala.toList.map(_.CellQualityRSRQ).filterNot(_.equals("0.000")))
            }


            val sr_27567_inst_list = bean47487_131075.sr_27567_inst_list.asScala.toList
            sr_27567_inst_list.foreach {
              case inst if inst.CcIndex.equals("0") =>
                nrServingPCCPCI_0xB97FVal = getPCI(inst.sr_27574_inst_list.asScala.toList)
                nrServingPCCRxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27591_inst_list.asScala.toList)
                nrServingPCCRxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27591_inst_list.asScala.toList)
                nrServingPCCRxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27574_inst_list.asScala.toList)
                nrServingPCCRxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27574_inst_list.asScala.toList)
                nrServingPCCRSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27574_inst_list.asScala.toList)
                nrServingPCCRSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27574_inst_list.asScala.toList)


              case inst if inst.CcIndex.equals("1") =>
                nrServingSCC1PCI_0xB97FVal = getPCI(inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC1RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27591_inst_list.asScala.toList)
                nrServingSCC1RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27591_inst_list.asScala.toList)
                nrServingSCC1RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC1RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC1RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC1RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27574_inst_list.asScala.toList)
              case inst if inst.CcIndex.equals("2") =>
                nrServingSCC2PCI_0xB97FVal = getPCI(inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC2RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27591_inst_list.asScala.toList)
                nrServingSCC2RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27591_inst_list.asScala.toList)
                nrServingSCC2RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC2RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC2RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC2RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27574_inst_list.asScala.toList)
              case inst if inst.CcIndex.equals("3") =>
                nrServingSCC3PCI_0xB97FVal = getPCI(inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC3RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27591_inst_list.asScala.toList)
                nrServingSCC3RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27591_inst_list.asScala.toList)
                nrServingSCC3RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC3RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC3RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC3RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27574_inst_list.asScala.toList)
              case inst if inst.CcIndex.equals("4") =>
                nrServingSCC4PCI_0xB97FVal = getPCI(inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC4RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27591_inst_list.asScala.toList)
                nrServingSCC4RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27591_inst_list.asScala.toList)
                nrServingSCC4RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC4RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC4RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC4RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27574_inst_list.asScala.toList)
              case inst if inst.CcIndex.equals("5") =>
                nrServingSCC5PCI_0xB97FVal = getPCI(inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC5RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27591_inst_list.asScala.toList)
                nrServingSCC5RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27591_inst_list.asScala.toList)
                nrServingSCC5RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC5RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC5RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27574_inst_list.asScala.toList)
                nrServingSCC5RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27574_inst_list.asScala.toList)
            }

            var servingCellIndx = Try(CCIndexVal.map(_.toInt)).getOrElse(null)
            var nrarfcn = Try(NRArfcnVal.map(_.toInt)).getOrElse(null)
            if(nrarfcn.size>=1) {
              nrArfcnPCC_0xB97FVal = nrarfcn(0)
              if (nrArfcnPCC_0xB97FVal != null && servingCellIndx != null) {
                nrBandPCC_0xB97FVal = ParseUtils.getNrBandIndicator(nrArfcnPCC_0xB97FVal)
                if (nrBandPCC_0xB97FVal != null) nrBandTypePCC_0xB97FVal = getNrBandType(nrBandPCC_0xB97FVal)
              }
            }

            if(nrarfcn.size>1 && servingCellIndx.size>1) {
              nrArfcnSCC1_0xB97FVal = nrarfcn(1)
              if (nrArfcnSCC1_0xB97FVal != null && servingCellIndx != null) {
                nrBandSCC1_0xB97FVal = ParseUtils.getNrBandIndicator(nrArfcnSCC1_0xB97FVal)
                if (nrBandSCC1_0xB97FVal != null) nrBandTypeSCC1_0xB97FVal = getNrBandType(nrBandSCC1_0xB97FVal)
              }
            }



            logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSBRSRP_0xB97F = nrServingPCCRSRP_0xB97FVal, NRSBRSRQ_0xB97F = nrServingPCCRSRQ_0xB97FVal, NRBEAMCOUNT_0XB97F = Try(NRBEAMCOUNTVal.map(_.toInt)).getOrElse(null), CCIndex_0xB97F = servingCellIndx,
              NRPCI_0XB97F = Try(NRPCIVal.map(_.toInt)).getOrElse(null), NRServingBeamIndex_0xB97F = Try(NRServingBeamIndexVal.head.toInt.asInstanceOf[Integer]).getOrElse(null), NRServingCellPci_0xB97F = Try(NRServingCellPCIVal.map(_.toInt)).getOrElse(null),
              NRServingPCCPCI_0xB97F = Try(nrServingPCCPCI_0xB97FVal.map(_.toInt)) getOrElse (null), NRServingSCC1PCI_0xB97F = Try(nrServingSCC1PCI_0xB97FVal.map(_.toInt)).getOrElse(null), NRServingSCC2PCI_0xB97F = Try(nrServingSCC2PCI_0xB97FVal.map(_.toInt)).getOrElse(null),
              NRServingSCC3PCI_0xB97F = Try(nrServingSCC3PCI_0xB97FVal.map(_.toInt)) getOrElse (null), NRServingSCC4PCI_0xB97F = Try(nrServingSCC4PCI_0xB97FVal.map(_.toInt)).getOrElse(null), NRServingSCC5PCI_0xB97F = Try(nrServingSCC5PCI_0xB97FVal.map(_.toInt)).getOrElse(null),
              NRServingPCCRxBeamId0_0xB97F = nrServingPCCRxBeamId0_0xB97FVal, NRServingPCCRxBeamId1_0xB97F = nrServingPCCRxBeamId1_0xB97FVal,
              NRServingSCC1RxBeamId0_0xB97F = nrServingSCC1RxBeamId0_0xB97FVal, NRServingSCC1RxBeamId1_0xB97F = nrServingSCC1RxBeamId1_0xB97FVal,
              NRServingSCC2RxBeamId0_0xB97F = nrServingSCC2RxBeamId0_0xB97FVal, NRServingSCC2RxBeamId1_0xB97F = nrServingSCC2RxBeamId1_0xB97FVal,
              NRServingSCC3RxBeamId0_0xB97F = nrServingSCC3RxBeamId0_0xB97FVal, NRServingSCC3RxBeamId1_0xB97F = nrServingSCC3RxBeamId1_0xB97FVal,
              NRServingSCC4RxBeamId0_0xB97F = nrServingSCC4RxBeamId0_0xB97FVal, NRServingSCC4RxBeamId1_0xB97F = nrServingSCC4RxBeamId1_0xB97FVal,
              NRServingSCC5RxBeamId0_0xB97F = nrServingSCC5RxBeamId0_0xB97FVal, NRServingSCC5RxBeamId1_0xB97F = nrServingSCC5RxBeamId1_0xB97FVal,
              NRServingPCCRxBeamRSRP0_0xB97F = Try(nrServingPCCRxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingPCCRxBeamRSRP1_0xB97F = Try(nrServingPCCRxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC1RxBeamRSRP0_0xB97F = Try(nrServingSCC1RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC1RxBeamRSRP1_0xB97F = Try(nrServingSCC1RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC2RxBeamRSRP0_0xB97F = Try(nrServingSCC2RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC2RxBeamRSRP1_0xB97F = Try(nrServingSCC2RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC3RxBeamRSRP0_0xB97F = Try(nrServingSCC3RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC3RxBeamRSRP1_0xB97F = Try(nrServingSCC3RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC4RxBeamRSRP0_0xB97F = Try(nrServingSCC4RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC4RxBeamRSRP1_0xB97F = Try(nrServingSCC4RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC5RxBeamRSRP0_0xB97F = Try(nrServingSCC5RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC5RxBeamRSRP1_0xB97F = Try(nrServingSCC5RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingPCCRSRP_0xB97F = Try(nrServingPCCRSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingPCCRSRQ_0xB97F = Try(nrServingPCCRSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC1RSRP_0xB97F = Try(nrServingSCC1RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC1RSRQ_0xB97F = Try(nrServingSCC1RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC2RSRP_0xB97F = Try(nrServingSCC2RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC2RSRQ_0xB97F = Try(nrServingSCC2RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC3RSRP_0xB97F = Try(nrServingSCC3RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC3RSRQ_0xB97F = Try(nrServingSCC3RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC4RSRP_0xB97F = Try(nrServingSCC4RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC4RSRQ_0xB97F = Try(nrServingSCC4RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC5RSRP_0xB97F = Try(nrServingSCC5RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC5RSRQ_0xB97F = Try(nrServingSCC5RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRBandType_0xB97F = nrBandTypePCC_0xB97FVal,NRBandInd_0xB97F = nrBandPCC_0xB97FVal)
            logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRBandTypeSCC1_0xB97F =  nrBandTypeSCC1_0xB97FVal,NRBandScc1_0xB97F= nrBandSCC1_0xB97FVal,
              NrPCCARFCN_0XB97F = nrArfcnPCC_0xB97FVal, NrSCC1ARFCN_0XB97F= nrArfcnSCC1_0xB97FVal)
            logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47487", hexLogCode = "0xB97F", logRecordQComm5G = logRecordQComm5GObj,logRecordQComm5G2 = logRecordQComm5G2Obj)
          case 131078 =>
            val bean47487_131078 = process47487_131078(x, exceptionUseArray)
            //logger.info("bean47487_131075 -> Bean as JSON>>>>>>>>"+Constants.getJsonFromBean(bean47487_131078))
            val NRSBRSRPVal = bean47487_131078.sr_27414_inst_list.asScala.toList.map(_.sr_27424_inst_list.asScala.toList.map(_.sr_27532_inst_list.asScala.toList.map(_.CellQualityRSRP))).flatten.flatten
            val NRSBRSRQVal = bean47487_131078.sr_27414_inst_list.asScala.toList.map(_.sr_27424_inst_list.asScala.toList.map(_.sr_27540_inst_list.asScala.toList.map(_.CellQualityRSRQ))).flatten.flatten
            val NRPCIVal = bean47487_131078.sr_27414_inst_list.asScala.toList.map(_.sr_27424_inst_list.asScala.toList.map(_.PCI)).flatten
            val NRBEAMCOUNTVal = bean47487_131078.sr_27414_inst_list.asScala.toList.map(_.sr_27424_inst_list.asScala.toList.map(_.NumBeams)).flatten
            val CCIndexVal = bean47487_131078.sr_27414_inst_list.asScala.toList.map(_.ServingCellIndex)
            val NRServingBeamIndexVal = bean47487_131078.sr_27414_inst_list.asScala.toList.map(_.ServingSSB)
            val NRServingCellPCIVal = bean47487_131078.sr_27414_inst_list.asScala.toList.map(_.ServingCellPCI)
            val NRArfcnVal = bean47487_131078.sr_27414_inst_list.asScala.toList.map(_.RasterARFCN)

            var nrServingPCCPCI_0xB97FVal: List[String] = null
            var nrServingSCC1PCI_0xB97FVal: List[String] = null
            var nrServingSCC2PCI_0xB97FVal: List[String] = null
            var nrServingSCC3PCI_0xB97FVal: List[String] = null
            var nrServingSCC4PCI_0xB97FVal: List[String] = null
            var nrServingSCC5PCI_0xB97FVal: List[String] = null

            var nrServingPCCRxBeamId0_0xB97FVal: Integer = null
            var nrServingPCCRxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC1RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC1RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC2RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC2RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC3RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC3RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC4RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC4RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC5RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC5RxBeamId1_0xB97FVal: Integer = null

            var nrServingPCCRxBeamRSRP0_0xB97FVal: List[String] = null
            var nrServingPCCRxBeamRSRP1_0xB97FVal: List[String] = null
            var nrServingSCC1RxBeamRSRP0_0xB97FVal: List[String] = null
            var nrServingSCC1RxBeamRSRP1_0xB97FVal: List[String] = null
            var nrServingSCC2RxBeamRSRP0_0xB97FVal: List[String] = null
            var nrServingSCC2RxBeamRSRP1_0xB97FVal: List[String] = null
            var nrServingSCC3RxBeamRSRP0_0xB97FVal: List[String] = null
            var nrServingSCC3RxBeamRSRP1_0xB97FVal: List[String] = null
            var nrServingSCC4RxBeamRSRP0_0xB97FVal: List[String] = null
            var nrServingSCC4RxBeamRSRP1_0xB97FVal: List[String] = null
            var nrServingSCC5RxBeamRSRP0_0xB97FVal: List[String] = null
            var nrServingSCC5RxBeamRSRP1_0xB97FVal: List[String] = null

            var nrServingPCCRSRP_0xB97FVal: List[String] = null
            var nrServingPCCRSRQ_0xB97FVal: List[String] = null
            var nrServingSCC1RSRP_0xB97FVal: List[String] = null
            var nrServingSCC1RSRQ_0xB97FVal: List[String] = null
            var nrServingSCC2RSRP_0xB97FVal: List[String] = null
            var nrServingSCC2RSRQ_0xB97FVal: List[String] = null
            var nrServingSCC3RSRP_0xB97FVal: List[String] = null
            var nrServingSCC3RSRQ_0xB97FVal: List[String] = null
            var nrServingSCC4RSRP_0xB97FVal: List[String] = null
            var nrServingSCC4RSRQ_0xB97FVal: List[String] = null
            var nrServingSCC5RSRP_0xB97FVal: List[String] = null
            var nrServingSCC5RSRQ_0xB97FVal: List[String] = null

            var nrArfcnPCC_0xB97FVal:Integer = null
            var nrArfcnSCC1_0xB97FVal:Integer = null
            var nrBandPCC_0xB97FVal:Integer = null
            var nrBandSCC1_0xB97FVal:Integer = null
            var nrBandTypePCC_0xB97FVal:String = null
            var nrBandTypeSCC1_0xB97FVal:String = null

            def getPCI(list: List[Dynamic_47487_131078_SR_27424_DTO]) = {
              list.map(_.PCI)
            }

            def getServingRxBeam(index: Int, list: List[Dynamic_47487_131078_ENUMERATION_27527_DTO]): Integer = {
              try {
                Integer.parseInt(list(index).ServingRXBeam)
              } catch {
                case _: NumberFormatException => null
              }
            }

            def getRSRP(index: Int, servingSSB: String, list: List[Dynamic_47487_131078_SR_27424_DTO]) = {
              list.flatMap(_.sr_27427_inst_list.asScala.toList.filter(_.SSBIndex.equals(servingSSB)).flatMap(_.sr_27438_inst_list.asScala.toList.map(_.sr_27544_inst_list.get(index).RSRP)))
            }

            def getCellQualityRSRP(list: List[Dynamic_47487_131078_SR_27424_DTO]) = {
              list.flatMap(_.sr_27532_inst_list.asScala.toList.map(_.CellQualityRSRP).filterNot(_.equals("0.000")))
            }

            def getCellQualityRSRQ(list: List[Dynamic_47487_131078_SR_27424_DTO]) = {
              list.flatMap(_.sr_27540_inst_list.asScala.toList.map(_.CellQualityRSRQ).filterNot(_.equals("0.000")))
            }

            val sr_27414_inst_list = bean47487_131078.sr_27414_inst_list.asScala.toList
            sr_27414_inst_list.foreach {
              case inst if inst.CcIndex.equals("0") =>
                nrServingPCCPCI_0xB97FVal = getPCI(inst.sr_27424_inst_list.asScala.toList)
                nrServingPCCRxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27527_inst_list.asScala.toList)
                nrServingPCCRxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27527_inst_list.asScala.toList)
                nrServingPCCRxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27424_inst_list.asScala.toList)
                nrServingPCCRxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27424_inst_list.asScala.toList)
                nrServingPCCRSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27424_inst_list.asScala.toList)
                nrServingPCCRSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27424_inst_list.asScala.toList)
              case inst if inst.CcIndex.equals("1") =>
                nrServingSCC1PCI_0xB97FVal = getPCI(inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC1RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27527_inst_list.asScala.toList)
                nrServingSCC1RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27527_inst_list.asScala.toList)
                nrServingSCC1RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC1RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC1RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC1RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27424_inst_list.asScala.toList)
              case inst if inst.CcIndex.equals("2") =>
                nrServingSCC2PCI_0xB97FVal = getPCI(inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC2RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27527_inst_list.asScala.toList)
                nrServingSCC2RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27527_inst_list.asScala.toList)
                nrServingSCC2RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC2RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC2RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC2RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27424_inst_list.asScala.toList)
              case inst if inst.CcIndex.equals("3") =>
                nrServingSCC3PCI_0xB97FVal = getPCI(inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC3RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27527_inst_list.asScala.toList)
                nrServingSCC3RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27527_inst_list.asScala.toList)
                nrServingSCC3RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC3RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC3RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC3RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27424_inst_list.asScala.toList)
              case inst if inst.CcIndex.equals("4") =>
                nrServingSCC4PCI_0xB97FVal = getPCI(inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC4RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27527_inst_list.asScala.toList)
                nrServingSCC4RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27527_inst_list.asScala.toList)
                nrServingSCC4RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC4RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC4RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC4RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27424_inst_list.asScala.toList)
              case inst if inst.CcIndex.equals("5") =>
                nrServingSCC5PCI_0xB97FVal = getPCI(inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC5RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27527_inst_list.asScala.toList)
                nrServingSCC5RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27527_inst_list.asScala.toList)
                nrServingSCC5RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC5RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC5RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27424_inst_list.asScala.toList)
                nrServingSCC5RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27424_inst_list.asScala.toList)
            }

            var servingCellIndx = Try(CCIndexVal.map(_.toInt)).getOrElse(null)
            var nrarfcn = Try(NRArfcnVal.map(_.toInt)).getOrElse(null)
            if(nrarfcn.size>=1) {
              nrArfcnPCC_0xB97FVal = nrarfcn(0)
              if (nrArfcnPCC_0xB97FVal != null && servingCellIndx != null) {
                nrBandPCC_0xB97FVal = ParseUtils.getNrBandIndicator(nrArfcnPCC_0xB97FVal)
                if (nrBandPCC_0xB97FVal != null) nrBandTypePCC_0xB97FVal = getNrBandType(nrBandPCC_0xB97FVal)
              }
            }

            if(nrarfcn.size>1 && servingCellIndx.size>1) {
              nrArfcnSCC1_0xB97FVal = nrarfcn(1)
              if (nrArfcnSCC1_0xB97FVal != null && servingCellIndx != null) {
                nrBandSCC1_0xB97FVal = ParseUtils.getNrBandIndicator(nrArfcnSCC1_0xB97FVal)
                if (nrBandSCC1_0xB97FVal != null) nrBandTypeSCC1_0xB97FVal = getNrBandType(nrBandSCC1_0xB97FVal)
              }
            }

            logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSBRSRP_0xB97F = nrServingPCCRSRP_0xB97FVal, NRSBRSRQ_0xB97F = nrServingPCCRSRQ_0xB97FVal, NRBEAMCOUNT_0XB97F = Try(NRBEAMCOUNTVal.map(_.toInt)).getOrElse(null), CCIndex_0xB97F = servingCellIndx,
              NRPCI_0XB97F = Try(NRPCIVal.map(_.toInt)).getOrElse(null), NRServingBeamIndex_0xB97F = Try(NRServingBeamIndexVal.head.toInt.asInstanceOf[Integer]).getOrElse(null), NRServingCellPci_0xB97F = Try(NRServingCellPCIVal.map(_.toInt)).getOrElse(null),
              NRServingPCCPCI_0xB97F = Try(nrServingPCCPCI_0xB97FVal.map(_.toInt)) getOrElse (null), NRServingSCC1PCI_0xB97F = Try(nrServingSCC1PCI_0xB97FVal.map(_.toInt)).getOrElse(null), NRServingSCC2PCI_0xB97F = Try(nrServingSCC2PCI_0xB97FVal.map(_.toInt)).getOrElse(null),
              NRServingSCC3PCI_0xB97F = Try(nrServingSCC3PCI_0xB97FVal.map(_.toInt)) getOrElse (null), NRServingSCC4PCI_0xB97F = Try(nrServingSCC4PCI_0xB97FVal.map(_.toInt)).getOrElse(null), NRServingSCC5PCI_0xB97F = Try(nrServingSCC5PCI_0xB97FVal.map(_.toInt)).getOrElse(null),
              NRServingPCCRxBeamId0_0xB97F = nrServingPCCRxBeamId0_0xB97FVal, NRServingPCCRxBeamId1_0xB97F = nrServingPCCRxBeamId1_0xB97FVal,
              NRServingSCC1RxBeamId0_0xB97F = nrServingSCC1RxBeamId0_0xB97FVal, NRServingSCC1RxBeamId1_0xB97F = nrServingSCC1RxBeamId1_0xB97FVal,
              NRServingSCC2RxBeamId0_0xB97F = nrServingSCC2RxBeamId0_0xB97FVal, NRServingSCC2RxBeamId1_0xB97F = nrServingSCC2RxBeamId1_0xB97FVal,
              NRServingSCC3RxBeamId0_0xB97F = nrServingSCC3RxBeamId0_0xB97FVal, NRServingSCC3RxBeamId1_0xB97F = nrServingSCC3RxBeamId1_0xB97FVal,
              NRServingSCC4RxBeamId0_0xB97F = nrServingSCC4RxBeamId0_0xB97FVal, NRServingSCC4RxBeamId1_0xB97F = nrServingSCC4RxBeamId1_0xB97FVal,
              NRServingSCC5RxBeamId0_0xB97F = nrServingSCC5RxBeamId0_0xB97FVal, NRServingSCC5RxBeamId1_0xB97F = nrServingSCC5RxBeamId1_0xB97FVal,
              NRServingPCCRxBeamRSRP0_0xB97F = Try(nrServingPCCRxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingPCCRxBeamRSRP1_0xB97F = Try(nrServingPCCRxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC1RxBeamRSRP0_0xB97F = Try(nrServingSCC1RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC1RxBeamRSRP1_0xB97F = Try(nrServingSCC1RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC2RxBeamRSRP0_0xB97F = Try(nrServingSCC2RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC2RxBeamRSRP1_0xB97F = Try(nrServingSCC2RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC3RxBeamRSRP0_0xB97F = Try(nrServingSCC3RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC3RxBeamRSRP1_0xB97F = Try(nrServingSCC3RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC4RxBeamRSRP0_0xB97F = Try(nrServingSCC4RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC4RxBeamRSRP1_0xB97F = Try(nrServingSCC4RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC5RxBeamRSRP0_0xB97F = Try(nrServingSCC5RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC5RxBeamRSRP1_0xB97F = Try(nrServingSCC5RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingPCCRSRP_0xB97F = Try(nrServingPCCRSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingPCCRSRQ_0xB97F = Try(nrServingPCCRSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC1RSRP_0xB97F = Try(nrServingSCC1RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC1RSRQ_0xB97F = Try(nrServingSCC1RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC2RSRP_0xB97F = Try(nrServingSCC2RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC2RSRQ_0xB97F = Try(nrServingSCC2RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC3RSRP_0xB97F = Try(nrServingSCC3RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC3RSRQ_0xB97F = Try(nrServingSCC3RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC4RSRP_0xB97F = Try(nrServingSCC4RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC4RSRQ_0xB97F = Try(nrServingSCC4RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC5RSRP_0xB97F = Try(nrServingSCC5RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC5RSRQ_0xB97F = Try(nrServingSCC5RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRBandType_0xB97F = nrBandTypePCC_0xB97FVal,NRBandInd_0xB97F = nrBandPCC_0xB97FVal)
            logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRBandTypeSCC1_0xB97F =  nrBandTypeSCC1_0xB97FVal,NRBandScc1_0xB97F= nrBandSCC1_0xB97FVal,
              NrPCCARFCN_0XB97F = nrArfcnPCC_0xB97FVal, NrSCC1ARFCN_0XB97F= nrArfcnSCC1_0xB97FVal)
            logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47487", hexLogCode = "0xB97F", logRecordQComm5G = logRecordQComm5GObj,logRecordQComm5G2 = logRecordQComm5G2Obj)
          case 131079 =>
            val bean47487_131079 = process47487_131079(x, exceptionUseArray)
            //("bean47487_131079 -> Bean as JSON>>>>>>>>" + Constants.getJsonFromBean(bean47487_131079))
            val NRSBRSRPVal = bean47487_131079.sr_27767_inst_list.asScala.toList.map(_.sr_27774_inst_list.asScala.toList.map(_.sr_27804_inst_list.asScala.toList.map(_.CellQualityRSRP))).flatten.flatten
            val NRSBRSRQVal = bean47487_131079.sr_27767_inst_list.asScala.toList.map(_.sr_27774_inst_list.asScala.toList.map(_.sr_27805_inst_list.asScala.toList.map(_.CellQualityRSRQ))).flatten.flatten
            val NRPCIVal = bean47487_131079.sr_27767_inst_list.asScala.toList.map(_.sr_27774_inst_list.asScala.toList.map(_.PCI)).flatten
            val NRBEAMCOUNTVal = bean47487_131079.sr_27767_inst_list.asScala.toList.map(_.sr_27774_inst_list.asScala.toList.map(_.NumBeams)).flatten
            val CCIndexVal = bean47487_131079.sr_27767_inst_list.asScala.toList.map(_.ServingCellIndex)
            val NRServingBeamIndexVal = bean47487_131079.sr_27767_inst_list.asScala.toList.map(_.ServingSSB)
            val NRServingCellPCIVal = bean47487_131079.sr_27767_inst_list.asScala.toList.map(_.ServingCellPCI)
            val NRArfcnVal = bean47487_131079.sr_27767_inst_list.asScala.toList.map(_.RasterARFCN)

            var nrServingPCCPCI_0xB97FVal: List[String] = null
            var nrServingSCC1PCI_0xB97FVal: List[String] = null
            var nrServingSCC2PCI_0xB97FVal: List[String] = null
            var nrServingSCC3PCI_0xB97FVal: List[String] = null
            var nrServingSCC4PCI_0xB97FVal: List[String] = null
            var nrServingSCC5PCI_0xB97FVal: List[String] = null

            var nrServingPCCRxBeamId0_0xB97FVal: Integer = null
            var nrServingPCCRxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC1RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC1RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC2RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC2RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC3RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC3RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC4RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC4RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC5RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC5RxBeamId1_0xB97FVal: Integer = null

            var nrServingPCCRxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingPCCRxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC1RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC1RxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC2RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC2RxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC3RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC3RxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC4RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC4RxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC5RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC5RxBeamRSRP1_0xB97FVal: List[String] = List[String]()

            var nrServingPCCRSRP_0xB97FVal: List[String] = List[String]()
            var nrServingPCCRSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC1RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC1RSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC2RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC2RSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC3RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC3RSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC4RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC4RSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC5RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC5RSRQ_0xB97FVal: List[String] = List[String]()

            var nrArfcnPCC_0xB97FVal:Integer = null
            var nrArfcnSCC1_0xB97FVal:Integer = null
            var nrBandPCC_0xB97FVal:Integer = null
            var nrBandSCC1_0xB97FVal:Integer = null
            var nrBandTypePCC_0xB97FVal:String = null
            var nrBandTypeSCC1_0xB97FVal:String = null

            def getPCI(list: List[Dynamic_47487_131079_SR_27774_DTO]) = {
              list.map(_.PCI)
            }

            def getServingRxBeam(index: Int, list: List[Dynamic_47487_131079_ENUMERATION_27791_DTO]): Integer = {
              try {
                Integer.parseInt(list(index).ServingRXBeam)
              } catch {
                case _: NumberFormatException => null
              }
            }

            def getRSRP(index: Int, servingSSB: String, list: List[Dynamic_47487_131079_SR_27774_DTO]) = {
              list.flatMap(_.sr_27779_inst_list.asScala.toList.filter(_.SSBIndex.equals(servingSSB)).flatMap(_.sr_27784_inst_list.asScala.toList.map(_.sr_27826_inst_list.get(index).RSRP)))
            }

            def getCellQualityRSRP(list: List[Dynamic_47487_131079_SR_27774_DTO]) = {
              list.flatMap(_.sr_27804_inst_list.asScala.toList.map(_.CellQualityRSRP).filterNot(_.equals("0.000")))
            }

            def getCellQualityRSRQ(list: List[Dynamic_47487_131079_SR_27774_DTO]) = {
              list.flatMap(_.sr_27805_inst_list.asScala.toList.map(_.CellQualityRSRQ).filterNot(_.equals("0.000")))
            }

            val sr_27767_inst_list = bean47487_131079.sr_27767_inst_list.asScala.toList
            sr_27767_inst_list.foreach {
              case inst if inst.CcIndex.equals("0") =>
                nrServingPCCPCI_0xB97FVal = getPCI(inst.sr_27774_inst_list.asScala.toList)
                //logger.info("nrServingPCCPCI_0xB97FVal >>>>>>>>" + nrServingPCCPCI_0xB97FVal)
                nrServingPCCRxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27791_inst_list.asScala.toList)
                nrServingPCCRxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27791_inst_list.asScala.toList)
                nrServingPCCRxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27774_inst_list.asScala.toList)
                //              logger.info("nrServingPCCRxBeamRSRP0_0xB97FVal >>>>>>>>" + nrServingPCCRxBeamRSRP0_0xB97FVal)
                nrServingPCCRxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27774_inst_list.asScala.toList)
                //              logger.info("nrServingPCCRxBeamRSRP1_0xB97FVal >>>>>>>>" + nrServingPCCRxBeamRSRP1_0xB97FVal)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingPCCRSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27774_inst_list.asScala.toList)
                //                logger.info("nrServingPCCRSRP_0xB97FVal >>>>>>>>" + nrServingPCCRSRP_0xB97FVal)
                nrServingPCCRSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27774_inst_list.asScala.toList)
              //                logger.info("nrServingPCCRSRQ_0xB97FVal >>>>>>>>" + nrServingPCCRSRQ_0xB97FVal)
              //              }
              case inst if inst.CcIndex.equals("1") =>
                nrServingSCC1PCI_0xB97FVal = getPCI(inst.sr_27774_inst_list.asScala.toList)
                //              logger.info("nrServingSCC1PCI_0xB97FVal >>>>>>>>" + nrServingSCC1PCI_0xB97FVal)
                nrServingSCC1RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27791_inst_list.asScala.toList)
                nrServingSCC1RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27791_inst_list.asScala.toList)
                nrServingSCC1RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27774_inst_list.asScala.toList)
                nrServingSCC1RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27774_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC1RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27774_inst_list.asScala.toList)
                nrServingSCC1RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27774_inst_list.asScala.toList)
              //              }
              case inst if inst.CcIndex.equals("2") =>
                nrServingSCC2PCI_0xB97FVal = getPCI(inst.sr_27774_inst_list.asScala.toList)
                //              logger.info("nrServingSCC2PCI_0xB97FVal >>>>>>>>" + nrServingSCC2PCI_0xB97FVal)
                nrServingSCC2RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27791_inst_list.asScala.toList)
                nrServingSCC2RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27791_inst_list.asScala.toList)
                nrServingSCC2RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27774_inst_list.asScala.toList)
                nrServingSCC2RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27774_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC2RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27774_inst_list.asScala.toList)
                nrServingSCC2RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27774_inst_list.asScala.toList)
              //              }
              case inst if inst.CcIndex.equals("3") =>
                nrServingSCC3PCI_0xB97FVal = getPCI(inst.sr_27774_inst_list.asScala.toList)
                //              logger.info("nrServingSCC3PCI_0xB97FVal >>>>>>>>" + nrServingSCC3PCI_0xB97FVal)
                nrServingSCC3RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27791_inst_list.asScala.toList)
                nrServingSCC3RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27791_inst_list.asScala.toList)
                nrServingSCC3RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27774_inst_list.asScala.toList)
                nrServingSCC3RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27774_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC3RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27774_inst_list.asScala.toList)
                nrServingSCC3RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27774_inst_list.asScala.toList)
              //              }
              case inst if inst.CcIndex.equals("4") =>
                nrServingSCC4PCI_0xB97FVal = getPCI(inst.sr_27774_inst_list.asScala.toList)
                //              logger.info("nrServingSCC4PCI_0xB97FVal >>>>>>>>" + nrServingSCC4PCI_0xB97FVal)
                nrServingSCC4RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27791_inst_list.asScala.toList)
                nrServingSCC4RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27791_inst_list.asScala.toList)
                nrServingSCC4RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27774_inst_list.asScala.toList)
                nrServingSCC4RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27774_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC4RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27774_inst_list.asScala.toList)
                nrServingSCC4RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27774_inst_list.asScala.toList)
              //              }
              case inst if inst.CcIndex.equals("5") =>
                nrServingSCC5PCI_0xB97FVal = getPCI(inst.sr_27774_inst_list.asScala.toList)
                //              logger.info("nrServingSCC5PCI_0xB97FVal >>>>>>>>" + nrServingSCC5PCI_0xB97FVal)
                nrServingSCC5RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_27791_inst_list.asScala.toList)
                nrServingSCC5RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_27791_inst_list.asScala.toList)
                nrServingSCC5RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_27774_inst_list.asScala.toList)
                nrServingSCC5RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_27774_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC5RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_27774_inst_list.asScala.toList)
                nrServingSCC5RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_27774_inst_list.asScala.toList)
              //              }
              case ccIndex =>
                logger.info(s"Ignoring ccIndex=$ccIndex for logCode=$logCode and logVersion=$logVersion")
            }

            var servingCellIndx = Try(CCIndexVal.map(_.toInt)).getOrElse(null)
            var nrarfcn = Try(NRArfcnVal.map(_.toInt)).getOrElse(null)
            if(nrarfcn.size>=1) {
              nrArfcnPCC_0xB97FVal = nrarfcn(0)
              if (nrArfcnPCC_0xB97FVal != null && servingCellIndx != null) {
                nrBandPCC_0xB97FVal = ParseUtils.getNrBandIndicator(nrArfcnPCC_0xB97FVal)
                if (nrBandPCC_0xB97FVal != null) nrBandTypePCC_0xB97FVal = getNrBandType(nrBandPCC_0xB97FVal)
              }
            }

            if(nrarfcn.size>1 && servingCellIndx.size>1) {
              nrArfcnSCC1_0xB97FVal = nrarfcn(1)
              if (nrArfcnSCC1_0xB97FVal != null && servingCellIndx != null) {
                nrBandSCC1_0xB97FVal = ParseUtils.getNrBandIndicator(nrArfcnSCC1_0xB97FVal)
                if (nrBandSCC1_0xB97FVal != null) nrBandTypeSCC1_0xB97FVal = getNrBandType(nrBandSCC1_0xB97FVal)
              }
            }

            logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSBRSRP_0xB97F = nrServingPCCRSRP_0xB97FVal, NRSBRSRQ_0xB97F = nrServingPCCRSRQ_0xB97FVal, NRBEAMCOUNT_0XB97F = Try(NRBEAMCOUNTVal.map(_.toInt)).getOrElse(null), CCIndex_0xB97F = servingCellIndx,
              NRPCI_0XB97F = Try(NRPCIVal.map(_.toInt)).getOrElse(null), NRServingBeamIndex_0xB97F = Try(NRServingBeamIndexVal.head.toInt.asInstanceOf[Integer]).getOrElse(null), NRServingCellPci_0xB97F = Try(NRServingCellPCIVal.map(_.toInt)).getOrElse(null),
              NRServingPCCPCI_0xB97F = Try(nrServingPCCPCI_0xB97FVal.map(_.toInt)) getOrElse (null), NRServingSCC1PCI_0xB97F = Try(nrServingSCC1PCI_0xB97FVal.map(_.toInt)).getOrElse(null), NRServingSCC2PCI_0xB97F = Try(nrServingSCC2PCI_0xB97FVal.map(_.toInt)).getOrElse(null),
              NRServingSCC3PCI_0xB97F = Try(nrServingSCC3PCI_0xB97FVal.map(_.toInt)) getOrElse (null), NRServingSCC4PCI_0xB97F = Try(nrServingSCC4PCI_0xB97FVal.map(_.toInt)).getOrElse(null), NRServingSCC5PCI_0xB97F = Try(nrServingSCC5PCI_0xB97FVal.map(_.toInt)).getOrElse(null),
              NRServingPCCRxBeamId0_0xB97F = nrServingPCCRxBeamId0_0xB97FVal, NRServingPCCRxBeamId1_0xB97F = nrServingPCCRxBeamId1_0xB97FVal,
              NRServingSCC1RxBeamId0_0xB97F = nrServingSCC1RxBeamId0_0xB97FVal, NRServingSCC1RxBeamId1_0xB97F = nrServingSCC1RxBeamId1_0xB97FVal,
              NRServingSCC2RxBeamId0_0xB97F = nrServingSCC2RxBeamId0_0xB97FVal, NRServingSCC2RxBeamId1_0xB97F = nrServingSCC2RxBeamId1_0xB97FVal,
              NRServingSCC3RxBeamId0_0xB97F = nrServingSCC3RxBeamId0_0xB97FVal, NRServingSCC3RxBeamId1_0xB97F = nrServingSCC3RxBeamId1_0xB97FVal,
              NRServingSCC4RxBeamId0_0xB97F = nrServingSCC4RxBeamId0_0xB97FVal, NRServingSCC4RxBeamId1_0xB97F = nrServingSCC4RxBeamId1_0xB97FVal,
              NRServingSCC5RxBeamId0_0xB97F = nrServingSCC5RxBeamId0_0xB97FVal, NRServingSCC5RxBeamId1_0xB97F = nrServingSCC5RxBeamId1_0xB97FVal,
              NRServingPCCRxBeamRSRP0_0xB97F = Try(nrServingPCCRxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingPCCRxBeamRSRP1_0xB97F = Try(nrServingPCCRxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC1RxBeamRSRP0_0xB97F = Try(nrServingSCC1RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC1RxBeamRSRP1_0xB97F = Try(nrServingSCC1RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC2RxBeamRSRP0_0xB97F = Try(nrServingSCC2RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC2RxBeamRSRP1_0xB97F = Try(nrServingSCC2RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC3RxBeamRSRP0_0xB97F = Try(nrServingSCC3RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC3RxBeamRSRP1_0xB97F = Try(nrServingSCC3RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC4RxBeamRSRP0_0xB97F = Try(nrServingSCC4RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC4RxBeamRSRP1_0xB97F = Try(nrServingSCC4RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC5RxBeamRSRP0_0xB97F = Try(nrServingSCC5RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC5RxBeamRSRP1_0xB97F = Try(nrServingSCC5RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingPCCRSRP_0xB97F = Try(nrServingPCCRSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingPCCRSRQ_0xB97F = Try(nrServingPCCRSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC1RSRP_0xB97F = Try(nrServingSCC1RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC1RSRQ_0xB97F = Try(nrServingSCC1RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC2RSRP_0xB97F = Try(nrServingSCC2RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC2RSRQ_0xB97F = Try(nrServingSCC2RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC3RSRP_0xB97F = Try(nrServingSCC3RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC3RSRQ_0xB97F = Try(nrServingSCC3RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC4RSRP_0xB97F = Try(nrServingSCC4RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC4RSRQ_0xB97F = Try(nrServingSCC4RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC5RSRP_0xB97F = Try(nrServingSCC5RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC5RSRQ_0xB97F = Try(nrServingSCC5RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRBandType_0xB97F = nrBandTypePCC_0xB97FVal,NRBandInd_0xB97F = nrBandPCC_0xB97FVal)
            logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRBandTypeSCC1_0xB97F =  nrBandTypeSCC1_0xB97FVal,NRBandScc1_0xB97F= nrBandSCC1_0xB97FVal,
              NrPCCARFCN_0XB97F = nrArfcnPCC_0xB97FVal, NrSCC1ARFCN_0XB97F= nrArfcnSCC1_0xB97FVal)
            //logger.info("logRecordQComm5GObj >>>>>>>> " + logRecordQComm5GObj)
            logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47487", hexLogCode = "0xB97F", logRecordQComm5G = logRecordQComm5GObj,logRecordQComm5G2 = logRecordQComm5G2Obj)
          case 131080 =>
            val bean47487_131080 = process47487_131080(x, exceptionUseArray)
            //logger.info("bean47487_131079 -> Bean as JSON>>>>>>>>" + Constants.getJsonFromBean(bean47487_131080))
            val NRSBRSRPVal = bean47487_131080.sr_48609_inst_list.asScala.toList.map(_.sr_48626_inst_list.asScala.toList.map(_.sr_48631_inst_list.asScala.toList.map(_.CellQualityRSRP))).flatten.flatten
            val NRSBRSRQVal = bean47487_131080.sr_48609_inst_list.asScala.toList.map(_.sr_48626_inst_list.asScala.toList.map(_.sr_48632_inst_list.asScala.toList.map(_.CellQualityRSRQ))).flatten.flatten
            val NRPCIVal = bean47487_131080.sr_48609_inst_list.asScala.toList.map(_.sr_48626_inst_list.asScala.toList.map(_.PCI0)).flatten
            val NRBEAMCOUNTVal = bean47487_131080.sr_48609_inst_list.asScala.toList.map(_.sr_48626_inst_list.asScala.toList.map(_.NumBeams)).flatten
            val CCIndexVal = bean47487_131080.sr_48609_inst_list.asScala.toList.map(_.ServingCellIndex)
            val NRServingBeamIndexVal = bean47487_131080.sr_48609_inst_list.asScala.toList.map(_.ServingSSB)
            val NRServingCellPCIVal = bean47487_131080.sr_48609_inst_list.asScala.toList.map(_.ServingCellPCI)
            val NRArfcnVal = bean47487_131080.sr_48609_inst_list.asScala.toList.map(_.RasterARFCN)

            var nrServingPCCPCI_0xB97FVal: List[String] = null
            var nrServingSCC1PCI_0xB97FVal: List[String] = null
            var nrServingSCC2PCI_0xB97FVal: List[String] = null
            var nrServingSCC3PCI_0xB97FVal: List[String] = null
            var nrServingSCC4PCI_0xB97FVal: List[String] = null
            var nrServingSCC5PCI_0xB97FVal: List[String] = null

            var nrServingPCCRxBeamId0_0xB97FVal: Integer = null
            var nrServingPCCRxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC1RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC1RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC2RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC2RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC3RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC3RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC4RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC4RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC5RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC5RxBeamId1_0xB97FVal: Integer = null

            var nrServingPCCRxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingPCCRxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC1RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC1RxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC2RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC2RxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC3RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC3RxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC4RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC4RxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC5RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC5RxBeamRSRP1_0xB97FVal: List[String] = List[String]()

            var nrServingPCCRSRP_0xB97FVal: List[String] = List[String]()
            var nrServingPCCRSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC1RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC1RSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC2RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC2RSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC3RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC3RSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC4RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC4RSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC5RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC5RSRQ_0xB97FVal: List[String] = List[String]()

            var nrArfcnPCC_0xB97FVal:Integer = null
            var nrArfcnSCC1_0xB97FVal:Integer = null
            var nrBandPCC_0xB97FVal:Integer = null
            var nrBandSCC1_0xB97FVal:Integer = null
            var nrBandTypePCC_0xB97FVal:String = null
            var nrBandTypeSCC1_0xB97FVal:String = null

            def getPCI(list: List[Dynamic_47487_131080_SR_48626_DTO]) = {
              list.map(_.PCI0)
            }

            def getServingRxBeam(index: Int, list: List[Dynamic_47487_131080_ENUMERATION_48620_DTO]): Integer = {
              try {
                Integer.parseInt(list(index).ServingRXBeam)
              } catch {
                case _: NumberFormatException => null
              }
            }

            def getRSRP(index: Int, servingSSB: String, list: List[Dynamic_47487_131080_SR_48626_DTO]) = {
              list.flatMap(_.sr_48633_inst_list.asScala.toList.filter(_.SSBIndex.equals(servingSSB)).flatMap(_.sr_48642_inst_list.asScala.toList.map(_.sr_48646_inst_list.get(index).RSRP)))
            }

            def getCellQualityRSRP(list: List[Dynamic_47487_131080_SR_48626_DTO]) = {
              list.flatMap(_.sr_48631_inst_list.asScala.toList.map(_.CellQualityRSRP).filterNot(_.equals("0.000")))
            }

            def getCellQualityRSRQ(list: List[Dynamic_47487_131080_SR_48626_DTO]) = {
              list.flatMap(_.sr_48632_inst_list.asScala.toList.map(_.CellQualityRSRQ).filterNot(_.equals("0.000")))
            }

            val sr_48609_inst_list = bean47487_131080.sr_48609_inst_list.asScala.toList
            sr_48609_inst_list.foreach {
              case inst if inst.CcIndex.equals("0") =>

                nrServingPCCPCI_0xB97FVal = getPCI(inst.sr_48626_inst_list.asScala.toList)
                //              logger.info("nrServingPCCPCI_0xB97FVal >>>>>>>>" + nrServingPCCPCI_0xB97FVal)
                nrServingPCCRxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_48620_list.asScala.toList)
                nrServingPCCRxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_48620_list.asScala.toList)
                nrServingPCCRxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_48626_inst_list.asScala.toList)
                //              logger.info("nrServingPCCRxBeamRSRP0_0xB97FVal >>>>>>>>" + nrServingPCCRxBeamRSRP0_0xB97FVal)
                nrServingPCCRxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_48626_inst_list.asScala.toList)
                //              logger.info("nrServingPCCRxBeamRSRP1_0xB97FVal >>>>>>>>" + nrServingPCCRxBeamRSRP1_0xB97FVal)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingPCCRSRP_0xB97FVal = getCellQualityRSRP(inst.sr_48626_inst_list.asScala.toList)
                //                logger.info("nrServingPCCRSRP_0xB97FVal >>>>>>>>" + nrServingPCCRSRP_0xB97FVal)
                nrServingPCCRSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_48626_inst_list.asScala.toList)
              //                logger.info("nrServingPCCRSRQ_0xB97FVal >>>>>>>>" + nrServingPCCRSRQ_0xB97FVal)
              //              }
              case inst if inst.CcIndex.equals("1") =>
                nrServingSCC1PCI_0xB97FVal = getPCI(inst.sr_48626_inst_list.asScala.toList)
                //              logger.info("nrServingSCC1PCI_0xB97FVal >>>>>>>>" + nrServingSCC1PCI_0xB97FVal)
                nrServingSCC1RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_48620_list.asScala.toList)
                nrServingSCC1RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_48620_list.asScala.toList)
                nrServingSCC1RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_48626_inst_list.asScala.toList)
                nrServingSCC1RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_48626_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC1RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_48626_inst_list.asScala.toList)
                nrServingSCC1RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_48626_inst_list.asScala.toList)
              //              }
              case inst if inst.CcIndex.equals("2") =>
                nrServingSCC2PCI_0xB97FVal = getPCI(inst.sr_48626_inst_list.asScala.toList)
                //              logger.info("nrServingSCC2PCI_0xB97FVal >>>>>>>>" + nrServingSCC2PCI_0xB97FVal)
                nrServingSCC2RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_48620_list.asScala.toList)
                nrServingSCC2RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_48620_list.asScala.toList)
                nrServingSCC2RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_48626_inst_list.asScala.toList)
                nrServingSCC2RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_48626_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC2RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_48626_inst_list.asScala.toList)
                nrServingSCC2RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_48626_inst_list.asScala.toList)
              //              }
              case inst if inst.CcIndex.equals("3") =>
                nrServingSCC3PCI_0xB97FVal = getPCI(inst.sr_48626_inst_list.asScala.toList)
                //              logger.info("nrServingSCC3PCI_0xB97FVal >>>>>>>>" + nrServingSCC3PCI_0xB97FVal)
                nrServingSCC3RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_48620_list.asScala.toList)
                nrServingSCC3RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_48620_list.asScala.toList)
                nrServingSCC3RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_48626_inst_list.asScala.toList)
                nrServingSCC3RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_48626_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC3RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_48626_inst_list.asScala.toList)
                nrServingSCC3RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_48626_inst_list.asScala.toList)
              //              }
              case inst if inst.CcIndex.equals("4") =>
                nrServingSCC4PCI_0xB97FVal = getPCI(inst.sr_48626_inst_list.asScala.toList)
                //              logger.info("nrServingSCC4PCI_0xB97FVal >>>>>>>>" + nrServingSCC4PCI_0xB97FVal)
                nrServingSCC4RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_48620_list.asScala.toList)
                nrServingSCC4RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_48620_list.asScala.toList)
                nrServingSCC4RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_48626_inst_list.asScala.toList)
                nrServingSCC4RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_48626_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC4RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_48626_inst_list.asScala.toList)
                nrServingSCC4RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_48626_inst_list.asScala.toList)
              //              }
              case inst if inst.CcIndex.equals("5") =>
                nrServingSCC5PCI_0xB97FVal = getPCI(inst.sr_48626_inst_list.asScala.toList)
                //              logger.info("nrServingSCC5PCI_0xB97FVal >>>>>>>>" + nrServingSCC5PCI_0xB97FVal)
                nrServingSCC5RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_48620_list.asScala.toList)
                nrServingSCC5RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_48620_list.asScala.toList)
                nrServingSCC5RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_48626_inst_list.asScala.toList)
                nrServingSCC5RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_48626_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC5RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_48626_inst_list.asScala.toList)
                nrServingSCC5RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_48626_inst_list.asScala.toList)
              //              }
              case ccIndex =>
                logger.info(s"Ignoring ccIndex=$ccIndex for logCode=$logCode and logVersion=$logVersion")
            }

            var servingCellIndx = Try(CCIndexVal.map(_.toInt)).getOrElse(null)
            var nrarfcn = Try(NRArfcnVal.map(_.toInt)).getOrElse(null)
            if(nrarfcn.size>=1) {
              nrArfcnPCC_0xB97FVal = nrarfcn(0)
              if (nrArfcnPCC_0xB97FVal != null && servingCellIndx != null) {
                nrBandPCC_0xB97FVal = ParseUtils.getNrBandIndicator(nrArfcnPCC_0xB97FVal)
                if (nrBandPCC_0xB97FVal != null) nrBandTypePCC_0xB97FVal = getNrBandType(nrBandPCC_0xB97FVal)
              }
            }

            if(nrarfcn.size>1 && servingCellIndx.size>1) {
              nrArfcnSCC1_0xB97FVal = nrarfcn(1)
              if (nrArfcnSCC1_0xB97FVal != null && servingCellIndx != null) {
                nrBandSCC1_0xB97FVal = ParseUtils.getNrBandIndicator(nrArfcnSCC1_0xB97FVal)
                if (nrBandSCC1_0xB97FVal != null) nrBandTypeSCC1_0xB97FVal = getNrBandType(nrBandSCC1_0xB97FVal)
              }
            }

            logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSBRSRP_0xB97F = nrServingPCCRSRP_0xB97FVal, NRSBRSRQ_0xB97F = nrServingPCCRSRQ_0xB97FVal, NRBEAMCOUNT_0XB97F = Try(NRBEAMCOUNTVal.map(_.toInt)).getOrElse(null), CCIndex_0xB97F = servingCellIndx,
              NRPCI_0XB97F = Try(NRPCIVal.map(_.toInt)).getOrElse(null), NRServingBeamIndex_0xB97F = Try(NRServingBeamIndexVal.head.toInt.asInstanceOf[Integer]).getOrElse(null), NRServingCellPci_0xB97F = Try(NRServingCellPCIVal.map(_.toInt)).getOrElse(null),
              NRServingPCCPCI_0xB97F = Try(nrServingPCCPCI_0xB97FVal.map(_.toInt)) getOrElse (null), NRServingSCC1PCI_0xB97F = Try(nrServingSCC1PCI_0xB97FVal.map(_.toInt)).getOrElse(null), NRServingSCC2PCI_0xB97F = Try(nrServingSCC2PCI_0xB97FVal.map(_.toInt)).getOrElse(null),
              NRServingSCC3PCI_0xB97F = Try(nrServingSCC3PCI_0xB97FVal.map(_.toInt)) getOrElse (null), NRServingSCC4PCI_0xB97F = Try(nrServingSCC4PCI_0xB97FVal.map(_.toInt)).getOrElse(null), NRServingSCC5PCI_0xB97F = Try(nrServingSCC5PCI_0xB97FVal.map(_.toInt)).getOrElse(null),
              NRServingPCCRxBeamId0_0xB97F = nrServingPCCRxBeamId0_0xB97FVal, NRServingPCCRxBeamId1_0xB97F = nrServingPCCRxBeamId1_0xB97FVal,
              NRServingSCC1RxBeamId0_0xB97F = nrServingSCC1RxBeamId0_0xB97FVal, NRServingSCC1RxBeamId1_0xB97F = nrServingSCC1RxBeamId1_0xB97FVal, NRServingSCC2RxBeamId0_0xB97F = nrServingSCC2RxBeamId0_0xB97FVal, NRServingSCC2RxBeamId1_0xB97F = nrServingSCC2RxBeamId1_0xB97FVal,
              NRServingSCC3RxBeamId0_0xB97F = nrServingSCC3RxBeamId0_0xB97FVal, NRServingSCC3RxBeamId1_0xB97F = nrServingSCC3RxBeamId1_0xB97FVal,
              NRServingSCC4RxBeamId0_0xB97F = nrServingSCC4RxBeamId0_0xB97FVal, NRServingSCC4RxBeamId1_0xB97F = nrServingSCC4RxBeamId1_0xB97FVal,
              NRServingSCC5RxBeamId0_0xB97F = nrServingSCC5RxBeamId0_0xB97FVal, NRServingSCC5RxBeamId1_0xB97F = nrServingSCC5RxBeamId1_0xB97FVal,
              NRServingPCCRxBeamRSRP0_0xB97F = Try(nrServingPCCRxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingPCCRxBeamRSRP1_0xB97F = Try(nrServingPCCRxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC1RxBeamRSRP0_0xB97F = Try(nrServingSCC1RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC1RxBeamRSRP1_0xB97F = Try(nrServingSCC1RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC2RxBeamRSRP0_0xB97F = Try(nrServingSCC2RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC2RxBeamRSRP1_0xB97F = Try(nrServingSCC2RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC3RxBeamRSRP0_0xB97F = Try(nrServingSCC3RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC3RxBeamRSRP1_0xB97F = Try(nrServingSCC3RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC4RxBeamRSRP0_0xB97F = Try(nrServingSCC4RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC4RxBeamRSRP1_0xB97F = Try(nrServingSCC4RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC5RxBeamRSRP0_0xB97F = Try(nrServingSCC5RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC5RxBeamRSRP1_0xB97F = Try(nrServingSCC5RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingPCCRSRP_0xB97F = Try(nrServingPCCRSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingPCCRSRQ_0xB97F = Try(nrServingPCCRSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC1RSRP_0xB97F = Try(nrServingSCC1RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC1RSRQ_0xB97F = Try(nrServingSCC1RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC2RSRP_0xB97F = Try(nrServingSCC2RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC2RSRQ_0xB97F = Try(nrServingSCC2RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC3RSRP_0xB97F = Try(nrServingSCC3RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC3RSRQ_0xB97F = Try(nrServingSCC3RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC4RSRP_0xB97F = Try(nrServingSCC4RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC4RSRQ_0xB97F = Try(nrServingSCC4RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC5RSRP_0xB97F = Try(nrServingSCC5RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC5RSRQ_0xB97F = Try(nrServingSCC5RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRBandType_0xB97F = nrBandTypePCC_0xB97FVal,NRBandInd_0xB97F = nrBandPCC_0xB97FVal)
            logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRBandTypeSCC1_0xB97F =  nrBandTypeSCC1_0xB97FVal,NRBandScc1_0xB97F= nrBandSCC1_0xB97FVal,
              NrPCCARFCN_0XB97F = nrArfcnPCC_0xB97FVal, NrSCC1ARFCN_0XB97F= nrArfcnSCC1_0xB97FVal)
            logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47487", hexLogCode = "0xB97F", logRecordQComm5G = logRecordQComm5GObj,logRecordQComm5G2 = logRecordQComm5G2Obj)
          case 131081 =>
            val bean47487_131081 = process47487_131081(x, exceptionUseArray)
            //logger.info("bean47487_131081 -> Bean as JSON>>>>>>>>" + Constants.getJsonFromBean(bean47487_131081))
            val NRSBRSRPVal = bean47487_131081.sr_57583_inst_list.asScala.toList.flatMap(_.sr_57542_inst_list.asScala.toList.flatMap(_.sr_57547_inst_list.asScala.toList.map(_.CellQualityRSRP)))
            val NRSBRSRQVal = bean47487_131081.sr_57583_inst_list.asScala.toList.flatMap(_.sr_57542_inst_list.asScala.toList.flatMap(_.sr_57573_inst_list.asScala.toList.map(_.CellQualityRSRQ)))
            val NRPCIVal = bean47487_131081.sr_57583_inst_list.asScala.toList.flatMap(_.sr_57542_inst_list.asScala.toList.map(_.PCI))
            val NRBEAMCOUNTVal = bean47487_131081.sr_57583_inst_list.asScala.toList.flatMap(_.sr_57542_inst_list.asScala.toList.map(_.NumBeams))
            val CCIndexVal = bean47487_131081.sr_57583_inst_list.asScala.toList.map(_.ServingCellIndex)
            val NRServingBeamIndexVal = bean47487_131081.sr_57583_inst_list.asScala.toList.map(_.ServingSSB)
            val NRServingCellPCIVal = bean47487_131081.sr_57583_inst_list.asScala.toList.map(_.ServingCellPCI)
            val NRArfcnVal = bean47487_131081.sr_57583_inst_list.asScala.toList.map(_.RasterARFCN)

            var nrServingPCCPCI_0xB97FVal: List[String] = null
            var nrServingSCC1PCI_0xB97FVal: List[String] = null
            var nrServingSCC2PCI_0xB97FVal: List[String] = null
            var nrServingSCC3PCI_0xB97FVal: List[String] = null
            var nrServingSCC4PCI_0xB97FVal: List[String] = null
            var nrServingSCC5PCI_0xB97FVal: List[String] = null

            var nrServingPCCRxBeamId0_0xB97FVal: Integer = null
            var nrServingPCCRxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC1RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC1RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC2RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC2RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC3RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC3RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC4RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC4RxBeamId1_0xB97FVal: Integer = null
            var nrServingSCC5RxBeamId0_0xB97FVal: Integer = null
            var nrServingSCC5RxBeamId1_0xB97FVal: Integer = null

            var nrServingPCCRxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingPCCRxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC1RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC1RxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC2RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC2RxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC3RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC3RxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC4RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC4RxBeamRSRP1_0xB97FVal: List[String] = List[String]()
            var nrServingSCC5RxBeamRSRP0_0xB97FVal: List[String] = List[String]()
            var nrServingSCC5RxBeamRSRP1_0xB97FVal: List[String] = List[String]()

            var nrServingPCCRSRP_0xB97FVal: List[String] = List[String]()
            var nrServingPCCRSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC1RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC1RSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC2RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC2RSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC3RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC3RSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC4RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC4RSRQ_0xB97FVal: List[String] = List[String]()
            var nrServingSCC5RSRP_0xB97FVal: List[String] = List[String]()
            var nrServingSCC5RSRQ_0xB97FVal: List[String] = List[String]()

            var nrArfcnPCC_0xB97FVal:Integer = null
            var nrArfcnSCC1_0xB97FVal:Integer = null
            var nrBandPCC_0xB97FVal:Integer = null
            var nrBandSCC1_0xB97FVal:Integer = null
            var nrBandTypePCC_0xB97FVal:String = null
            var nrBandTypeSCC1_0xB97FVal:String = null

            def getPCI(list: List[Dynamic_47487_131081_SR_57542_DTO]) = {
              list.map(_.PCI)
            }

            def getServingRxBeam(index: Int, list: List[Dynamic_47487_131081_ENUMERATION_57536_DTO]): Integer = {
              try {
                Integer.parseInt(list(index).ServingRXBeam)
              } catch {
                case _: NumberFormatException => null
              }
            }

            def getRSRP(index: Int, servingSSB: String, list: List[Dynamic_47487_131081_SR_57542_DTO]) = {
              list.flatMap(_.sr_57548_inst_list.asScala.toList.filter(_.SSBIndex.equals(servingSSB)).flatMap(_.sr_57538_inst_list.asScala.toList.map(_.sr_57563_inst_list.get(index).RSRP)))
            }

            def getCellQualityRSRP(list: List[Dynamic_47487_131081_SR_57542_DTO]) = {
              list.flatMap(_.sr_57547_inst_list.asScala.toList.map(_.CellQualityRSRP).filterNot(_.equals("0.000")))
            }

            def getCellQualityRSRQ(list: List[Dynamic_47487_131081_SR_57542_DTO]) = {
              list.flatMap(_.sr_57573_inst_list.asScala.toList.map(_.CellQualityRSRQ).filterNot(_.equals("0.000")))
            }

            val sr_57583_inst_list = bean47487_131081.sr_57583_inst_list.asScala.toList
            sr_57583_inst_list.foreach {
              case inst if inst.CC_ID.equals("0") =>
                nrServingPCCPCI_0xB97FVal = getPCI(inst.sr_57542_inst_list.asScala.toList)
                //              logger.info("nrServingPCCPCI_0xB97FVal >>>>>>>>" + nrServingPCCPCI_0xB97FVal)
                nrServingPCCRxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_57536_inst_list.asScala.toList)
                nrServingPCCRxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_57536_inst_list.asScala.toList)
                nrServingPCCRxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_57542_inst_list.asScala.toList)
                //              logger.info("nrServingPCCRxBeamRSRP0_0xB97FVal >>>>>>>>" + nrServingPCCRxBeamRSRP0_0xB97FVal)
                nrServingPCCRxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_57542_inst_list.asScala.toList)
                //              logger.info("nrServingPCCRxBeamRSRP1_0xB97FVal >>>>>>>>" + nrServingPCCRxBeamRSRP1_0xB97FVal)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingPCCRSRP_0xB97FVal = getCellQualityRSRP(inst.sr_57542_inst_list.asScala.toList)
                //                logger.info("nrServingPCCRSRP_0xB97FVal >>>>>>>>" + nrServingPCCRSRP_0xB97FVal)
                nrServingPCCRSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_57542_inst_list.asScala.toList)
              //                logger.info("nrServingPCCRSRQ_0xB97FVal >>>>>>>>" + nrServingPCCRSRQ_0xB97FVal)
              //              }
              case inst if inst.CC_ID.equals("1") =>
                nrServingSCC1PCI_0xB97FVal = getPCI(inst.sr_57542_inst_list.asScala.toList)
                //              logger.info("nrServingSCC1PCI_0xB97FVal >>>>>>>>" + nrServingSCC1PCI_0xB97FVal)
                nrServingSCC1RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_57536_inst_list.asScala.toList)
                nrServingSCC1RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_57536_inst_list.asScala.toList)
                nrServingSCC1RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_57542_inst_list.asScala.toList)
                nrServingSCC1RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_57542_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC1RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_57542_inst_list.asScala.toList)
                nrServingSCC1RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_57542_inst_list.asScala.toList)
              //              }
              case inst if inst.CC_ID.equals("2") =>
                nrServingSCC2PCI_0xB97FVal = getPCI(inst.sr_57542_inst_list.asScala.toList)
                //              logger.info("nrServingSCC2PCI_0xB97FVal >>>>>>>>" + nrServingSCC2PCI_0xB97FVal)
                nrServingSCC2RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_57536_inst_list.asScala.toList)
                nrServingSCC2RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_57536_inst_list.asScala.toList)
                nrServingSCC2RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_57542_inst_list.asScala.toList)
                nrServingSCC2RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_57542_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC2RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_57542_inst_list.asScala.toList)
                nrServingSCC2RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_57542_inst_list.asScala.toList)
              //              }
              case inst if inst.CC_ID.equals("3") =>
                nrServingSCC3PCI_0xB97FVal = getPCI(inst.sr_57542_inst_list.asScala.toList)
                //              logger.info("nrServingSCC3PCI_0xB97FVal >>>>>>>>" + nrServingSCC3PCI_0xB97FVal)
                nrServingSCC3RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_57536_inst_list.asScala.toList)
                nrServingSCC3RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_57536_inst_list.asScala.toList)
                nrServingSCC3RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_57542_inst_list.asScala.toList)
                nrServingSCC3RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_57542_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC3RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_57542_inst_list.asScala.toList)
                nrServingSCC3RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_57542_inst_list.asScala.toList)
              //              }
              case inst if inst.CC_ID.equals("4") =>
                nrServingSCC4PCI_0xB97FVal = getPCI(inst.sr_57542_inst_list.asScala.toList)
                //              logger.info("nrServingSCC4PCI_0xB97FVal >>>>>>>>" + nrServingSCC4PCI_0xB97FVal)
                nrServingSCC4RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_57536_inst_list.asScala.toList)
                nrServingSCC4RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_57536_inst_list.asScala.toList)
                nrServingSCC4RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_57542_inst_list.asScala.toList)
                nrServingSCC4RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_57542_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC4RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_57542_inst_list.asScala.toList)
                nrServingSCC4RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_57542_inst_list.asScala.toList)
              //              }
              case inst if inst.CC_ID.equals("5") =>
                nrServingSCC5PCI_0xB97FVal = getPCI(inst.sr_57542_inst_list.asScala.toList)
                //              logger.info("nrServingSCC5PCI_0xB97FVal >>>>>>>>" + nrServingSCC5PCI_0xB97FVal)
                nrServingSCC5RxBeamId0_0xB97FVal = getServingRxBeam(0, inst.Enumeration_57536_inst_list.asScala.toList)
                nrServingSCC5RxBeamId1_0xB97FVal = getServingRxBeam(1, inst.Enumeration_57536_inst_list.asScala.toList)
                nrServingSCC5RxBeamRSRP0_0xB97FVal = getRSRP(0, inst.ServingSSB, inst.sr_57542_inst_list.asScala.toList)
                nrServingSCC5RxBeamRSRP1_0xB97FVal = getRSRP(1, inst.ServingSSB, inst.sr_57542_inst_list.asScala.toList)
                // TODO check if the filter is needed
                //              if (inst.RasterARFCN.equals("2075995")) {
                nrServingSCC5RSRP_0xB97FVal = getCellQualityRSRP(inst.sr_57542_inst_list.asScala.toList)
                nrServingSCC5RSRQ_0xB97FVal = getCellQualityRSRQ(inst.sr_57542_inst_list.asScala.toList)
              //              }
              case ccIndex =>
                logger.info(s"Ignoring ccIndex=$ccIndex for logCode=$logCode and logVersion=$logVersion")
            }

            var servingCellIndx = Try(CCIndexVal.map(_.toInt)).getOrElse(null)
            var nrarfcn = Try(NRArfcnVal.map(_.toInt)).getOrElse(null)
            if(nrarfcn.size>=1) {
              nrArfcnPCC_0xB97FVal = nrarfcn(0)
              if (nrArfcnPCC_0xB97FVal != null && servingCellIndx != null) {
                nrBandPCC_0xB97FVal = ParseUtils.getNrBandIndicator(nrArfcnPCC_0xB97FVal)
                if (nrBandPCC_0xB97FVal != null) nrBandTypePCC_0xB97FVal = getNrBandType(nrBandPCC_0xB97FVal)
              }
            }

            if(nrarfcn.size>1 && servingCellIndx.size>1) {
              nrArfcnSCC1_0xB97FVal = nrarfcn(1)
              if (nrArfcnSCC1_0xB97FVal != null && servingCellIndx != null) {
                nrBandSCC1_0xB97FVal = ParseUtils.getNrBandIndicator(nrArfcnSCC1_0xB97FVal)
                if (nrBandSCC1_0xB97FVal != null) nrBandTypeSCC1_0xB97FVal = getNrBandType(nrBandSCC1_0xB97FVal)
              }
            }

            logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSBRSRP_0xB97F = NRSBRSRPVal, NRSBRSRQ_0xB97F = NRSBRSRQVal, NRBEAMCOUNT_0XB97F = Try(NRBEAMCOUNTVal.map(_.toInt)).getOrElse(null), CCIndex_0xB97F = servingCellIndx,
              NRPCI_0XB97F = Try(NRPCIVal.map(_.toInt)).getOrElse(null), NRServingBeamIndex_0xB97F = Try(NRServingBeamIndexVal.head.toInt.asInstanceOf[Integer]).getOrElse(null), NRServingCellPci_0xB97F = Try(NRServingCellPCIVal.map(_.toInt)).getOrElse(null),
              NRServingPCCPCI_0xB97F = Try(nrServingPCCPCI_0xB97FVal.map(_.toInt)) getOrElse (null), NRServingSCC1PCI_0xB97F = Try(nrServingSCC1PCI_0xB97FVal.map(_.toInt)).getOrElse(null), NRServingSCC2PCI_0xB97F = Try(nrServingSCC2PCI_0xB97FVal.map(_.toInt)).getOrElse(null),
              NRServingSCC3PCI_0xB97F = Try(nrServingSCC3PCI_0xB97FVal.map(_.toInt)) getOrElse (null), NRServingSCC4PCI_0xB97F = Try(nrServingSCC4PCI_0xB97FVal.map(_.toInt)).getOrElse(null), NRServingSCC5PCI_0xB97F = Try(nrServingSCC5PCI_0xB97FVal.map(_.toInt)).getOrElse(null),
              NRServingPCCRxBeamId0_0xB97F = nrServingPCCRxBeamId0_0xB97FVal, NRServingPCCRxBeamId1_0xB97F = nrServingPCCRxBeamId1_0xB97FVal,
              NRServingSCC1RxBeamId0_0xB97F = nrServingSCC1RxBeamId0_0xB97FVal, NRServingSCC1RxBeamId1_0xB97F = nrServingSCC1RxBeamId1_0xB97FVal,
              NRServingSCC2RxBeamId0_0xB97F = nrServingSCC2RxBeamId0_0xB97FVal, NRServingSCC2RxBeamId1_0xB97F = nrServingSCC2RxBeamId1_0xB97FVal,
              NRServingSCC3RxBeamId0_0xB97F = nrServingSCC3RxBeamId0_0xB97FVal, NRServingSCC3RxBeamId1_0xB97F = nrServingSCC3RxBeamId1_0xB97FVal,
              NRServingSCC4RxBeamId0_0xB97F = nrServingSCC4RxBeamId0_0xB97FVal, NRServingSCC4RxBeamId1_0xB97F = nrServingSCC4RxBeamId1_0xB97FVal,
              NRServingSCC5RxBeamId0_0xB97F = nrServingSCC5RxBeamId0_0xB97FVal, NRServingSCC5RxBeamId1_0xB97F = nrServingSCC5RxBeamId1_0xB97FVal,
              NRServingPCCRxBeamRSRP0_0xB97F = Try(nrServingPCCRxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingPCCRxBeamRSRP1_0xB97F = Try(nrServingPCCRxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC1RxBeamRSRP0_0xB97F = Try(nrServingSCC1RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC1RxBeamRSRP1_0xB97F = Try(nrServingSCC1RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC2RxBeamRSRP0_0xB97F = Try(nrServingSCC2RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC2RxBeamRSRP1_0xB97F = Try(nrServingSCC2RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC3RxBeamRSRP0_0xB97F = Try(nrServingSCC3RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC3RxBeamRSRP1_0xB97F = Try(nrServingSCC3RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC4RxBeamRSRP0_0xB97F = Try(nrServingSCC4RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC4RxBeamRSRP1_0xB97F = Try(nrServingSCC4RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC5RxBeamRSRP0_0xB97F = Try(nrServingSCC5RxBeamRSRP0_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC5RxBeamRSRP1_0xB97F = Try(nrServingSCC5RxBeamRSRP1_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingPCCRSRP_0xB97F = Try(nrServingPCCRSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingPCCRSRQ_0xB97F = Try(nrServingPCCRSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC1RSRP_0xB97F = Try(nrServingSCC1RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC1RSRQ_0xB97F = Try(nrServingSCC1RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC2RSRP_0xB97F = Try(nrServingSCC2RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC2RSRQ_0xB97F = Try(nrServingSCC2RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC3RSRP_0xB97F = Try(nrServingSCC3RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC3RSRQ_0xB97F = Try(nrServingSCC3RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC4RSRP_0xB97F = Try(nrServingSCC4RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC4RSRQ_0xB97F = Try(nrServingSCC4RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRServingSCC5RSRP_0xB97F = Try(nrServingSCC5RSRP_0xB97FVal.map(_.toFloat)) getOrElse (null), NRServingSCC5RSRQ_0xB97F = Try(nrServingSCC5RSRQ_0xB97FVal.map(_.toFloat)).getOrElse(null),
              NRBandType_0xB97F = nrBandTypePCC_0xB97FVal, NRBandInd_0xB97F = nrBandPCC_0xB97FVal)
            logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRBandTypeSCC1_0xB97F =  nrBandTypeSCC1_0xB97FVal,NRBandScc1_0xB97F= nrBandSCC1_0xB97FVal,
              NrPCCARFCN_0XB97F = nrArfcnPCC_0xB97FVal, NrSCC1ARFCN_0XB97F= nrArfcnSCC1_0xB97FVal)
            logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47487", hexLogCode = "0xB97F", logRecordQComm5G = logRecordQComm5GObj)
          case _ =>
            logger.info("Unknown 47487=========>" + logVersion)
            logRecord.copy(logRecordName = "QCOMM5G", missingVersion = logVersion, logCode = "47487", hexLogCode = "0xB97F")
        }
      } catch {
        case e: Throwable =>
          logger.error(s"Got exception for logCode=$logCode, logVersion=$logVersion", e)
          logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47487", hexLogCode = "0xB97F", exceptionOccured = true, exceptionCause = e.toString)
      }
    }

    def parse47141(parentlogRecord: LogRecord): LogRecord = {
      var logRecord: LogRecord = parentlogRecord
      var logRecordQComm5GObj: LogRecordQComm5G = LogRecordQComm5G(is5GTechnology = true)
      logVersion match {
        case 3 =>
          val bean47141_3 = process47141_3(x, exceptionUseArray)
          val NrConnectivityTypeVal = bean47141_3.sr_27265_inst_list.asScala.toList.map(_.sr_27283_inst_list.asScala.toList.map(_.BandType)).flatten
          val NrConnectivityModeVal = bean47141_3.ConnectivityMode
          val NRcellId = bean47141_3.sr_27265_inst_list.asScala.toList.map(_.sr_27283_inst_list.asScala.toList.map(_.CellId)).flatten
          val NRArfcn = bean47141_3.sr_27265_inst_list.asScala.toList.map(_.sr_27283_inst_list.asScala.toList.map(_.DLArfcn)).flatten
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRConnectivityType_0xB825 = Try(NrConnectivityTypeVal).getOrElse(null), NRConnectivityMode_0xB825 = Try(NrConnectivityModeVal).getOrElse(null))
          logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47141", hexLogCode = "0xB825", logRecordQComm5G = logRecordQComm5GObj)
        case 7 =>
          val bean47141_7 = process47141_7(x, exceptionUseArray)
          val NrConnectivityTypeVal = bean47141_7.sr_31599_inst_list.asScala.toList.map(_.BandType)
          val NrConnectivityModeVal = bean47141_7.ConnectivityMode
          val NRcellId = bean47141_7.sr_31599_inst_list.asScala.toList.map(_.CellId)
          val NRArfcn = bean47141_7.sr_31599_inst_list.asScala.toList.map(_.DLArfcn)
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRConnectivityType_0xB825 = Try(NrConnectivityTypeVal).getOrElse(null), NRConnectivityMode_0xB825 = Try(NrConnectivityModeVal).getOrElse(null))
          logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47141", hexLogCode = "0xB825", logRecordQComm5G = logRecordQComm5GObj)
        case 8 =>
          val bean47141_8 = process47141_8(x, exceptionUseArray)
          val NrConnectivityTypeVal = bean47141_8.sr_36532_inst_list.asScala.toList.map(_.BandType)
          val NrConnectivityModeVal = bean47141_8.ConnectivityMode
          val NRcellId = bean47141_8.sr_36532_inst_list.asScala.toList.map(_.CellId)
          val NRArfcn = bean47141_8.sr_36532_inst_list.asScala.toList.map(_.DLArfcn)
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRConnectivityType_0xB825 = Try(NrConnectivityTypeVal).getOrElse(null), NRConnectivityMode_0xB825 = Try(NrConnectivityModeVal).getOrElse(null))
          logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47141", hexLogCode = "0xB825", logRecordQComm5G = logRecordQComm5GObj)
        case 10 =>
          val bean47141_10 = process47141_10(x, exceptionUseArray)
          val NrConnectivityTypeVal = bean47141_10.sr_38765_inst_list.asScala.toList.map(_.BandType)
          val NrConnectivityModeVal = bean47141_10.ConnectivityMode
          val NRcellId = bean47141_10.sr_38765_inst_list.asScala.toList.map(_.CellId)
          val NRArfcn = bean47141_10.sr_38765_inst_list.asScala.toList.map(_.DLArfcn)
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRConnectivityType_0xB825 = Try(NrConnectivityTypeVal).getOrElse(null), NRConnectivityMode_0xB825 = Try(NrConnectivityModeVal).getOrElse(null))
          logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47141", hexLogCode = "0xB825", logRecordQComm5G = logRecordQComm5GObj)
        case 131072 =>
          val bean47141_131072 = process47141_131072(x, exceptionUseArray)
          val NrConnectivityTypeVal = bean47141_131072.sr_48033_inst_list.asScala.toList.map(_.BandType)
          val NrConnectivityModeVal = bean47141_131072.ConnectivityMode
          val NRcellId = bean47141_131072.sr_48033_inst_list.asScala.toList.map(_.CellId)
          val NRArfcn = bean47141_131072.sr_48033_inst_list.asScala.toList.map(_.DLArfcn)
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRConnectivityType_0xB825 = Try(NrConnectivityTypeVal).getOrElse(null), NRConnectivityMode_0xB825 = Try(NrConnectivityModeVal).getOrElse(null))
          logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47141", hexLogCode = "0xB825", logRecordQComm5G = logRecordQComm5GObj)
        case 196608 =>
          val bean47141_196608 = process47141_196608(x, exceptionUseArray)
          val NrConnectivityTypeVal = bean47141_196608.sr_58863_inst_list.asScala.toList.map(_.BandType)
          val NrConnectivityModeVal = bean47141_196608.ConnectivityMode
          val NRcellId = bean47141_196608.sr_58863_inst_list.asScala.toList.map(_.CellId)
          val NRArfcn = bean47141_196608.sr_58863_inst_list.asScala.toList.map(_.DLArfcn)
          logRecordQComm5GObj = logRecordQComm5GObj.copy(NRConnectivityType_0xB825 = Try(NrConnectivityTypeVal).getOrElse(null), NRConnectivityMode_0xB825 = Try(NrConnectivityModeVal).getOrElse(null))
          logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47141", hexLogCode = "0xB825", logRecordQComm5G = logRecordQComm5GObj)
        case _ =>
          logger.info("Unknown 47141=========>" + logVersion)
          logRecord.copy(logRecordName = "QCOMM5G", missingVersion = logVersion, logCode = "47141", hexLogCode = "0xB825")
      }
    }

    def parse47235(parentlogRecord: LogRecord): LogRecord = {
      var logRecord: LogRecord = parentlogRecord
      var logRecordQComm5GObj: LogRecordQComm5G = LogRecordQComm5G()
      logVersion match {
        case 131079 =>
          //logger.info("47235  =====>  131079!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
          val bean47235_131079 = process47235_131079(x, exceptionUseArray)
          //logger.info("bean47235_131079 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47235_131079))
          //list(list(list((a,b))))
          val txTypeBytesList = bean47235_131079.sr_20969_inst_list.asScala.map(_.sr_20986_inst_list.asScala.map(_.sr_20993_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes))))
            .flatten.flatten
          val txTypeBytesListCarrier0 = bean47235_131079.sr_20969_inst_list.asScala.map(_.sr_20986_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("0")).map(_.sr_20993_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes))))
            .flatten.flatten
          val txTypeBytesListCarrier1 = bean47235_131079.sr_20969_inst_list.asScala.map(_.sr_20986_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("1")).map(_.sr_20993_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes))))
            .flatten.flatten
          //logger.info("47235  =====>  131079>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" +txTypeBytesList)
          val txNewTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
          val txReTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).map(newtx => newtx._2.toLong).sum

          val txNewTbCarrier0Sum = txTypeBytesListCarrier0.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
          val txNewTbCarrier1Sum = txTypeBytesListCarrier1.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
          val txNewTbCarrier0Count = txTypeBytesListCarrier0.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).count(x => true)
          val txNewTbCarrier1Count = txTypeBytesListCarrier1.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).count(x => true)
          val txReTbCarrier0Count = txTypeBytesListCarrier0.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).count(x => true)
          val txReTbCarrier1Count = txTypeBytesListCarrier1.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).count(x => true)

          val txTotalTbSum = txTypeBytesList.map(newtx => newtx._2.toLong).sum
          val numerology = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20978_inst_list.asScala.toList.map(_.Numerology)).flatten

          var nr5GPhyULInstTputPCC: lang.Double = null
          var nr5GPhyULInstTputSCC1: lang.Double = null
          var nr5GPhyULInstTput: lang.Double = null
          var nr5GPhyULInstRetransRatePCC: lang.Double = null
          var nr5GPhyULInstRetransRateSCC1: lang.Double = null
          var nr5GPhyULInstRetransRate: lang.Double = null
          var firstSfn: Int = 0
          var firstSfnSlot: Int = 0
          var lastSfn: Int = 0
          var lastSfnSlot: Int = 0
          var firstRecord: Boolean = true
          val numerology_sfn = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20978_inst_list.asScala.toList.map(_.Numerology)).flatten.head
          for (i <- 0 to bean47235_131079.sr_20969_inst_list.size() - 1) {
            val record: Dynamic_47235_131079_SR_20969_DTO = bean47235_131079.sr_20969_inst_list.get(i)
            if (firstRecord) {
              firstSfn = record.getSr_20978_inst_list.get(0).getFrame.toInt
              firstSfnSlot = record.getSr_20978_inst_list.get(0).getSlot.toInt
              firstRecord = false
            }
            lastSfn = record.getSr_20978_inst_list.get(0).getFrame.toInt
            lastSfnSlot = record.getSr_20978_inst_list.get(0).getSlot.toInt
          }
          val durationMS = Constants.CalculateMsFromSfnAndSubFrames5gNr(numerology_sfn, firstSfn, firstSfnSlot, lastSfn, lastSfnSlot)
          if (durationMS > 0) {
            if (txNewTbCarrier0Sum > 0)
              nr5GPhyULInstTputPCC = BigDecimal((txNewTbCarrier0Sum * 8 * (1000 / durationMS.asInstanceOf[Double])) / Constants.Mbps).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
            if (txNewTbCarrier1Sum > 0)
              nr5GPhyULInstTputSCC1 = BigDecimal((txNewTbCarrier1Sum * 8 * (1000 / durationMS.asInstanceOf[Double])) / Constants.Mbps).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
            if (nr5GPhyULInstTputPCC != null && nr5GPhyULInstTputSCC1 != null)
              nr5GPhyULInstTput = nr5GPhyULInstTputPCC + nr5GPhyULInstTputSCC1
            else if (nr5GPhyULInstTputPCC != null)
              nr5GPhyULInstTput = nr5GPhyULInstTputPCC
            else if (nr5GPhyULInstTputSCC1 != null)
              nr5GPhyULInstTput = nr5GPhyULInstTputSCC1
          }
          if (txReTbCarrier0Count > 0 || txReTbCarrier1Count > 0 || txNewTbCarrier0Count > 0 || txNewTbCarrier1Count > 0) {
            nr5GPhyULInstRetransRate = BigDecimal(((txReTbCarrier0Count.toDouble + txReTbCarrier1Count.toDouble) * 100) / (txReTbCarrier0Count.toDouble + txReTbCarrier1Count.toDouble + txNewTbCarrier0Count.toDouble + txNewTbCarrier1Count.toDouble)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
            if (txReTbCarrier0Count > 0 || txReTbCarrier1Count > 0) {
              nr5GPhyULInstRetransRatePCC = BigDecimal((txReTbCarrier0Count.toDouble * 100) / (txReTbCarrier0Count.toDouble + txNewTbCarrier0Count.toDouble)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
              nr5GPhyULInstRetransRateSCC1 = BigDecimal((txReTbCarrier1Count.toDouble * 100) / (txReTbCarrier1Count.toDouble + txNewTbCarrier1Count.toDouble)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
            }
          }

          val puschCarrierId = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.CarrierID)).flatten
          val puschHarq = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.sr_20993_inst_list.asScala.toList.map(_.HARQID))).flatten.flatten
          val puschMcs = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.sr_20993_inst_list.asScala.toList.map(_.MCS))).flatten.flatten
          val puschNumRbs = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.sr_20993_inst_list.asScala.toList.map(_.NumRBs))).flatten.flatten
          val puschTbsize = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.sr_20993_inst_list.asScala.toList.map(_.TBSizebytes))).flatten.flatten
          val puschTxmode = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.sr_20993_inst_list.asScala.toList.map(_.TXMode))).flatten.flatten
          val puschTxtype = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.sr_20993_inst_list.asScala.toList.map(_.TXType))).flatten.flatten
          val puschBwpIdx = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.sr_20993_inst_list.asScala.toList.map(_.BWPIdx))).flatten.flatten

          logRecordQComm5GObj = logRecordQComm5GObj.copy(txNewTbSum_0xB883 = txNewTbSum, txReTbSum_0xB883 = txReTbSum, txTotalSum_0xB883 = txTotalTbSum,
            carrierId_0XB883 = Try(puschCarrierId.map(_.toInt)).getOrElse(null), harq_0XB883 = Try(puschHarq.map(_.toInt)).getOrElse(null)
            , numerology_0xB883 = numerology, mcs_0XB883 = Try(puschMcs.map(_.toInt)).getOrElse(null), numrbs_0XB883 = Try(puschNumRbs.map(_.toInt)).getOrElse(null), tbsize_0XB883 = Try(puschTbsize.map(_.toLong)).getOrElse(null),
            txtype_0XB883 = puschTxtype, txmode_0XB883 = puschTxmode, bwpidx_0XB883 = Try(puschBwpIdx.map(_.toInt)).getOrElse(null), nr5GPhyULInstTputPCC_0XB883 = nr5GPhyULInstTputPCC, nr5GPhyULInstTputSCC1_0XB883 = nr5GPhyULInstTputSCC1,
            nr5GPhyULInstTput_0XB883 = nr5GPhyULInstTput, nr5GPhyULInstRetransRatePCC_0XB883 = nr5GPhyULInstRetransRatePCC, nr5GPhyULInstRetransRateSCC1_0XB883 = nr5GPhyULInstRetransRateSCC1, nr5GPhyULInstRetransRate_0XB883 = nr5GPhyULInstRetransRate)

          logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47235", hexLogCode = "0xB883", logRecordQComm5G = logRecordQComm5GObj)

        case 131080 =>
          val bean47235_131080 = process47235_131080(x, exceptionUseArray)
          //logger.info("bean47235_131080 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47235_131080))
          val txTypeBytesList = bean47235_131080.sr_21640_inst_list.asScala.map(_.sr_21641_inst_list.asScala.map(_.sr_21648_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes))))
            .flatten.flatten

          val txTypeBytesListCarrier0 = bean47235_131080.sr_21640_inst_list.asScala.map(_.sr_21641_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("0")).map(_.sr_21648_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes))))
            .flatten.flatten
          val txTypeBytesListCarrier1 = bean47235_131080.sr_21640_inst_list.asScala.map(_.sr_21641_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("1")).map(_.sr_21648_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes))))
            .flatten.flatten

          val txNewTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
          val txReTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).map(newtx => newtx._2.toLong).sum
          val txTotalTbSum = txTypeBytesList.map(newtx => newtx._2.toLong).sum

          val txNewTbCarrier0Sum = txTypeBytesListCarrier0.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
          val txNewTbCarrier1Sum = txTypeBytesListCarrier1.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
          val txNewTbCarrier0Count = txTypeBytesListCarrier0.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).count(x => true)
          val txNewTbCarrier1Count = txTypeBytesListCarrier1.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).count(x => true)
          val txReTbCarrier0Count = txTypeBytesListCarrier0.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).count(x => true)
          val txReTbCarrier1Count = txTypeBytesListCarrier1.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).count(x => true)


          val numerology = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21629_inst_list.asScala.toList.map(_.Numerology)).flatten

          var nr5GPhyULInstTputPCC: lang.Double = null
          var nr5GPhyULInstTputSCC1: lang.Double = null
          var nr5GPhyULInstTput: lang.Double = null
          var nr5GPhyULInstRetransRatePCC: lang.Double = null
          var nr5GPhyULInstRetransRateSCC1: lang.Double = null
          var nr5GPhyULInstRetransRate: lang.Double = null
          var firstSfn: Int = 0
          var firstSfnSlot: Int = 0
          var lastSfn: Int = 0
          var lastSfnSlot: Int = 0
          var firstRecord: Boolean = true
          val numerology_sfn = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21629_inst_list.asScala.toList.map(_.Numerology)).flatten.head
          for (i <- 0 to bean47235_131080.sr_21640_inst_list.size() - 1) {
            val record: Dynamic_47235_131080_SR_21640_DTO = bean47235_131080.sr_21640_inst_list.get(i)
            if (firstRecord) {
              firstSfn = record.getSr_21629_inst_list.get(0).getFrame.toInt
              firstSfnSlot = record.getSr_21629_inst_list.get(0).getSlot.toInt
              firstRecord = false
            }
            lastSfn = record.getSr_21629_inst_list.get(0).getFrame.toInt
            lastSfnSlot = record.getSr_21629_inst_list.get(0).getSlot.toInt
          }
          val durationMS = Constants.CalculateMsFromSfnAndSubFrames5gNr(numerology_sfn, firstSfn, firstSfnSlot, lastSfn, lastSfnSlot)
          if (durationMS > 0) {
            if (txNewTbCarrier0Sum > 0)
              nr5GPhyULInstTputPCC = BigDecimal((txNewTbCarrier0Sum * 8 * (1000 / durationMS.asInstanceOf[Double])) / Constants.Mbps).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
            if (txNewTbCarrier1Sum > 0)
              nr5GPhyULInstTputSCC1 = BigDecimal((txNewTbCarrier1Sum * 8 * (1000 / durationMS.asInstanceOf[Double])) / Constants.Mbps).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
            if (nr5GPhyULInstTputPCC != null && nr5GPhyULInstTputSCC1 != null)
              nr5GPhyULInstTput = nr5GPhyULInstTputPCC + nr5GPhyULInstTputSCC1
            else if (nr5GPhyULInstTputPCC != null)
              nr5GPhyULInstTput = nr5GPhyULInstTputPCC
            else if (nr5GPhyULInstTputSCC1 != null)
              nr5GPhyULInstTput = nr5GPhyULInstTputSCC1
          }
          if (txReTbCarrier0Count > 0 || txReTbCarrier1Count > 0 || txNewTbCarrier0Count > 0 || txNewTbCarrier1Count > 0) {
            nr5GPhyULInstRetransRate = BigDecimal(((txReTbCarrier0Count.toDouble + txReTbCarrier1Count.toDouble) * 100) / (txReTbCarrier0Count.toDouble + txReTbCarrier1Count.toDouble + txNewTbCarrier0Count.toDouble + txNewTbCarrier1Count.toDouble)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
            if (txReTbCarrier0Count > 0 || txReTbCarrier1Count > 0) {
              nr5GPhyULInstRetransRatePCC = BigDecimal((txReTbCarrier0Count.toDouble * 100) / (txReTbCarrier0Count.toDouble + txNewTbCarrier0Count.toDouble)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
              nr5GPhyULInstRetransRateSCC1 = BigDecimal((txReTbCarrier1Count.toDouble * 100) / (txReTbCarrier1Count.toDouble + txNewTbCarrier1Count.toDouble)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
            }
          }

          val puschCarrierId = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.CarrierID)).flatten
          val puschHarq = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.sr_21648_inst_list.asScala.map(_.HARQID))).flatten.flatten
          val puschMcs = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.sr_21648_inst_list.asScala.toList.map(_.MCS))).flatten.flatten
          val puschNumRbs = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.sr_21648_inst_list.asScala.toList.map(_.NumRBs))).flatten.flatten
          val puschTbsize = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.sr_21648_inst_list.asScala.toList.map(_.TBSizebytes))).flatten.flatten
          val puschTxmode = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.sr_21648_inst_list.asScala.toList.map(_.TXMode))).flatten.flatten
          val puschTxtype = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.sr_21648_inst_list.asScala.toList.map(_.TXType))).flatten.flatten
          val puschBwpIdx = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.sr_21648_inst_list.asScala.toList.map(_.BWPIdx))).flatten.flatten

          logRecordQComm5GObj = logRecordQComm5GObj.copy(txNewTbSum_0xB883 = txNewTbSum, txReTbSum_0xB883 = txReTbSum, txTotalSum_0xB883 = txTotalTbSum, carrierId_0XB883 = Try(puschCarrierId.map(_.toInt)).getOrElse(null), harq_0XB883 = Try(puschHarq.map(_.toInt)).getOrElse(null)
            , numerology_0xB883 = numerology, mcs_0XB883 = Try(puschMcs.map(_.toInt)).getOrElse(null), numrbs_0XB883 = Try(puschNumRbs.map(_.toInt)).getOrElse(null), tbsize_0XB883 = Try(puschTbsize.map(_.toLong)).getOrElse(null),
            txtype_0XB883 = puschTxtype, txmode_0XB883 = puschTxmode, bwpidx_0XB883 = Try(puschBwpIdx.map(_.toInt)).getOrElse(null), nr5GPhyULInstTputPCC_0XB883 = nr5GPhyULInstTputPCC, nr5GPhyULInstTputSCC1_0XB883 = nr5GPhyULInstTputSCC1,
            nr5GPhyULInstTput_0XB883 = nr5GPhyULInstTput, nr5GPhyULInstRetransRatePCC_0XB883 = nr5GPhyULInstRetransRatePCC, nr5GPhyULInstRetransRateSCC1_0XB883 = nr5GPhyULInstRetransRateSCC1, nr5GPhyULInstRetransRate_0XB883 = nr5GPhyULInstRetransRate)

          logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47235", hexLogCode = "0xB883", logRecordQComm5G = logRecordQComm5GObj)

        case 131083 =>
          val bean47235_131083 = process47235_131083(x, exceptionUseArray)
          //logger.info("bean47235_131083 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47235_131083))
          val txTypeBytesList = bean47235_131083.sr_27932_inst_list.asScala.map(_.sr_27969_inst_list.asScala.map(_.sr_27966_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes))))
            .flatten.flatten
          val txTypeBytesListCarrier0 = bean47235_131083.sr_27932_inst_list.asScala.map(_.sr_27969_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("0")).map(_.sr_27966_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes))))
            .flatten.flatten
          val txTypeBytesListCarrier1 = bean47235_131083.sr_27932_inst_list.asScala.map(_.sr_27969_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("1")).map(_.sr_27966_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes))))
            .flatten.flatten
          val txNewTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
          val txReTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).map(newtx => newtx._2.toLong).sum
          val txTotalTbSum = txTypeBytesList.map(newtx => newtx._2.toLong).sum
          val txNewTbCarrier0Sum = txTypeBytesListCarrier0.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
          val txNewTbCarrier1Sum = txTypeBytesListCarrier1.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
          val txNewTbCarrier0Count = txTypeBytesListCarrier0.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).count(x => true)
          val txNewTbCarrier1Count = txTypeBytesListCarrier1.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).count(x => true)
          val txReTbCarrier0Count = txTypeBytesListCarrier0.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).count(x => true)
          val txReTbCarrier1Count = txTypeBytesListCarrier1.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).count(x => true)
          val numerology = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27950_inst_list.asScala.toList.map(_.Numerology)).flatten
          var nr5GPhyULInstTputPCC: lang.Double = null
          var nr5GPhyULInstTputSCC1: lang.Double = null
          var nr5GPhyULInstTput: lang.Double = null
          var nr5GPhyULInstRetransRatePCC: lang.Double = null
          var nr5GPhyULInstRetransRateSCC1: lang.Double = null
          var nr5GPhyULInstRetransRate: lang.Double = null
          var firstSfn: Int = 0
          var firstSfnSlot: Int = 0
          var lastSfn: Int = 0
          var lastSfnSlot: Int = 0
          var firstRecord: Boolean = true
          val numerology_sfn = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27950_inst_list.asScala.toList.map(_.Numerology)).flatten.head
          for (i <- 0 to bean47235_131083.sr_27932_inst_list.size() - 1) {
            val record: Dynamic_47235_131083_SR_27932_DTO = bean47235_131083.sr_27932_inst_list.get(i)
            if (firstRecord) {
              firstSfn = record.getSr_27950_inst_list.get(0).getFrame.toInt
              firstSfnSlot = record.getSr_27950_inst_list.get(0).getSlot.toInt
              firstRecord = false
            }
            lastSfn = record.getSr_27950_inst_list.get(0).getFrame.toInt
            lastSfnSlot = record.getSr_27950_inst_list.get(0).getSlot.toInt
          }

          val durationMS = Constants.CalculateMsFromSfnAndSubFrames5gNr(numerology_sfn, firstSfn, firstSfnSlot, lastSfn, lastSfnSlot)
          if (durationMS > 0) {
            if (txNewTbCarrier0Sum > 0)
              nr5GPhyULInstTputPCC = BigDecimal((txNewTbCarrier0Sum * 8 * (1000 / durationMS.asInstanceOf[Double])) / Constants.Mbps).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
            if (txNewTbCarrier1Sum > 0)
              nr5GPhyULInstTputSCC1 = BigDecimal((txNewTbCarrier1Sum * 8 * (1000 / durationMS.asInstanceOf[Double])) / Constants.Mbps).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
            if (nr5GPhyULInstTputPCC != null && nr5GPhyULInstTputSCC1 != null)
              nr5GPhyULInstTput = nr5GPhyULInstTputPCC + nr5GPhyULInstTputSCC1
            else if (nr5GPhyULInstTputPCC != null)
              nr5GPhyULInstTput = nr5GPhyULInstTputPCC
            else if (nr5GPhyULInstTputSCC1 != null)
              nr5GPhyULInstTput = nr5GPhyULInstTputSCC1
          }

          if (txReTbCarrier0Count > 0 || txReTbCarrier1Count > 0 || txNewTbCarrier0Count > 0 || txNewTbCarrier1Count > 0) {
            nr5GPhyULInstRetransRate = BigDecimal(((txReTbCarrier0Count.toDouble + txReTbCarrier1Count.toDouble) * 100) / (txReTbCarrier0Count.toDouble + txReTbCarrier1Count.toDouble + txNewTbCarrier0Count.toDouble + txNewTbCarrier1Count.toDouble)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
            if (txReTbCarrier0Count > 0 || txReTbCarrier1Count > 0) {
              nr5GPhyULInstRetransRatePCC = BigDecimal((txReTbCarrier0Count.toDouble * 100) / (txReTbCarrier0Count.toDouble + txNewTbCarrier0Count.toDouble)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
              nr5GPhyULInstRetransRateSCC1 = BigDecimal((txReTbCarrier1Count.toDouble * 100) / (txReTbCarrier1Count.toDouble + txNewTbCarrier1Count.toDouble)).setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
            }
          }

          val puschCarrierId = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.CarrierID)).flatten
          val puschHarq = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.sr_27966_inst_list.asScala.toList.map(_.HARQID))).flatten.flatten
          val puschMcs = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.sr_27966_inst_list.asScala.toList.map(_.MCS))).flatten.flatten
          val puschNumRbs = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.sr_27966_inst_list.asScala.toList.map(_.NumRBs))).flatten.flatten
          val puschTbsize = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.sr_27966_inst_list.asScala.toList.map(_.TBSizebytes))).flatten.flatten
          val puschTxmode = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.sr_27966_inst_list.asScala.toList.map(_.TXMode))).flatten.flatten
          val puschTxtype = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.sr_27966_inst_list.asScala.toList.map(_.TXType))).flatten.flatten
          val puschBwpIdx = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.sr_27966_inst_list.asScala.toList.map(_.BWPIdx))).flatten.flatten

          logRecordQComm5GObj = logRecordQComm5GObj.copy(txNewTbSum_0xB883 = txNewTbSum, txReTbSum_0xB883 = txReTbSum, txTotalSum_0xB883 = txTotalTbSum, carrierId_0XB883 = Try(puschCarrierId.map(_.toInt)).getOrElse(null), harq_0XB883 = Try(puschHarq.map(_.toInt)).getOrElse(null),
            numerology_0xB883 = numerology, mcs_0XB883 = Try(puschMcs.map(_.toInt)).getOrElse(null), numrbs_0XB883 = Try(puschNumRbs.map(_.toInt)).getOrElse(null), tbsize_0XB883 = Try(puschTbsize.map(_.toLong)).getOrElse(null),
            txtype_0XB883 = puschTxtype, txmode_0XB883 = puschTxmode, bwpidx_0XB883 = Try(puschBwpIdx.map(_.toInt)).getOrElse(null), nr5GPhyULInstTputPCC_0XB883 = nr5GPhyULInstTputPCC, nr5GPhyULInstTputSCC1_0XB883 = nr5GPhyULInstTputSCC1,
            nr5GPhyULInstTput_0XB883 = nr5GPhyULInstTput, nr5GPhyULInstRetransRatePCC_0XB883 = nr5GPhyULInstRetransRatePCC, nr5GPhyULInstRetransRateSCC1_0XB883 = nr5GPhyULInstRetransRateSCC1, nr5GPhyULInstRetransRate_0XB883 = nr5GPhyULInstRetransRate)

          logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47235", hexLogCode = "0xB883", logRecordQComm5G = logRecordQComm5GObj)
        case _ =>
          // logger.info("47235  =====>  unknown logversion>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+logVersion)
          logRecord.copy(logRecordName = "QCOMM5G", logCode = "47235", hexLogCode = "0xB883", missingVersion = logVersion)
      }
    }

    logCode match {
      case 47487 =>
        parse47487(parentlogRecord)
      case 47141 =>
        parse47141(parentlogRecord)
      case 47235 =>
        parse47235(parentlogRecord)
      case _ =>
        parentlogRecord.copy(logRecordName = "QCOMM5G", missingLogCode = logCode)
    }
  }

  def parseLogRecordQCOMM5GSet4_47235(logVersion: Int, x: Array[Byte], exceptionUseArray: List[String], logRecordQComm5GObjValue: LogRecordQComm5G, logRecordQComm5G2ObjValue: LogRecordQComm5G2, logRecordValue: LogRecord) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = logRecordValue
    var logRecordQComm5GObj: LogRecordQComm5G = logRecordQComm5GObjValue
    var logRecordQComm5G2Obj: LogRecordQComm5G2 = logRecordQComm5G2ObjValue

    def defineNRPUSCHWaveform(transformPrecoding: String): String = {
      var res = ""
      if (transformPrecoding == null || transformPrecoding.isEmpty || transformPrecoding.equals("0")) {
        res = "CP-OFDM"
      } else if (transformPrecoding.equalsIgnoreCase("1")) {
        res = "DFT-S-OFDM"
      }
      res
    }

    logVersion match {
      case 131079 =>
        //logger.info("47235  =====>  131079!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        val bean47235_131079 = process47235_131079(x, exceptionUseArray)
        //logger.info("bean47235_131079 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47235_131079))
        val txTypeBytesList = bean47235_131079.sr_20969_inst_list.asScala.map(_.sr_20986_inst_list.asScala.map(_.sr_20993_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes))))
          .flatten.flatten
        //logger.info("47235  =====>  131079>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" +txTypeBytesList)
        val txNewTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
        val txReTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).map(newtx => newtx._2.toLong).sum
        val txTotalTbSum = txTypeBytesList.map(newtx => newtx._2.toLong).sum
        val numerology = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20978_inst_list.asScala.toList.map(_.Numerology)).flatten
        val puschCarrierId = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.CarrierID)).flatten
        val puschHarq = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.sr_20993_inst_list.asScala.toList.map(_.HARQID))).flatten.flatten
        val puschMcs = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.sr_20993_inst_list.asScala.toList.map(_.MCS))).flatten.flatten
        val puschNumRbs = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.sr_20993_inst_list.asScala.toList.map(_.NumRBs))).flatten.flatten
        val puschTbsize = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.sr_20993_inst_list.asScala.toList.map(_.TBSizebytes))).flatten.flatten
        val puschTxmode = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.sr_20993_inst_list.asScala.toList.map(_.TXMode))).flatten.flatten
        val puschTxtype = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.sr_20993_inst_list.asScala.toList.map(_.TXType))).flatten.flatten
        val puschBwpIdx = bean47235_131079.sr_20969_inst_list.asScala.toList.map(_.sr_20986_inst_list.asScala.toList.map(_.sr_20993_inst_list.asScala.toList.map(_.BWPIdx))).flatten.flatten

        val txTypes: List[String] = bean47235_131079.sr_20969_inst_list.asScala.flatMap(_.sr_20986_inst_list.asScala.flatMap(_.sr_20993_inst_list.asScala.map(_.TXType))).toList
        val numNEW_TX: Int = txTypes.count(_.equals("NEW_TX"))
        val numRE_TX: Int = txTypes.count(_.equals("RE_TX"))
        val carrierID_0 = bean47235_131079.sr_20969_inst_list.asScala.flatMap(_.sr_20986_inst_list.asScala.filter(_.CarrierID.equals("0")))
        val carrierID_1 = bean47235_131079.sr_20969_inst_list.asScala.flatMap(_.sr_20986_inst_list.asScala.filter(_.CarrierID.equals("1")))
        val numNEW_TXPCC: Int = carrierID_0.flatMap(_.sr_20993_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numNEW_TXSCC1: Int = carrierID_1.flatMap(_.sr_20993_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numRE_TXPCC: Int = carrierID_0.flatMap(_.sr_20993_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val numRE_TXSCC1: Int = carrierID_1.flatMap(_.sr_20993_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val sr_27966_inst_list = bean47235_131079.sr_20969_inst_list.asScala.flatMap(_.sr_20986_inst_list.asScala.flatMap(_.sr_20993_inst_list.asScala)).toList
        val numRE_TXRV_0: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("0"))
        val numRE_TXRV_1: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("1"))
        val numRE_TXRV_2: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("2"))
        val numRE_TXRV_3: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("3"))

        val transformPrecodingPCC: String = Try(bean47235_131079.sr_20969_inst_list.asScala.flatMap(_.sr_20986_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("0")).flatMap(_.sr_20993_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val transformPrecodingSCC1: String = Try(bean47235_131079.sr_20969_inst_list.asScala.flatMap(_.sr_20986_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("1")).flatMap(_.sr_20993_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val NRPUSCHWaveformPCCVal = defineNRPUSCHWaveform(transformPrecodingPCC)
        val NRPUSCHWaveformSCC1Val = defineNRPUSCHWaveform(transformPrecodingSCC1)

        logRecordQComm5GObj = logRecordQComm5GObj.copy(txNewTbSum_0xB883 = txNewTbSum, txReTbSum_0xB883 = txReTbSum, txTotalSum_0xB883 = txTotalTbSum,
          carrierId_0XB883 = Try(puschCarrierId.map(_.toInt)).getOrElse(null), harq_0XB883 = Try(puschHarq.map(_.toInt)).getOrElse(null),
          numerology_0xB883 = numerology, mcs_0XB883 = Try(puschMcs.map(_.toInt)).getOrElse(null), numrbs_0XB883 = Try(puschNumRbs.map(_.toInt)).getOrElse(null), tbsize_0XB883 = Try(puschTbsize.map(_.toLong)).getOrElse(null),
          txtype_0XB883 = puschTxtype, txmode_0XB883 = puschTxmode, bwpidx_0XB883 = Try(puschBwpIdx.map(_.toInt)).getOrElse(null),
          numNEW_TX_0XB883 = numNEW_TX, numRE_TX_0XB883 = numRE_TX, numNEW_TXPCC_0XB883 = numNEW_TXPCC, numNEW_TXSCC1_0XB883 = numNEW_TXSCC1, numRE_TXPCC_0XB883 = numRE_TXPCC,
          numRE_TXSCC1_0XB883 = numRE_TXSCC1, numRE_TXRV_0_0XB883 = numRE_TXRV_0, numRE_TXRV_1_0XB883 = numRE_TXRV_1, numRE_TXRV_2_0XB883 = numRE_TXRV_2, numRE_TXRV_3_0XB883 = numRE_TXRV_3)

        logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRPUSCHWaveformPCC = NRPUSCHWaveformPCCVal, NRPUSCHWaveformSCC1 = NRPUSCHWaveformSCC1Val)

        logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47235", hexLogCode = "0xB883", logRecordQComm5G = logRecordQComm5GObj, logRecordQComm5G2 = logRecordQComm5G2Obj)

      case 131080 =>
        val bean47235_131080 = process47235_131080(x, exceptionUseArray)
        //logger.info("bean47235_131080 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47235_131080))
        val txTypeBytesList = bean47235_131080.sr_21640_inst_list.asScala.map(_.sr_21641_inst_list.asScala.map(_.sr_21648_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes))))
          .flatten.flatten
        val txNewTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
        val txReTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).map(newtx => newtx._2.toLong).sum
        val txTotalTbSum = txTypeBytesList.map(newtx => newtx._2.toLong).sum

        val numerology = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21629_inst_list.asScala.toList.map(_.Numerology)).flatten
        val puschCarrierId = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.CarrierID)).flatten
        val puschHarq = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.sr_21648_inst_list.asScala.map(_.HARQID))).flatten.flatten
        val puschMcs = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.sr_21648_inst_list.asScala.toList.map(_.MCS))).flatten.flatten
        val puschNumRbs = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.sr_21648_inst_list.asScala.toList.map(_.NumRBs))).flatten.flatten
        val puschTbsize = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.sr_21648_inst_list.asScala.toList.map(_.TBSizebytes))).flatten.flatten
        val puschTxmode = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.sr_21648_inst_list.asScala.toList.map(_.TXMode))).flatten.flatten
        val puschTxtype = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.sr_21648_inst_list.asScala.toList.map(_.TXType))).flatten.flatten
        val puschBwpIdx = bean47235_131080.sr_21640_inst_list.asScala.toList.map(_.sr_21641_inst_list.asScala.toList.map(_.sr_21648_inst_list.asScala.toList.map(_.BWPIdx))).flatten.flatten

        val txTypes: List[String] = bean47235_131080.sr_21640_inst_list.asScala.flatMap(_.sr_21641_inst_list.asScala.flatMap(_.sr_21648_inst_list.asScala.map(_.TXType))).toList
        val numNEW_TX: Int = txTypes.count(_.equals("NEW_TX"))
        val numRE_TX: Int = txTypes.count(_.equals("RE_TX"))
        val carrierID_0 = bean47235_131080.sr_21640_inst_list.asScala.flatMap(_.sr_21641_inst_list.asScala.filter(_.CarrierID.equals("0")))
        val carrierID_1 = bean47235_131080.sr_21640_inst_list.asScala.flatMap(_.sr_21641_inst_list.asScala.filter(_.CarrierID.equals("1")))
        val numNEW_TXPCC: Int = carrierID_0.flatMap(_.sr_21648_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numNEW_TXSCC1: Int = carrierID_1.flatten(_.sr_21648_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numRE_TXPCC: Int = carrierID_0.flatMap(_.sr_21648_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val numRE_TXSCC1: Int = carrierID_1.flatMap(_.sr_21648_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val sr_27966_inst_list = bean47235_131080.sr_21640_inst_list.asScala.flatMap(_.sr_21641_inst_list.asScala.flatMap(_.sr_21648_inst_list.asScala)).toList
        val numRE_TXRV_0: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("0"))
        val numRE_TXRV_1: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("1"))
        val numRE_TXRV_2: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("2"))
        val numRE_TXRV_3: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("3"))

        val transformPrecodingPCC: String = Try(bean47235_131080.sr_21640_inst_list.asScala.flatMap(_.sr_21641_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("0")).flatMap(_.sr_21648_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val transformPrecodingSCC1: String = Try(bean47235_131080.sr_21640_inst_list.asScala.flatMap(_.sr_21641_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("1")).flatMap(_.sr_21648_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val NRPUSCHWaveformPCCVal = defineNRPUSCHWaveform(transformPrecodingPCC)
        val NRPUSCHWaveformSCC1Val = defineNRPUSCHWaveform(transformPrecodingSCC1)

        logRecordQComm5GObj = logRecordQComm5GObj.copy(txNewTbSum_0xB883 = txNewTbSum, txReTbSum_0xB883 = txReTbSum, txTotalSum_0xB883 = txTotalTbSum, carrierId_0XB883 = Try(puschCarrierId.map(_.toInt)).getOrElse(null), harq_0XB883 = Try(puschHarq.map(_.toInt)).getOrElse(null),
          numerology_0xB883 = numerology, mcs_0XB883 = Try(puschMcs.map(_.toInt)).getOrElse(null), numrbs_0XB883 = Try(puschNumRbs.map(_.toInt)).getOrElse(null), tbsize_0XB883 = Try(puschTbsize.map(_.toLong)).getOrElse(null),
          txtype_0XB883 = puschTxtype, txmode_0XB883 = puschTxmode, bwpidx_0XB883 = Try(puschBwpIdx.map(_.toInt)).getOrElse(null),
          numNEW_TX_0XB883 = numNEW_TX, numRE_TX_0XB883 = numRE_TX, numNEW_TXPCC_0XB883 = numNEW_TXPCC, numNEW_TXSCC1_0XB883 = numNEW_TXSCC1, numRE_TXPCC_0XB883 = numRE_TXPCC,
          numRE_TXSCC1_0XB883 = numRE_TXSCC1, numRE_TXRV_0_0XB883 = numRE_TXRV_0, numRE_TXRV_1_0XB883 = numRE_TXRV_1, numRE_TXRV_2_0XB883 = numRE_TXRV_2, numRE_TXRV_3_0XB883 = numRE_TXRV_3)

        logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRPUSCHWaveformPCC = NRPUSCHWaveformPCCVal, NRPUSCHWaveformSCC1 = NRPUSCHWaveformSCC1Val)

        logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47235", hexLogCode = "0xB883", logRecordQComm5G = logRecordQComm5GObj, logRecordQComm5G2 = logRecordQComm5G2Obj)

      case 131082 =>
        val bean47235_131082 = process47235_131082(x, exceptionUseArray)
        //logger.info("bean47235_131082 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47235_131082))
        val txTypeBytesList = bean47235_131082.sr_33877_inst_list.asScala.map(_.sr_33984_inst_list.asScala.map(_.sr_33981_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes))))
          .flatten.flatten
        val txNewTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
        val txReTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).map(newtx => newtx._2.toLong).sum
        val txTotalTbSum = txTypeBytesList.map(newtx => newtx._2.toLong).sum

        val numerology = bean47235_131082.sr_33877_inst_list.asScala.toList.map(_.sr_33972_inst_list.asScala.toList.map(_.Numerology)).flatten
        val puschCarrierId = bean47235_131082.sr_33877_inst_list.asScala.toList.map(_.sr_33984_inst_list.asScala.toList.map(_.CarrierID)).flatten
        val puschHarq = bean47235_131082.sr_33877_inst_list.asScala.toList.map(_.sr_33984_inst_list.asScala.toList.map(_.sr_33981_inst_list.asScala.map(_.HARQID))).flatten.flatten
        val puschMcs = bean47235_131082.sr_33877_inst_list.asScala.toList.map(_.sr_33984_inst_list.asScala.toList.map(_.sr_33981_inst_list.asScala.toList.map(_.MCS))).flatten.flatten
        val puschNumRbs = bean47235_131082.sr_33877_inst_list.asScala.toList.map(_.sr_33984_inst_list.asScala.toList.map(_.sr_33981_inst_list.asScala.toList.map(_.NumRBs))).flatten.flatten
        val puschTbsize = bean47235_131082.sr_33877_inst_list.asScala.toList.map(_.sr_33984_inst_list.asScala.toList.map(_.sr_33981_inst_list.asScala.toList.map(_.TBSizebytes))).flatten.flatten
        val puschTxmode = bean47235_131082.sr_33877_inst_list.asScala.toList.map(_.sr_33984_inst_list.asScala.toList.map(_.sr_33981_inst_list.asScala.toList.map(_.TXMode))).flatten.flatten
        val puschTxtype = bean47235_131082.sr_33877_inst_list.asScala.toList.map(_.sr_33984_inst_list.asScala.toList.map(_.sr_33981_inst_list.asScala.toList.map(_.TXType))).flatten.flatten
        val puschBwpIdx = bean47235_131082.sr_33877_inst_list.asScala.toList.map(_.sr_33984_inst_list.asScala.toList.map(_.sr_33981_inst_list.asScala.toList.map(_.BWPIdx))).flatten.flatten

        val txTypes: List[String] = bean47235_131082.sr_33877_inst_list.asScala.flatMap(_.sr_33984_inst_list.asScala.flatMap(_.sr_33981_inst_list.asScala.map(_.TXType))).toList
        val numNEW_TX: Int = txTypes.count(_.equals("NEW_TX"))
        val numRE_TX: Int = txTypes.count(_.equals("RE_TX"))
        val carrierID_0 = bean47235_131082.sr_33877_inst_list.asScala.flatMap(_.sr_33984_inst_list.asScala.filter(_.CarrierID.equals("0")))
        val carrierID_1 = bean47235_131082.sr_33877_inst_list.asScala.flatMap(_.sr_33984_inst_list.asScala.filter(_.CarrierID.equals("1")))
        val numNEW_TXPCC: Int = carrierID_0.flatMap(_.sr_33981_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numNEW_TXSCC1: Int = carrierID_1.flatMap(_.sr_33981_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numRE_TXPCC: Int = carrierID_0.flatMap(_.sr_33981_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val numRE_TXSCC1: Int = carrierID_1.flatMap(_.sr_33981_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val sr_27966_inst_list = bean47235_131082.sr_33877_inst_list.asScala.flatMap(_.sr_33984_inst_list.asScala.flatMap(_.sr_33981_inst_list.asScala)).toList
        val numRE_TXRV_0: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("0"))
        val numRE_TXRV_1: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("1"))
        val numRE_TXRV_2: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("2"))
        val numRE_TXRV_3: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("3"))

        val transformPrecodingPCC: String = Try(bean47235_131082.sr_33877_inst_list.asScala.flatMap(_.sr_33984_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("0")).flatMap(_.sr_33981_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val transformPrecodingSCC1: String = Try(bean47235_131082.sr_33877_inst_list.asScala.flatMap(_.sr_33984_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("1")).flatMap(_.sr_33981_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val NRPUSCHWaveformPCCVal = defineNRPUSCHWaveform(transformPrecodingPCC)
        val NRPUSCHWaveformSCC1Val = defineNRPUSCHWaveform(transformPrecodingSCC1)

        logRecordQComm5GObj = logRecordQComm5GObj.copy(txNewTbSum_0xB883 = txNewTbSum, txReTbSum_0xB883 = txReTbSum, txTotalSum_0xB883 = txTotalTbSum, carrierId_0XB883 = Try(puschCarrierId.map(_.toInt)).getOrElse(null), harq_0XB883 = Try(puschHarq.map(_.toInt)).getOrElse(null),
          numerology_0xB883 = numerology, mcs_0XB883 = Try(puschMcs.map(_.toInt)).getOrElse(null), numrbs_0XB883 = Try(puschNumRbs.map(_.toInt)).getOrElse(null), tbsize_0XB883 = Try(puschTbsize.map(_.toLong)).getOrElse(null),
          txtype_0XB883 = puschTxtype, txmode_0XB883 = puschTxmode, bwpidx_0XB883 = Try(puschBwpIdx.map(_.toInt)).getOrElse(null),
          numNEW_TX_0XB883 = numNEW_TX, numRE_TX_0XB883 = numRE_TX, numNEW_TXPCC_0XB883 = numNEW_TXPCC, numNEW_TXSCC1_0XB883 = numNEW_TXSCC1, numRE_TXPCC_0XB883 = numRE_TXPCC,
          numRE_TXSCC1_0XB883 = numRE_TXSCC1, numRE_TXRV_0_0XB883 = numRE_TXRV_0, numRE_TXRV_1_0XB883 = numRE_TXRV_1, numRE_TXRV_2_0XB883 = numRE_TXRV_2, numRE_TXRV_3_0XB883 = numRE_TXRV_3)

        logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRPUSCHWaveformPCC = NRPUSCHWaveformPCCVal, NRPUSCHWaveformSCC1 = NRPUSCHWaveformSCC1Val)

        logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47235", hexLogCode = "0xB883", logRecordQComm5G = logRecordQComm5GObj, logRecordQComm5G2 = logRecordQComm5G2Obj)

      case 131083 =>
        val bean47235_131083 = process47235_131083(x, exceptionUseArray)
        //logger.info("bean47235_131083 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47235_131083))
        val txTypeBytesList = bean47235_131083.sr_27932_inst_list.asScala.map(_.sr_27969_inst_list.asScala.map(_.sr_27966_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes)))).flatten.flatten
        val txNewTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
        val txReTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).map(newtx => newtx._2.toLong).sum
        val txTotalTbSum = txTypeBytesList.map(newtx => newtx._2.toLong).sum

        val numerology = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27950_inst_list.asScala.toList.map(_.Numerology)).flatten
        val puschCarrierId = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.CarrierID)).flatten
        val puschHarq = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.sr_27966_inst_list.asScala.toList.map(_.HARQID))).flatten.flatten
        val puschMcs = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.sr_27966_inst_list.asScala.toList.map(_.MCS))).flatten.flatten
        val puschNumRbs = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.sr_27966_inst_list.asScala.toList.map(_.NumRBs))).flatten.flatten
        val puschTbsize = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.sr_27966_inst_list.asScala.toList.map(_.TBSizebytes))).flatten.flatten
        val puschTxmode = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.sr_27966_inst_list.asScala.toList.map(_.TXMode))).flatten.flatten
        val puschTxtype = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.sr_27966_inst_list.asScala.toList.map(_.TXType))).flatten.flatten
        val puschBwpIdx = bean47235_131083.sr_27932_inst_list.asScala.toList.map(_.sr_27969_inst_list.asScala.toList.map(_.sr_27966_inst_list.asScala.toList.map(_.BWPIdx))).flatten.flatten

        val txTypes: List[String] = bean47235_131083.sr_27932_inst_list.asScala.flatMap(_.sr_27969_inst_list.asScala.flatMap(_.sr_27966_inst_list.asScala.map(_.TXType))).toList
        val numNEW_TX: Int = txTypes.count(_.equals("NEW_TX"))
        val numRE_TX: Int = txTypes.count(_.equals("RE_TX"))
        val carrierID_0 = bean47235_131083.sr_27932_inst_list.asScala.flatMap(_.sr_27969_inst_list.asScala.filter(_.CarrierID.equals("0")))
        val carrierID_1 = bean47235_131083.sr_27932_inst_list.asScala.flatMap(_.sr_27969_inst_list.asScala.filter(_.CarrierID.equals("1")))
        val numNEW_TXPCC: Int = carrierID_0.flatMap(_.sr_27966_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numNEW_TXSCC1: Int = carrierID_1.flatMap(_.sr_27966_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numRE_TXPCC: Int = carrierID_0.flatMap(_.sr_27966_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val numRE_TXSCC1: Int = carrierID_1.flatMap(_.sr_27966_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val sr_27966_inst_list = bean47235_131083.sr_27932_inst_list.asScala.flatMap(_.sr_27969_inst_list.asScala.flatMap(_.sr_27966_inst_list.asScala)).toList
        val numRE_TXRV_0: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("0"))
        val numRE_TXRV_1: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("1"))
        val numRE_TXRV_2: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("2"))
        val numRE_TXRV_3: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("3"))

        val transformPrecodingPCC: String = Try(bean47235_131083.sr_27932_inst_list.asScala.flatMap(_.sr_27969_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("0")).flatMap(_.sr_27966_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val transformPrecodingSCC1: String = Try(bean47235_131083.sr_27932_inst_list.asScala.flatMap(_.sr_27969_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("1")).flatMap(_.sr_27966_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val NRPUSCHWaveformPCCVal = defineNRPUSCHWaveform(transformPrecodingPCC)
        val NRPUSCHWaveformSCC1Val = defineNRPUSCHWaveform(transformPrecodingSCC1)

        logRecordQComm5GObj = logRecordQComm5GObj.copy(txNewTbSum_0xB883 = txNewTbSum, txReTbSum_0xB883 = txReTbSum, txTotalSum_0xB883 = txTotalTbSum, carrierId_0XB883 = Try(puschCarrierId.map(_.toInt)).getOrElse(null), harq_0XB883 = Try(puschHarq.map(_.toInt)).getOrElse(null),
          numerology_0xB883 = numerology, mcs_0XB883 = Try(puschMcs.map(_.toInt)).getOrElse(null), numrbs_0XB883 = Try(puschNumRbs.map(_.toInt)).getOrElse(null), tbsize_0XB883 = Try(puschTbsize.map(_.toLong)).getOrElse(null),
          txtype_0XB883 = puschTxtype, txmode_0XB883 = puschTxmode, bwpidx_0XB883 = Try(puschBwpIdx.map(_.toInt)).getOrElse(null),
          numNEW_TX_0XB883 = numNEW_TX, numRE_TX_0XB883 = numRE_TX, numNEW_TXPCC_0XB883 = numNEW_TXPCC, numNEW_TXSCC1_0XB883 = numNEW_TXSCC1, numRE_TXPCC_0XB883 = numRE_TXPCC,
          numRE_TXSCC1_0XB883 = numRE_TXSCC1, numRE_TXRV_0_0XB883 = numRE_TXRV_0, numRE_TXRV_1_0XB883 = numRE_TXRV_1, numRE_TXRV_2_0XB883 = numRE_TXRV_2, numRE_TXRV_3_0XB883 = numRE_TXRV_3)

        logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRPUSCHWaveformPCC = NRPUSCHWaveformPCCVal, NRPUSCHWaveformSCC1 = NRPUSCHWaveformSCC1Val)

        logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47235", hexLogCode = "0xB883", logRecordQComm5G = logRecordQComm5GObj, logRecordQComm5G2 = logRecordQComm5G2Obj)

      case 131085 =>
        val bean47235_131085 = process47235_131085(x, exceptionUseArray)
        //logger.info("bean47235_131085 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47235_131085))
        val txTypeBytesList = bean47235_131085.sr_46642_inst_list.asScala.map(_.sr_46636_inst_list.asScala.map(_.sr_46640_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes)))).flatten.flatten
        val txNewTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
        val txReTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).map(newtx => newtx._2.toLong).sum
        val txTotalTbSum = txTypeBytesList.map(newtx => newtx._2.toLong).sum

        val numerology = bean47235_131085.sr_46642_inst_list.asScala.toList.map(_.sr_46624_inst_list.asScala.toList.map(_.SCS)).flatten //Numerology field has been replaced with SCS
        val puschCarrierId = bean47235_131085.sr_46642_inst_list.asScala.toList.map(_.sr_46636_inst_list.asScala.toList.map(_.CarrierID)).flatten
        val puschHarq = bean47235_131085.sr_46642_inst_list.asScala.toList.map(_.sr_46636_inst_list.asScala.toList.map(_.sr_46640_inst_list.asScala.toList.map(_.HARQID))).flatten.flatten
        val puschMcs = bean47235_131085.sr_46642_inst_list.asScala.toList.map(_.sr_46636_inst_list.asScala.toList.map(_.sr_46640_inst_list.asScala.toList.map(_.MCS))).flatten.flatten
        val puschNumRbs = bean47235_131085.sr_46642_inst_list.asScala.toList.map(_.sr_46636_inst_list.asScala.toList.map(_.sr_46640_inst_list.asScala.toList.map(_.NumRBs))).flatten.flatten
        val puschTbsize = bean47235_131085.sr_46642_inst_list.asScala.toList.map(_.sr_46636_inst_list.asScala.toList.map(_.sr_46640_inst_list.asScala.toList.map(_.TBSizebytes))).flatten.flatten
        val puschTxmode = bean47235_131085.sr_46642_inst_list.asScala.toList.map(_.sr_46636_inst_list.asScala.toList.map(_.sr_46640_inst_list.asScala.toList.map(_.TXMode))).flatten.flatten
        val puschTxtype = bean47235_131085.sr_46642_inst_list.asScala.toList.map(_.sr_46636_inst_list.asScala.toList.map(_.sr_46640_inst_list.asScala.toList.map(_.TXType))).flatten.flatten
        val puschBwpIdx = bean47235_131085.sr_46642_inst_list.asScala.toList.map(_.sr_46636_inst_list.asScala.toList.map(_.sr_46640_inst_list.asScala.toList.map(_.BWPId))).flatten.flatten

        val txTypes: List[String] = bean47235_131085.sr_46642_inst_list.asScala.flatMap(_.sr_46636_inst_list.asScala.flatMap(_.sr_46640_inst_list.asScala.map(_.TXType))).toList
        val numNEW_TX: Int = txTypes.count(_.equals("NEW_TX"))
        val numRE_TX: Int = txTypes.count(_.equals("RE_TX"))
        val carrierID_0 = bean47235_131085.sr_46642_inst_list.asScala.flatMap(_.sr_46636_inst_list.asScala.filter(_.CarrierID.equals("0")))
        val carrierID_1 = bean47235_131085.sr_46642_inst_list.asScala.flatMap(_.sr_46636_inst_list.asScala.filter(_.CarrierID.equals("1")))
        val numNEW_TXPCC: Int = carrierID_0.flatMap(_.sr_46640_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numNEW_TXSCC1: Int = carrierID_1.flatMap(_.sr_46640_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numRE_TXPCC: Int = carrierID_0.flatMap(_.sr_46640_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val numRE_TXSCC1: Int = carrierID_1.flatMap(_.sr_46640_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val sr_27966_inst_list = bean47235_131085.sr_46642_inst_list.asScala.flatMap(_.sr_46636_inst_list.asScala.flatMap(_.sr_46640_inst_list.asScala)).toList
        val numRE_TXRV_0: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("0"))
        val numRE_TXRV_1: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("1"))
        val numRE_TXRV_2: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("2"))
        val numRE_TXRV_3: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("3"))

        val transformPrecodingPCC: String = Try(bean47235_131085.sr_46642_inst_list.asScala.flatMap(_.sr_46636_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("0")).flatMap(_.sr_46640_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val transformPrecodingSCC1: String = Try(bean47235_131085.sr_46642_inst_list.asScala.flatMap(_.sr_46636_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("1")).flatMap(_.sr_46640_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val NRPUSCHWaveformPCCVal = defineNRPUSCHWaveform(transformPrecodingPCC)
        val NRPUSCHWaveformSCC1Val = defineNRPUSCHWaveform(transformPrecodingSCC1)

        logRecordQComm5GObj = logRecordQComm5GObj.copy(txNewTbSum_0xB883 = txNewTbSum, txReTbSum_0xB883 = txReTbSum, txTotalSum_0xB883 = txTotalTbSum, carrierId_0XB883 = Try(puschCarrierId.map(_.toInt)).getOrElse(null), harq_0XB883 = Try(puschHarq.map(_.toInt)).getOrElse(null),
          numerology_0xB883 = numerology, mcs_0XB883 = Try(puschMcs.map(_.toInt)).getOrElse(null), numrbs_0XB883 = Try(puschNumRbs.map(_.toInt)).getOrElse(null), tbsize_0XB883 = Try(puschTbsize.map(_.toLong)).getOrElse(null),
          txtype_0XB883 = puschTxtype, txmode_0XB883 = puschTxmode, bwpidx_0XB883 = Try(puschBwpIdx.map(_.toInt)).getOrElse(null),
          numNEW_TX_0XB883 = numNEW_TX, numRE_TX_0XB883 = numRE_TX, numNEW_TXPCC_0XB883 = numNEW_TXPCC, numNEW_TXSCC1_0XB883 = numNEW_TXSCC1, numRE_TXPCC_0XB883 = numRE_TXPCC,
          numRE_TXSCC1_0XB883 = numRE_TXSCC1, numRE_TXRV_0_0XB883 = numRE_TXRV_0, numRE_TXRV_1_0XB883 = numRE_TXRV_1, numRE_TXRV_2_0XB883 = numRE_TXRV_2, numRE_TXRV_3_0XB883 = numRE_TXRV_3)

        logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRPUSCHWaveformPCC = NRPUSCHWaveformPCCVal, NRPUSCHWaveformSCC1 = NRPUSCHWaveformSCC1Val)

        logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47235", hexLogCode = "0xB883", logRecordQComm5G = logRecordQComm5GObj, logRecordQComm5G2 = logRecordQComm5G2Obj)

      case 131086 =>
        val bean47235_131086 = process47235_131086(x, exceptionUseArray)
        //logger.info("bean47235_131086 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47235_131086))
        val txTypeBytesList = bean47235_131086.sr_48976_inst_list.asScala.map(_.sr_48957_inst_list.asScala.map(_.sr_48969_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes)))).flatten.flatten
        val txNewTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
        val txReTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).map(newtx => newtx._2.toLong).sum
        val txTotalTbSum = txTypeBytesList.map(newtx => newtx._2.toLong).sum

        val numerology = bean47235_131086.sr_48976_inst_list.asScala.toList.map(_.sr_48950_inst_list.asScala.toList.map(_.SCS)).flatten //Numerology field has been replaced with SCS
        val puschCarrierId = bean47235_131086.sr_48976_inst_list.asScala.toList.map(_.sr_48957_inst_list.asScala.toList.map(_.CarrierID)).flatten
        val puschHarq = bean47235_131086.sr_48976_inst_list.asScala.toList.map(_.sr_48957_inst_list.asScala.toList.map(_.sr_48969_inst_list.asScala.toList.map(_.HARQID))).flatten.flatten
        val puschMcs = bean47235_131086.sr_48976_inst_list.asScala.toList.map(_.sr_48957_inst_list.asScala.toList.map(_.sr_48969_inst_list.asScala.toList.map(_.MCS))).flatten.flatten
        val puschNumRbs = bean47235_131086.sr_48976_inst_list.asScala.toList.map(_.sr_48957_inst_list.asScala.toList.map(_.sr_48969_inst_list.asScala.toList.map(_.NumRBs))).flatten.flatten
        val puschTbsize = bean47235_131086.sr_48976_inst_list.asScala.toList.map(_.sr_48957_inst_list.asScala.toList.map(_.sr_48969_inst_list.asScala.toList.map(_.TBSizebytes))).flatten.flatten
        val puschTxmode = bean47235_131086.sr_48976_inst_list.asScala.toList.map(_.sr_48957_inst_list.asScala.toList.map(_.sr_48969_inst_list.asScala.toList.map(_.TXMode))).flatten.flatten
        val puschTxtype = bean47235_131086.sr_48976_inst_list.asScala.toList.map(_.sr_48957_inst_list.asScala.toList.map(_.sr_48969_inst_list.asScala.toList.map(_.TXType))).flatten.flatten
        val puschBwpIdx = bean47235_131086.sr_48976_inst_list.asScala.toList.map(_.sr_48957_inst_list.asScala.toList.map(_.sr_48969_inst_list.asScala.toList.map(_.BWPId))).flatten.flatten

        val txTypes: List[String] = bean47235_131086.sr_48976_inst_list.asScala.flatMap(_.sr_48957_inst_list.asScala.flatMap(_.sr_48969_inst_list.asScala.map(_.TXType))).toList
        val numNEW_TX: Int = txTypes.count(_.equals("NEW_TX"))
        val numRE_TX: Int = txTypes.count(_.equals("RE_TX"))
        val carrierID_0 = bean47235_131086.sr_48976_inst_list.asScala.flatMap(_.sr_48957_inst_list.asScala.filter(_.CarrierID.equals("0")))
        val carrierID_1 = bean47235_131086.sr_48976_inst_list.asScala.flatMap(_.sr_48957_inst_list.asScala.filter(_.CarrierID.equals("1")))
        val numNEW_TXPCC: Int = carrierID_0.flatMap(_.sr_48969_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numNEW_TXSCC1: Int = carrierID_1.flatMap(_.sr_48969_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numRE_TXPCC: Int = carrierID_0.flatMap(_.sr_48969_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val numRE_TXSCC1: Int = carrierID_1.flatMap(_.sr_48969_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val sr_27966_inst_list = bean47235_131086.sr_48976_inst_list.asScala.flatMap(_.sr_48957_inst_list.asScala.flatMap(_.sr_48969_inst_list.asScala)).toList
        val numRE_TXRV_0: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("0"))
        val numRE_TXRV_1: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("1"))
        val numRE_TXRV_2: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("2"))
        val numRE_TXRV_3: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("3"))

        val transformPrecodingPCC: String = Try(bean47235_131086.sr_48976_inst_list.asScala.flatMap(_.sr_48957_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("0")).flatMap(_.sr_48969_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val transformPrecodingSCC1: String = Try(bean47235_131086.sr_48976_inst_list.asScala.flatMap(_.sr_48957_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("1")).flatMap(_.sr_48969_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val NRPUSCHWaveformPCCVal = defineNRPUSCHWaveform(transformPrecodingPCC)
        val NRPUSCHWaveformSCC1Val = defineNRPUSCHWaveform(transformPrecodingSCC1)

        logRecordQComm5GObj = logRecordQComm5GObj.copy(txNewTbSum_0xB883 = txNewTbSum, txReTbSum_0xB883 = txReTbSum, txTotalSum_0xB883 = txTotalTbSum, carrierId_0XB883 = Try(puschCarrierId.map(_.toInt)).getOrElse(null), harq_0XB883 = Try(puschHarq.map(_.toInt)).getOrElse(null)
          , numerology_0xB883 = numerology, mcs_0XB883 = Try(puschMcs.map(_.toInt)).getOrElse(null), numrbs_0XB883 = Try(puschNumRbs.map(_.toInt)).getOrElse(null), tbsize_0XB883 = Try(puschTbsize.map(_.toLong)).getOrElse(null),
          txtype_0XB883 = puschTxtype, txmode_0XB883 = puschTxmode, bwpidx_0XB883 = Try(puschBwpIdx.map(_.toInt)).getOrElse(null),
          numNEW_TX_0XB883 = numNEW_TX, numRE_TX_0XB883 = numRE_TX, numNEW_TXPCC_0XB883 = numNEW_TXPCC, numNEW_TXSCC1_0XB883 = numNEW_TXSCC1, numRE_TXPCC_0XB883 = numRE_TXPCC,
          numRE_TXSCC1_0XB883 = numRE_TXSCC1, numRE_TXRV_0_0XB883 = numRE_TXRV_0, numRE_TXRV_1_0XB883 = numRE_TXRV_1, numRE_TXRV_2_0XB883 = numRE_TXRV_2, numRE_TXRV_3_0XB883 = numRE_TXRV_3)

        logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRPUSCHWaveformPCC = NRPUSCHWaveformPCCVal, NRPUSCHWaveformSCC1 = NRPUSCHWaveformSCC1Val)

        logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47235", hexLogCode = "0xB883", logRecordQComm5G = logRecordQComm5GObj, logRecordQComm5G2 = logRecordQComm5G2Obj)

      case 131087 =>
        val bean47235_131087 = process47235_131087(x, exceptionUseArray)
        //logger.info("bean47235_131087 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47235_131087))
        val txTypeBytesList = bean47235_131087.sr_56933_inst_list.asScala.map(_.sr_56934_inst_list.asScala.map(_.sr_56935_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes)))).flatten.flatten
        val txNewTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
        val txReTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).map(newtx => newtx._2.toLong).sum
        val txTotalTbSum = txTypeBytesList.map(newtx => newtx._2.toLong).sum

        val numerology = bean47235_131087.sr_56933_inst_list.asScala.toList.map(_.sr_56924_inst_list.asScala.toList.map(_.SCS)).flatten //Numerology field has been replaced with SCS
        val puschCarrierId = bean47235_131087.sr_56933_inst_list.asScala.toList.map(_.sr_56934_inst_list.asScala.toList.map(_.CarrierID)).flatten
        val puschHarq = bean47235_131087.sr_56933_inst_list.asScala.toList.map(_.sr_56934_inst_list.asScala.toList.map(_.sr_56935_inst_list.asScala.toList.map(_.HARQID))).flatten.flatten
        val puschMcs = bean47235_131087.sr_56933_inst_list.asScala.toList.map(_.sr_56934_inst_list.asScala.toList.map(_.sr_56935_inst_list.asScala.toList.map(_.MCS))).flatten.flatten
        val puschNumRbs = bean47235_131087.sr_56933_inst_list.asScala.toList.map(_.sr_56934_inst_list.asScala.toList.map(_.sr_56935_inst_list.asScala.toList.map(_.NumRBs))).flatten.flatten
        val puschTbsize = bean47235_131087.sr_56933_inst_list.asScala.toList.map(_.sr_56934_inst_list.asScala.toList.map(_.sr_56935_inst_list.asScala.toList.map(_.TBSizebytes))).flatten.flatten
        val puschTxmode = bean47235_131087.sr_56933_inst_list.asScala.toList.map(_.sr_56934_inst_list.asScala.toList.map(_.sr_56935_inst_list.asScala.toList.map(_.TXMode))).flatten.flatten
        val puschTxtype = bean47235_131087.sr_56933_inst_list.asScala.toList.map(_.sr_56934_inst_list.asScala.toList.map(_.sr_56935_inst_list.asScala.toList.map(_.TXType))).flatten.flatten
        val puschBwpIdx = bean47235_131087.sr_56933_inst_list.asScala.toList.map(_.sr_56934_inst_list.asScala.toList.map(_.sr_56935_inst_list.asScala.toList.map(_.BWPId))).flatten.flatten

        val txTypes: List[String] = bean47235_131087.sr_56933_inst_list.asScala.flatMap(_.sr_56934_inst_list.asScala.flatMap(_.sr_56935_inst_list.asScala.map(_.TXType))).toList
        val numNEW_TX: Int = txTypes.count(_.equals("NEW_TX"))
        val numRE_TX: Int = txTypes.count(_.equals("RE_TX"))
        val carrierID_0 = bean47235_131087.sr_56933_inst_list.asScala.flatMap(_.sr_56934_inst_list.asScala.filter(_.CarrierID.equals("0")))
        val carrierID_1 = bean47235_131087.sr_56933_inst_list.asScala.flatMap(_.sr_56934_inst_list.asScala.filter(_.CarrierID.equals("1")))
        val numNEW_TXPCC: Int = carrierID_0.flatMap(_.sr_56935_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numNEW_TXSCC1: Int = carrierID_1.flatMap(_.sr_56935_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numRE_TXPCC: Int = carrierID_0.flatMap(_.sr_56935_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val numRE_TXSCC1: Int = carrierID_1.flatMap(_.sr_56935_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val sr_27966_inst_list = bean47235_131087.sr_56933_inst_list.asScala.flatMap(_.sr_56934_inst_list.asScala.flatMap(_.sr_56935_inst_list.asScala)).toList
        val numRE_TXRV_0: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("0"))
        val numRE_TXRV_1: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("1"))
        val numRE_TXRV_2: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("2"))
        val numRE_TXRV_3: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("3"))

        val transformPrecodingPCC: String = Try(bean47235_131087.sr_56933_inst_list.asScala.flatMap(_.sr_56934_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("0")).flatMap(_.sr_56935_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val transformPrecodingSCC1: String = Try(bean47235_131087.sr_56933_inst_list.asScala.flatMap(_.sr_56934_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("1")).flatMap(_.sr_56935_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val NRPUSCHWaveformPCCVal = defineNRPUSCHWaveform(transformPrecodingPCC)
        val NRPUSCHWaveformSCC1Val = defineNRPUSCHWaveform(transformPrecodingSCC1)

        logRecordQComm5GObj = logRecordQComm5GObj.copy(txNewTbSum_0xB883 = txNewTbSum, txReTbSum_0xB883 = txReTbSum, txTotalSum_0xB883 = txTotalTbSum, carrierId_0XB883 = Try(puschCarrierId.map(_.toInt)).getOrElse(null), harq_0XB883 = Try(puschHarq.map(_.toInt)).getOrElse(null),
          numerology_0xB883 = numerology, mcs_0XB883 = Try(puschMcs.map(_.toInt)).getOrElse(null), numrbs_0XB883 = Try(puschNumRbs.map(_.toInt)).getOrElse(null), tbsize_0XB883 = Try(puschTbsize.map(_.toLong)).getOrElse(null),
          txtype_0XB883 = puschTxtype, txmode_0XB883 = puschTxmode, bwpidx_0XB883 = Try(puschBwpIdx.map(_.toInt)).getOrElse(null),
          numNEW_TX_0XB883 = numNEW_TX, numRE_TX_0XB883 = numRE_TX, numNEW_TXPCC_0XB883 = numNEW_TXPCC, numNEW_TXSCC1_0XB883 = numNEW_TXSCC1, numRE_TXPCC_0XB883 = numRE_TXPCC,
          numRE_TXSCC1_0XB883 = numRE_TXSCC1, numRE_TXRV_0_0XB883 = numRE_TXRV_0, numRE_TXRV_1_0XB883 = numRE_TXRV_1, numRE_TXRV_2_0XB883 = numRE_TXRV_2, numRE_TXRV_3_0XB883 = numRE_TXRV_3)

        logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRPUSCHWaveformPCC = NRPUSCHWaveformPCCVal, NRPUSCHWaveformSCC1 = NRPUSCHWaveformSCC1Val)

        logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47235", hexLogCode = "0xB883", logRecordQComm5G = logRecordQComm5GObj, logRecordQComm5G2 = logRecordQComm5G2Obj)

      case 131089 =>
        val bean47235_131089 = process47235_131089(x, exceptionUseArray)
        //logger.info("bean47235_131089 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47235_131089))
        val txTypeBytesList = bean47235_131089.sr_53437_inst_list.asScala.map(_.sr_53493_inst_list.asScala.map(_.sr_53481_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes)))).flatten.flatten
        val txNewTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
        val txReTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).map(newtx => newtx._2.toLong).sum
        val txTotalTbSum = txTypeBytesList.map(newtx => newtx._2.toLong).sum

        val numerology = bean47235_131089.sr_53437_inst_list.asScala.toList.map(_.sr_53480_inst_list.asScala.toList.map(_.SCS)).flatten //Numerology field has been replaced with SCS
        val puschCarrierId = bean47235_131089.sr_53437_inst_list.asScala.toList.map(_.sr_53493_inst_list.asScala.toList.map(_.CarrierID)).flatten
        val puschHarq = bean47235_131089.sr_53437_inst_list.asScala.toList.map(_.sr_53493_inst_list.asScala.toList.map(_.sr_53481_inst_list.asScala.toList.map(_.HARQID))).flatten.flatten
        val puschMcs = bean47235_131089.sr_53437_inst_list.asScala.toList.map(_.sr_53493_inst_list.asScala.toList.map(_.sr_53481_inst_list.asScala.toList.map(_.MCS))).flatten.flatten
        val puschNumRbs = bean47235_131089.sr_53437_inst_list.asScala.toList.map(_.sr_53493_inst_list.asScala.toList.map(_.sr_53481_inst_list.asScala.toList.map(_.NumRBs))).flatten.flatten
        val puschTbsize = bean47235_131089.sr_53437_inst_list.asScala.toList.map(_.sr_53493_inst_list.asScala.toList.map(_.sr_53481_inst_list.asScala.toList.map(_.TBSizebytes))).flatten.flatten
        val puschTxmode = bean47235_131089.sr_53437_inst_list.asScala.toList.map(_.sr_53493_inst_list.asScala.toList.map(_.sr_53481_inst_list.asScala.toList.map(_.TXMode))).flatten.flatten
        val puschTxtype = bean47235_131089.sr_53437_inst_list.asScala.toList.map(_.sr_53493_inst_list.asScala.toList.map(_.sr_53481_inst_list.asScala.toList.map(_.TXType))).flatten.flatten
        val puschBwpIdx = bean47235_131089.sr_53437_inst_list.asScala.toList.map(_.sr_53493_inst_list.asScala.toList.map(_.sr_53481_inst_list.asScala.toList.map(_.BWPId))).flatten.flatten

        val txTypes: List[String] = bean47235_131089.sr_53437_inst_list.asScala.flatMap(_.sr_53493_inst_list.asScala.flatMap(_.sr_53481_inst_list.asScala.map(_.TXType))).toList
        val numNEW_TX: Int = txTypes.count(_.equals("NEW_TX"))
        val numRE_TX: Int = txTypes.count(_.equals("RE_TX"))
        val carrierID_0 = bean47235_131089.sr_53437_inst_list.asScala.flatMap(_.sr_53493_inst_list.asScala.filter(_.CarrierID.equals("0"))).toList
        val carrierID_1 = bean47235_131089.sr_53437_inst_list.asScala.flatMap(_.sr_53493_inst_list.asScala.filter(_.CarrierID.equals("1"))).toList
        val numNEW_TXPCC: Int = carrierID_0.flatMap(_.sr_53481_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numNEW_TXSCC1: Int = carrierID_1.flatMap(_.sr_53481_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numRE_TXPCC: Int = carrierID_0.flatMap(_.sr_53481_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val numRE_TXSCC1: Int = carrierID_1.flatMap(_.sr_53481_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val sr_27966_inst_list = bean47235_131089.sr_53437_inst_list.asScala.flatMap(_.sr_53493_inst_list.asScala.flatMap(_.sr_53481_inst_list.asScala)).toList
        val numRE_TXRV_0: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("0"))
        val numRE_TXRV_1: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("1"))
        val numRE_TXRV_2: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("2"))
        val numRE_TXRV_3: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("3"))

        val transformPrecodingPCC: String = Try(bean47235_131089.sr_53437_inst_list.asScala.flatMap(_.sr_53493_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("0")).flatMap(_.sr_53481_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val transformPrecodingSCC1: String = Try(bean47235_131089.sr_53437_inst_list.asScala.flatMap(_.sr_53493_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("1")).flatMap(_.sr_53481_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val NRPUSCHWaveformPCCVal = defineNRPUSCHWaveform(transformPrecodingPCC)
        val NRPUSCHWaveformSCC1Val = defineNRPUSCHWaveform(transformPrecodingSCC1)

        logRecordQComm5GObj = logRecordQComm5GObj.copy(txNewTbSum_0xB883 = txNewTbSum, txReTbSum_0xB883 = txReTbSum, txTotalSum_0xB883 = txTotalTbSum, carrierId_0XB883 = Try(puschCarrierId.map(_.toInt)).getOrElse(null), harq_0XB883 = Try(puschHarq.map(_.toInt)).getOrElse(null)
          , numerology_0xB883 = numerology, mcs_0XB883 = Try(puschMcs.map(_.toInt)).getOrElse(null), numrbs_0XB883 = Try(puschNumRbs.map(_.toInt)).getOrElse(null), tbsize_0XB883 = Try(puschTbsize.map(_.toLong)).getOrElse(null),
          txtype_0XB883 = puschTxtype, txmode_0XB883 = puschTxmode, bwpidx_0XB883 = Try(puschBwpIdx.map(_.toInt)).getOrElse(null),
          numNEW_TX_0XB883 = numNEW_TX, numRE_TX_0XB883 = numRE_TX, numNEW_TXPCC_0XB883 = numNEW_TXPCC, numNEW_TXSCC1_0XB883 = numNEW_TXSCC1, numRE_TXPCC_0XB883 = numRE_TXPCC,
          numRE_TXSCC1_0XB883 = numRE_TXSCC1, numRE_TXRV_0_0XB883 = numRE_TXRV_0, numRE_TXRV_1_0XB883 = numRE_TXRV_1, numRE_TXRV_2_0XB883 = numRE_TXRV_2, numRE_TXRV_3_0XB883 = numRE_TXRV_3)

        logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRPUSCHWaveformPCC = NRPUSCHWaveformPCCVal, NRPUSCHWaveformSCC1 = NRPUSCHWaveformSCC1Val)

        logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47235", hexLogCode = "0xB883", logRecordQComm5G = logRecordQComm5GObj, logRecordQComm5G2 = logRecordQComm5G2Obj)

      case 196618 =>
        val bean47235_196618 = process47235_196618(x, exceptionUseArray)
        val txTypeBytesList = bean47235_196618.sr_58430_inst_list.asScala.flatMap(_.sr_58434_inst_list.asScala.flatMap(_.sr_58480_inst_list.asScala.map(obj => (obj.getTXType, obj.TBSizebytes))))
        val txNewTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("NEW_TX")).map(newtx => newtx._2.toLong).sum
        val txReTbSum = txTypeBytesList.filter(tx => tx._1.toString().equalsIgnoreCase("RE_TX")).map(newtx => newtx._2.toLong).sum
        val txTotalTbSum = txTypeBytesList.map(newtx => newtx._2.toLong).sum

        val numerology = bean47235_196618.sr_58430_inst_list.asScala.toList.flatMap(_.sr_58431_inst_list.asScala.toList.map(_.SCS)) //Numerology field has been replaced with SCS
        val puschCarrierId = bean47235_196618.sr_58430_inst_list.asScala.toList.flatMap(_.sr_58434_inst_list.asScala.toList.map(_.CarrierID))
        val puschHarq = bean47235_196618.sr_58430_inst_list.asScala.toList.flatMap(_.sr_58434_inst_list.asScala.toList.flatMap(_.sr_58480_inst_list.asScala.toList.map(_.HARQID)))
        val puschMcs = bean47235_196618.sr_58430_inst_list.asScala.toList.flatMap(_.sr_58434_inst_list.asScala.toList.flatMap(_.sr_58480_inst_list.asScala.toList.map(_.MCS)))
        val puschNumRbs = bean47235_196618.sr_58430_inst_list.asScala.toList.flatMap(_.sr_58434_inst_list.asScala.toList.flatMap(_.sr_58480_inst_list.asScala.toList.map(_.NumRBs)))
        val puschTbsize = bean47235_196618.sr_58430_inst_list.asScala.toList.flatMap(_.sr_58434_inst_list.asScala.toList.flatMap(_.sr_58480_inst_list.asScala.toList.map(_.TBSizebytes)))
        val puschTxmode = bean47235_196618.sr_58430_inst_list.asScala.toList.flatMap(_.sr_58434_inst_list.asScala.toList.flatMap(_.sr_58480_inst_list.asScala.toList.map(_.TXMode)))
        val puschTxtype = bean47235_196618.sr_58430_inst_list.asScala.toList.flatMap(_.sr_58434_inst_list.asScala.toList.flatMap(_.sr_58480_inst_list.asScala.toList.map(_.TXType)))
        val puschBwpIdx = bean47235_196618.sr_58430_inst_list.asScala.toList.flatMap(_.sr_58434_inst_list.asScala.toList.flatMap(_.sr_58480_inst_list.asScala.toList.map(_.BWPId)))

        val txTypes: List[String] = bean47235_196618.sr_58430_inst_list.asScala.flatMap(_.sr_58434_inst_list.asScala.flatMap(_.sr_58480_inst_list.asScala.map(_.TXType))).toList
        val numNEW_TX: Int = txTypes.count(_.equals("NEW_TX"))
        val numRE_TX: Int = txTypes.count(_.equals("RE_TX"))
        val carrierID_0 = bean47235_196618.sr_58430_inst_list.asScala.flatMap(_.sr_58434_inst_list.asScala.filter(_.CarrierID.equals("0"))).toList
        val carrierID_1 = bean47235_196618.sr_58430_inst_list.asScala.flatMap(_.sr_58434_inst_list.asScala.filter(_.CarrierID.equals("1"))).toList
        val numNEW_TXPCC: Int = carrierID_0.flatMap(_.sr_58480_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numNEW_TXSCC1: Int = carrierID_1.flatMap(_.sr_58480_inst_list.asScala.map(_.TXType)).count(_.equals("NEW_TX"))
        val numRE_TXPCC: Int = carrierID_0.flatMap(_.sr_58480_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val numRE_TXSCC1: Int = carrierID_1.flatMap(_.sr_58480_inst_list.asScala.map(_.TXType)).count(_.equals("RE_TX"))
        val sr_27966_inst_list = bean47235_196618.sr_58430_inst_list.asScala.flatMap(_.sr_58434_inst_list.asScala.flatMap(_.sr_58480_inst_list.asScala)).toList
        val numRE_TXRV_0: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("0"))
        val numRE_TXRV_1: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("1"))
        val numRE_TXRV_2: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("2"))
        val numRE_TXRV_3: Int = sr_27966_inst_list.count(e => e.TXType.equals("RE_TX") && e.RVIndex.equals("3"))

        val transformPrecodingPCC: String = Try(bean47235_196618.sr_58430_inst_list.asScala.flatMap(_.sr_58434_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("0")).flatMap(_.sr_58480_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val transformPrecodingSCC1: String = Try(bean47235_196618.sr_58430_inst_list.asScala.flatMap(_.sr_58434_inst_list.asScala.filter(_.CarrierID.equalsIgnoreCase("1")).flatMap(_.sr_58480_inst_list.asScala.map(_.TransformPrecoding))).toList.head.trim).getOrElse(null)
        val NRPUSCHWaveformPCCVal = defineNRPUSCHWaveform(transformPrecodingPCC)
        val NRPUSCHWaveformSCC1Val = defineNRPUSCHWaveform(transformPrecodingSCC1)

        logRecordQComm5GObj = logRecordQComm5GObj.copy(txNewTbSum_0xB883 = txNewTbSum, txReTbSum_0xB883 = txReTbSum, txTotalSum_0xB883 = txTotalTbSum, carrierId_0XB883 = Try(puschCarrierId.map(_.toInt)).getOrElse(null), harq_0XB883 = Try(puschHarq.map(_.toInt)).getOrElse(null)
          , numerology_0xB883 = numerology, mcs_0XB883 = Try(puschMcs.map(_.toInt)).getOrElse(null), numrbs_0XB883 = Try(puschNumRbs.map(_.toInt)).getOrElse(null), tbsize_0XB883 = Try(puschTbsize.map(_.toLong)).getOrElse(null),
          txtype_0XB883 = puschTxtype, txmode_0XB883 = puschTxmode, bwpidx_0XB883 = Try(puschBwpIdx.map(_.toInt)).getOrElse(null),
          numNEW_TX_0XB883 = numNEW_TX, numRE_TX_0XB883 = numRE_TX, numNEW_TXPCC_0XB883 = numNEW_TXPCC, numNEW_TXSCC1_0XB883 = numNEW_TXSCC1, numRE_TXPCC_0XB883 = numRE_TXPCC,
          numRE_TXSCC1_0XB883 = numRE_TXSCC1, numRE_TXRV_0_0XB883 = numRE_TXRV_0, numRE_TXRV_1_0XB883 = numRE_TXRV_1, numRE_TXRV_2_0XB883 = numRE_TXRV_2, numRE_TXRV_3_0XB883 = numRE_TXRV_3)

        logRecordQComm5G2Obj = logRecordQComm5G2Obj.copy(NRPUSCHWaveformPCC = NRPUSCHWaveformPCCVal, NRPUSCHWaveformSCC1 = NRPUSCHWaveformSCC1Val)

        logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47235", hexLogCode = "0xB883", logRecordQComm5G = logRecordQComm5GObj, logRecordQComm5G2 = logRecordQComm5G2Obj)

      case _ =>
        logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47235", hexLogCode = "0xB883", missingVersion = logVersion)
    }

    logRecord
  }

  def parseLogRecordQCOMM5GSet6(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQComm5GObj: LogRecordQComm5G = LogRecordQComm5G()
    var logRecordQComm5G2Obj: LogRecordQComm5G2 = LogRecordQComm5G2()

    def parse47233 = {
      logVersion match {

        case 131073 =>
          val bean47233_131073 = process47233_131073(x, exceptionUseArray)
          //            logger.info("bean47233_131072 >>>>>>>>>>"+Constants.getJsonFromBean(bean47233_131073))
          //            println("bean47233_131072 >>>>>>>>>>"+Constants.getJsonFromBean(bean47233_131073))
          val tbNewTxBytes = bean47233_131073.sr_27350_inst_list.asScala.toList.map(_.TBNewTxBytes.toLong).sum
          val numNewTxTb = bean47233_131073.sr_27350_inst_list.asScala.toList.map(_.NumNewTXTB.toLong).sum
          val numReTxTb = bean47233_131073.sr_27350_inst_list.asScala.toList.map(_.NumReTxTB.toLong).sum

          logRecordQComm5GObj = logRecordQComm5GObj.copy(tbNewTxBytesB881 = if (tbNewTxBytes > 0) tbNewTxBytes else null, numNewTxTbB881 = if (numNewTxTb > 0) numNewTxTb else null, numReTxTbB881 = if (numReTxTb > 0) numReTxTb else null)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47233", hexLogCode = "0xB881", logRecordQComm5G = logRecordQComm5GObj)

        case 131074 =>
          val bean47233_131074 = process47233_131074(x, exceptionUseArray)
          //            logger.info("bean47233_131072 >>>>>>>>>>"+Constants.getJsonFromBean(bean47233_131073))
          //            println("bean47233_131072 >>>>>>>>>>"+Constants.getJsonFromBean(bean47233_131073))
          val tbNewTxBytes = bean47233_131074.sr_54808_inst_list.asScala.toList.map(_.TBNewTxBytes.toLong).sum
          val numNewTxTb = bean47233_131074.sr_54808_inst_list.asScala.toList.map(_.NumNewTXTB.toLong).sum
          val numReTxTb = bean47233_131074.sr_54808_inst_list.asScala.toList.map(_.NumReTxTB.toLong).sum

          logRecordQComm5GObj = logRecordQComm5GObj.copy(tbNewTxBytesB881 = if (tbNewTxBytes > 0) tbNewTxBytes else null, numNewTxTbB881 = if (numNewTxTb > 0) numNewTxTb else null, numReTxTbB881 = if (numReTxTb > 0) numReTxTb else null)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47233", hexLogCode = "0xB881", logRecordQComm5G = logRecordQComm5GObj)

        case _ =>
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47233", hexLogCode = "0xB881", missingVersion = logVersion)

      }
    }

    def parse47208 = {
      logVersion match {
        case 1 =>
          val bean47208_1 = process47208_1(x, exceptionUseArray)
          //logger.info("bean47208_1 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47208_1))
          val totalTxBytes = bean47208_1.sr_8721_inst_list.asScala.map(_.Total_Tx_Bytes).filter(obj => (obj != null && obj.trim.size > 0)).map(_.toLong).sum
          //logger.info("47208======>1>>>>>>"+totalTxBytes)
          val totalReTxPDUsSum_0x0xB868Val: lang.Long = bean47208_1.sr_8721_inst_list.asScala.map(_.Total_ReTx_PDUs).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val totalTxPDUsSum_0x0xB868Val: lang.Long = bean47208_1.sr_8721_inst_list.asScala.map(_.Total_Tx_PDUs).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          //logger.info(s"totalReTxPDUsSum_0x0xB868 = $totalReTxPDUsSum_0x0xB868Val, totalTxPDUsSum_0x0xB868 = $totalTxPDUsSum_0x0xB868Val, time = ${logRecord.dmTimeStamp}")
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalTxBytes_0xB868 = totalTxBytes, totalReTxPDUsSum_0x0xB868 = totalReTxPDUsSum_0x0xB868Val, totalTxPDUsSum_0x0xB868 = totalTxPDUsSum_0x0xB868Val)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47208", hexLogCode = "0xB868", logRecordQComm5G = logRecordQComm5GObj)
        case 2 =>
          val bean47208_2 = process47208_2(x, exceptionUseArray)
          //logger.info("bean47208_1 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47208_2))
          val totalTxBytes = bean47208_2.sr_12876_inst_list.asScala.map(_.Total_Tx_Bytes).filter(obj => (obj != null && obj.trim.size > 0)).map(_.toLong).sum
          //logger.info("47208======>2>>>>>>"+totalTxBytes)
          val totalReTxPDUsSum_0x0xB868Val: lang.Long = bean47208_2.sr_12876_inst_list.asScala.map(_.Total_ReTx_PDUs).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val totalTxPDUsSum_0x0xB868Val: lang.Long = bean47208_2.sr_12876_inst_list.asScala.map(_.Total_Tx_PDUs).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          //logger.info(s"totalReTxPDUsSum_0x0xB868 = $totalReTxPDUsSum_0x0xB868Val, totalTxPDUsSum_0x0xB868 = $totalTxPDUsSum_0x0xB868Val, time = ${logRecord.dmTimeStamp}")
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalTxBytes_0xB868 = totalTxBytes, totalReTxPDUsSum_0x0xB868 = totalReTxPDUsSum_0x0xB868Val, totalTxPDUsSum_0x0xB868 = totalTxPDUsSum_0x0xB868Val)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47208", hexLogCode = "0xB868", logRecordQComm5G = logRecordQComm5GObj)
        case 3 =>
          val bean47208_3 = process47208_3(x, exceptionUseArray)
          //logger.info("bean47208_1 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47208_3))
          val totalTxBytes = bean47208_3.sr_27636_inst_list.asScala.map(_.TotalTxBytes).filter(obj => (obj != null && obj.trim.size > 0)).map(_.toLong).sum
          //logger.info("47208======>2>>>>>>"+totalTxBytes)
          val totalReTxPDUsSum_0x0xB868Val: lang.Long = bean47208_3.sr_27636_inst_list.asScala.map(_.TotalReTxPDUs).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val totalTxPDUsSum_0x0xB868Val: lang.Long = bean47208_3.sr_27636_inst_list.asScala.map(_.TotalTxPDUs).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          //logger.info(s"totalReTxPDUsSum_0x0xB868 = $totalReTxPDUsSum_0x0xB868Val, totalTxPDUsSum_0x0xB868 = $totalTxPDUsSum_0x0xB868Val, time = ${logRecord.dmTimeStamp}")
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalTxBytes_0xB868 = totalTxBytes, totalReTxPDUsSum_0x0xB868 = totalReTxPDUsSum_0x0xB868Val, totalTxPDUsSum_0x0xB868 = totalTxPDUsSum_0x0xB868Val)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47208", hexLogCode = "0xB868", logRecordQComm5G = logRecordQComm5GObj)
        case 4 =>
          val bean47208_4 = process47208_4(x, exceptionUseArray)
          //logger.info("bean47208_1 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47208_4))
          val totalTxBytes = bean47208_4.sr_28001_inst_list.asScala.map(_.TotalTxBytes).filter(obj => (obj != null && obj.trim.length > 0)).map(_.toLong).sum
          val totalReTxPDUsSum_0x0xB868Val: lang.Long = bean47208_4.sr_28001_inst_list.asScala.map(_.TotalReTxPDUs).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val totalTxPDUsSum_0x0xB868Val: lang.Long = bean47208_4.sr_28001_inst_list.asScala.map(_.TotalTxPDUs).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          //logger.info(s"totalReTxPDUsSum_0x0xB868 = $totalReTxPDUsSum_0x0xB868Val, totalTxPDUsSum_0x0xB868 = $totalTxPDUsSum_0x0xB868Val, time = ${logRecord.dmTimeStamp}")
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalTxBytes_0xB868 = totalTxBytes, totalReTxPDUsSum_0x0xB868 = totalReTxPDUsSum_0x0xB868Val, totalTxPDUsSum_0x0xB868 = totalTxPDUsSum_0x0xB868Val)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47208", hexLogCode = "0xB868", logRecordQComm5G = logRecordQComm5GObj)
        case 131072 =>
          val bean47208_131072 = process47208_131072(x, exceptionUseArray)
          val totalTxBytes = bean47208_131072.sr_48188_inst_list.asScala.map(_.TotalTxBytes).filter(obj => (obj != null && obj.trim.length > 0)).map(_.toLong).sum
          val totalReTxPDUsSum_0x0xB868Val: lang.Long = bean47208_131072.sr_48188_inst_list.asScala.map(_.TotalReTxPDUs).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val totalTxPDUsSum_0x0xB868Val: lang.Long = bean47208_131072.sr_48188_inst_list.asScala.map(_.TotalTxPDUs).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          //logger.info(s"totalReTxPDUsSum_0x0xB868 = $totalReTxPDUsSum_0x0xB868Val, totalTxPDUsSum_0x0xB868 = $totalTxPDUsSum_0x0xB868Val, time = ${logRecord.dmTimeStamp}")
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalTxBytes_0xB868 = totalTxBytes, totalReTxPDUsSum_0x0xB868 = totalReTxPDUsSum_0x0xB868Val, totalTxPDUsSum_0x0xB868 = totalTxPDUsSum_0x0xB868Val)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47208", hexLogCode = "0xB868", logRecordQComm5G = logRecordQComm5GObj)
        case 131073 =>
          val bean47208_131073 = process47208_131073(x, exceptionUseArray)
          val totalTxBytes = bean47208_131073.sr_59141_inst_list.asScala.map(_.TotalTxBytes).filter(obj => (obj != null && obj.trim.length > 0)).map(_.toLong).sum
          val totalReTxPDUsSum_0x0xB868Val: lang.Long = bean47208_131073.sr_59141_inst_list.asScala.map(_.TotalReTxPDUs).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val totalTxPDUsSum_0x0xB868Val: lang.Long = bean47208_131073.sr_59141_inst_list.asScala.map(_.TotalTxPDUs).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalTxBytes_0xB868 = totalTxBytes, totalReTxPDUsSum_0x0xB868 = totalReTxPDUsSum_0x0xB868Val, totalTxPDUsSum_0x0xB868 = totalTxPDUsSum_0x0xB868Val)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47208", hexLogCode = "0xB868", logRecordQComm5G = logRecordQComm5GObj)
        case 196609 =>
          val bean47208_196609 = process47208_196609(x, exceptionUseArray)
          val totalTxBytes = bean47208_196609.sr_58231_inst_list.asScala.map(_.TotalTxBytes).filter(obj => (obj != null && obj.trim.nonEmpty)).map(_.toLong).sum
          val totalReTxPDUsSum_0x0xB868Val: lang.Long = bean47208_196609.sr_58231_inst_list.asScala.map(_.TotalReTxPDUs).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val totalTxPDUsSum_0x0xB868Val: lang.Long = bean47208_196609.sr_58231_inst_list.asScala.map(_.TotalTxPDUs).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalTxBytes_0xB868 = totalTxBytes, totalReTxPDUsSum_0x0xB868 = totalReTxPDUsSum_0x0xB868Val, totalTxPDUsSum_0x0xB868 = totalTxPDUsSum_0x0xB868Val)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47208", hexLogCode = "0xB868", logRecordQComm5G = logRecordQComm5GObj)
        case _ =>
          //logger.info("47208======>unknown logversion>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+logVersion)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", missingVersion = logVersion, logCode = "47208", hexLogCode = "0xB868")
      }
    }

    def parse47200 = {
      logVersion match {
        /*case 1 =>
            logger.info("47200=========>"+logVersion)
            val bean47200_1 = process47200_1(x, exceptionUseArray) //process47200_1
            logger.info("47200_1======>2>>>>>>"+Constants.getJsonFromBean(bean47200_1))47200
            //RB Path Type is LTE_AND_NR or NR
            bean47200_1.sr_10184_inst_list
          case 2 =>
            logger.info("47200=========>"+logVersion)
            val bean47200_2 = process47200_2(x, exceptionUseArray) //process47200_2*/
        case 4 =>
          val bean47200_4 = process47200_4(x, exceptionUseArray) //process47200_5
          //TotalNumRXBytes
          val toalNumRxBytesList = bean47200_4.sr_12168_inst_list.asScala.filter(obj => obj.TotalNumRXBytes.toLong != 0L)
            .map(obj => obj.TotalNumRXBytes)
          val toalNumRxBytes = Try(toalNumRxBytesList.head).getOrElse(null)
          val toalNumRxBytes0xB860: lang.Long = if (toalNumRxBytes != null) toalNumRxBytes.toLong else null
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalNumRxBytes_0xB860 = toalNumRxBytes0xB860)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47200", hexLogCode = "0xB860", logRecordQComm5G = logRecordQComm5GObj)
        case 5 =>
          val bean47200_5 = process47200_5(x, exceptionUseArray) //process47200_5
          //logger.info("0xB860(47200) JSON for Timestamp : "+logRecord.dmTimeStamp+" ........"+Constants.getJsonFromBean(bean47200_5))
          //TotalNumRXBytes
          val toalNumRxBytesList = bean47200_5.sr_22264_inst_list.asScala.filter(obj => obj.RBPathType.equalsIgnoreCase("LTE_AND_NR") || obj.RBPathType.equalsIgnoreCase("NR"))
            .map(obj => obj.TotalNumRXBytes)
          val toalNumRxBytes = Try(toalNumRxBytesList.head).getOrElse(null)
          val toalNumRxBytes0xB860: lang.Long = if (toalNumRxBytes != null) toalNumRxBytes.toLong else null
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalNumRxBytes_0xB860 = toalNumRxBytes0xB860)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47200", hexLogCode = "0xB860", logRecordQComm5G = logRecordQComm5GObj)
        case 8 =>
          val bean47200_8 = process47200_8(x, exceptionUseArray) //process47200_5
          //TotalNumRXBytes
          //logger.info("bean47200_8 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47200_8))
          val toalNumRxBytesList = bean47200_8.sr_34375_inst_list.asScala.filter(obj => obj.RBPathType.equalsIgnoreCase("LTE_AND_NR") || obj.RBPathType.equalsIgnoreCase("NR"))
            .map(obj => obj.TotalNumRXBytes)
          val toalNumRxBytes = Try(toalNumRxBytesList.head).getOrElse(null)
          val toalNumRxBytes0xB860: lang.Long = if (toalNumRxBytes != null) toalNumRxBytes.toLong else null
          val totalNumDiscardPacketsSum_0xB860Val: lang.Long = bean47200_8.sr_34375_inst_list.asScala.toList.map(_.TotalNumDiscardPackets).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numRXPacketsSum_0xB860Val: lang.Long = bean47200_8.sr_34375_inst_list.asScala.toList.map(_.NumRXPackets).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalNumRxBytes_0xB860 = toalNumRxBytes0xB860, totalNumDiscardPacketsSum_0xB860 = totalNumDiscardPacketsSum_0xB860Val,
            numRXPacketsSum_0xB860 = numRXPacketsSum_0xB860Val)
          //logger.info(s"totalNumDiscardPacketsSum_0xB860 = $totalNumDiscardPacketsSum_0xB860Val, numRXPacketsSum_0xB860 = $numRXPacketsSum_0xB860Val, time = ${logRecord.dmTimeStamp}")
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47200", hexLogCode = "0xB860", logRecordQComm5G = logRecordQComm5GObj)
        case 9 =>
          val bean47200_9 = process47200_9(x, exceptionUseArray)
          //TotalNumRXBytes
          //logger.info("bean47200_9 -> Bean as JSON >>>>>>>>"+Constants.getJsonFromBean(bean47200_9))
          val toalNumRxBytesList = bean47200_9.sr_28056_inst_list.asScala.filter(obj => obj.RBPathType.equalsIgnoreCase("LTE_AND_NR") || obj.RBPathType.equalsIgnoreCase("NR"))
            .map(obj => obj.TotalNumRXBytes)
          val toalNumRxBytes = Try(toalNumRxBytesList.head).getOrElse(null)
          val toalNumRxBytes0xB860: lang.Long = if (toalNumRxBytes != null) toalNumRxBytes.toLong else null
          val totalNumDiscardPacketsSum_0xB860Val: lang.Long = bean47200_9.sr_28056_inst_list.asScala.toList.map(_.TotalNumDiscardPackets).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numRXPacketsSum_0xB860Val: lang.Long = bean47200_9.sr_28056_inst_list.asScala.toList.map(_.NumRXPackets).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalNumRxBytes_0xB860 = toalNumRxBytes0xB860, totalNumDiscardPacketsSum_0xB860 = totalNumDiscardPacketsSum_0xB860Val,
            numRXPacketsSum_0xB860 = numRXPacketsSum_0xB860Val)
          //logger.info(s"totalNumDiscardPacketsSum_0xB860 = $totalNumDiscardPacketsSum_0xB860Val, numRXPacketsSum_0xB860 = $numRXPacketsSum_0xB860Val, time = ${logRecord.dmTimeStamp}")
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47200", hexLogCode = "0xB860", logRecordQComm5G = logRecordQComm5GObj)
        case 131072 =>
          val bean47200_131072 = process47200_131072(x, exceptionUseArray)
          val toalNumRxBytesList = bean47200_131072.sr_38523_inst_list.asScala.filter(obj => obj.RBPathType.equalsIgnoreCase("LTE_AND_NR") || obj.RBPathType.equalsIgnoreCase("NR"))
            .map(obj => obj.TotalNumRXBytes)
          val toalNumRxBytes = Try(toalNumRxBytesList.head).getOrElse(null)
          val toalNumRxBytes0xB860: lang.Long = if (toalNumRxBytes != null) toalNumRxBytes.toLong else null
          val totalNumDiscardPacketsSum_0xB860Val: lang.Long = bean47200_131072.sr_38523_inst_list.asScala.toList.map(_.TotalNumDiscardPackets).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numRXPacketsSum_0xB860Val: lang.Long = bean47200_131072.sr_38523_inst_list.asScala.toList.map(_.NumRXPackets).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalNumRxBytes_0xB860 = toalNumRxBytes0xB860, totalNumDiscardPacketsSum_0xB860 = totalNumDiscardPacketsSum_0xB860Val,
            numRXPacketsSum_0xB860 = numRXPacketsSum_0xB860Val)
          //logger.info(s"totalNumDiscardPacketsSum_0xB860 = $totalNumDiscardPacketsSum_0xB860Val, numRXPacketsSum_0xB860 = $numRXPacketsSum_0xB860Val, time = ${logRecord.dmTimeStamp}")
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47200", hexLogCode = "0xB860", logRecordQComm5G = logRecordQComm5GObj)
        case 131073 =>
          val bean47200_131073 = process47200_131073(x, exceptionUseArray)
          val toalNumRxBytesList = bean47200_131073.sr_56801_inst_list.asScala.filter(obj => obj.RBPathType.equalsIgnoreCase("LTE_AND_NR") || obj.RBPathType.equalsIgnoreCase("NR"))
            .map(obj => obj.TotalNumRXBytes)
          val toalNumRxBytes = Try(toalNumRxBytesList.head).getOrElse(null)
          val toalNumRxBytes0xB860: lang.Long = if (toalNumRxBytes != null) toalNumRxBytes.toLong else null
          val totalNumDiscardPacketsSum_0xB860Val: lang.Long = bean47200_131073.sr_56801_inst_list.asScala.toList.map(_.TotalNumDiscardPackets).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numRXPacketsSum_0xB860Val: lang.Long = bean47200_131073.sr_56801_inst_list.asScala.toList.map(_.NumRXPackets).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalNumRxBytes_0xB860 = toalNumRxBytes0xB860, totalNumDiscardPacketsSum_0xB860 = totalNumDiscardPacketsSum_0xB860Val,
            numRXPacketsSum_0xB860 = numRXPacketsSum_0xB860Val)
          //logger.info(s"totalNumDiscardPacketsSum_0xB860 = $totalNumDiscardPacketsSum_0xB860Val, numRXPacketsSum_0xB860 = $numRXPacketsSum_0xB860Val, time = ${logRecord.dmTimeStamp}")
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47200", hexLogCode = "0xB860", logRecordQComm5G = logRecordQComm5GObj)
        case 131074 =>
          val bean47200_131074 = process47200_131074(x, exceptionUseArray)
          val toalNumRxBytesList = bean47200_131074.sr_48228_inst_list.asScala.filter(obj => obj.RBPathType.equalsIgnoreCase("LTE_AND_NR") || obj.RBPathType.equalsIgnoreCase("NR"))
            .map(obj => obj.TotalNumRXBytes)
          val toalNumRxBytes = Try(toalNumRxBytesList.head).getOrElse(null)
          val toalNumRxBytes0xB860: lang.Long = if (toalNumRxBytes != null) toalNumRxBytes.toLong else null
          val totalNumDiscardPacketsSum_0xB860Val: lang.Long = bean47200_131074.sr_48228_inst_list.asScala.toList.map(_.TotalNumDiscardPackets).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numRXPacketsSum_0xB860Val: lang.Long = bean47200_131074.sr_48228_inst_list.asScala.toList.map(_.NumRXPackets).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalNumRxBytes_0xB860 = toalNumRxBytes0xB860, totalNumDiscardPacketsSum_0xB860 = totalNumDiscardPacketsSum_0xB860Val,
            numRXPacketsSum_0xB860 = numRXPacketsSum_0xB860Val)
          //logger.info(s"totalNumDiscardPacketsSum_0xB860 = $totalNumDiscardPacketsSum_0xB860Val, numRXPacketsSum_0xB860 = $numRXPacketsSum_0xB860Val, time = ${logRecord.dmTimeStamp}")
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47200", hexLogCode = "0xB860", logRecordQComm5G = logRecordQComm5GObj)
        case 131075 =>
          val bean47200_131075 = process47200_131075(x, exceptionUseArray)
          val toalNumRxBytesList = bean47200_131075.sr_59163_inst_list.asScala.filter(obj => obj.RBPathType.equalsIgnoreCase("LTE_AND_NR") || obj.RBPathType.equalsIgnoreCase("NR"))
            .map(obj => obj.TotalNumRXBytes)
          val toalNumRxBytes = Try(toalNumRxBytesList.head).getOrElse(null)
          val toalNumRxBytes0xB860: lang.Long = if (toalNumRxBytes != null) toalNumRxBytes.toLong else null
          val totalNumDiscardPacketsSum_0xB860Val: lang.Long = bean47200_131075.sr_59163_inst_list.asScala.toList.map(_.TotalNumDiscardPackets).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numRXPacketsSum_0xB860Val: lang.Long = bean47200_131075.sr_59163_inst_list.asScala.toList.map(_.NumRXPackets).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalNumRxBytes_0xB860 = toalNumRxBytes0xB860, totalNumDiscardPacketsSum_0xB860 = totalNumDiscardPacketsSum_0xB860Val,
            numRXPacketsSum_0xB860 = numRXPacketsSum_0xB860Val)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47200", hexLogCode = "0xB860", logRecordQComm5G = logRecordQComm5GObj)
        case 196610 =>
          val bean47200_196610 = process47200_196610(x, exceptionUseArray)
          val toalNumRxBytesList = bean47200_196610.sr_58941_inst_list.asScala.filter(obj => obj.RBPathType.equalsIgnoreCase("LTE_AND_NR") || obj.RBPathType.equalsIgnoreCase("NR"))
            .map(obj => obj.TotalNumRXBytes)
          val toalNumRxBytes = Try(toalNumRxBytesList.head).getOrElse(null)
          val toalNumRxBytes0xB860: lang.Long = if (toalNumRxBytes != null) toalNumRxBytes.toLong else null
          val totalNumDiscardPacketsSum_0xB860Val: lang.Long = bean47200_196610.sr_58941_inst_list.asScala.toList.map(_.TotalNumDiscardPackets).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          val numRXPacketsSum_0xB860Val: lang.Long = bean47200_196610.sr_58941_inst_list.asScala.toList.map(_.NumRXPackets).filter(str => str != null && str.trim.nonEmpty).map(str => parseLong(str)).sum
          logRecordQComm5GObj = logRecordQComm5GObj.copy(totalNumRxBytes_0xB860 = toalNumRxBytes0xB860, totalNumDiscardPacketsSum_0xB860 = totalNumDiscardPacketsSum_0xB860Val,
            numRXPacketsSum_0xB860 = numRXPacketsSum_0xB860Val)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47200", hexLogCode = "0xB860", logRecordQComm5G = logRecordQComm5GObj)
        case _ =>
          logger.info("Unknown 47200=========>" + logVersion)
          logRecord = logRecord.copy(logRecordName = "QCOMM5G", missingVersion = logVersion, logCode = "47200", hexLogCode = "0xB860")
      }
    }

    logCode match {
      case 47235 =>
        logRecord = parseLogRecordQCOMM5GSet4_47235(logVersion, x, exceptionUseArray, logRecordQComm5GObj, logRecordQComm5G2Obj, logRecord)

      case 47233 =>
        parse47233

      case 47208 =>
        parse47208

      case 47200 =>
        parse47200
    }
    logRecord
  }

  def parseLogRecordQCOMM5GSet7(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQComm5GObj: LogRecordQComm5G = LogRecordQComm5G()
    logCode match {

      case 47506 =>
        logVersion match {
          case 131073 =>
            val bean47506_131073 = process47506_131073(x, exceptionUseArray)
            val logType = bean47506_131073.LogType
            val numCC = bean47506_131073.NumCC
            var perRxFtlSnr: String = null
            val perRxFtlSnrList = bean47506_131073.sr_31283_inst_list.asScala.toList.map(_.sr_31741_inst_list.asScala.toList.map(_.sr_31743_inst_list.asScala.toList.map(_.sr_31772_inst_list.asScala.toList.map(_.RXFTLSNR)))).flatten.flatten.flatten.filter(x => !x.equalsIgnoreCase("-Infinity"))
            var nrServingRxBeamSNR0_0xB992Val: java.lang.Float = null
            var nrServingRxBeamSNR1_0xB992Val: java.lang.Float = null
            if (logType == "LOOP_STATE" && numCC.toInt > 0 && perRxFtlSnrList.nonEmpty) {
              perRxFtlSnr = perRxFtlSnrList.head
            }
            if (perRxFtlSnrList.nonEmpty) {
              nrServingRxBeamSNR0_0xB992Val = try {
                java.lang.Float.parseFloat(perRxFtlSnrList.head)
              } catch {
                case _: NumberFormatException => null
              }
              if (perRxFtlSnrList.tail.nonEmpty) {
                nrServingRxBeamSNR1_0xB992Val = try {
                  java.lang.Float.parseFloat(perRxFtlSnrList.tail.head)
                } catch {
                  case _: NumberFormatException => null
                }
              }
            }

            logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSBSNR_0xB992 = perRxFtlSnr,
              NRServingRxBeamSNR0_0xB992 = nrServingRxBeamSNR0_0xB992Val, NRServingRxBeamSNR1_0xB992 = nrServingRxBeamSNR1_0xB992Val)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47506", hexLogCode = "0xB992", logRecordQComm5G = logRecordQComm5GObj)
          case 131074 =>
            val bean47506_131074 = process47506_131074(x, exceptionUseArray)
            val logTypeVal = bean47506_131074.sr_48575_inst_list.asScala.toList.map(obj => obj.LogType)
            val numCCVal = bean47506_131074.sr_48575_inst_list.asScala.toList.map(obj => obj.NumCC)
            var logType: String = ""
            var numCC: String = ""
            if (logTypeVal != null && !logTypeVal.isEmpty) {
              logType = logTypeVal(0)
            }
            if (numCCVal != null && !numCCVal.isEmpty) {
              numCC = numCCVal(0)
            }
            var perRxFtlSnr: String = null
            val perRxFtlSnrList = bean47506_131074.sr_48576_inst_list.asScala.toList.map(_.sr_48585_inst_list.asScala.toList.map(_.sr_48587_inst_list.asScala.toList.map(_.RXFTLSNR))).flatten.flatten.filter(x => !x.equalsIgnoreCase("-Infinity"))
            if (logType == "LOOP_STATE" && numCC.toInt > 0 && perRxFtlSnrList.nonEmpty) {
              perRxFtlSnr = perRxFtlSnrList.head
            }

            logRecordQComm5GObj = logRecordQComm5GObj.copy(NRSBSNR_0xB992 = perRxFtlSnr)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47506", hexLogCode = "0xB992", logRecordQComm5G = logRecordQComm5GObj)


          case _ =>
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47506", hexLogCode = "0xB992", missingVersion = logVersion)
        }

      case 47248 =>
        logVersion match {
          case 65536 =>
            var bean47248_65536 = process47248_65536(x, exceptionUseArray)
            val slotVal = bean47248_65536.sr_18481_inst_list.asScala.toList.map(obj => obj.Slot)
            val frameVal = bean47248_65536.sr_18481_inst_list.asScala.toList.map(obj => obj.Frame)
            val cdrxEventVal = bean47248_65536.sr_18481_inst_list.asScala.toList.map(_.CDRXEvent)
            val numerologyVal = bean47248_65536.sr_18481_inst_list.asScala.toList.map(obj => obj.Numerology)
            var cdrxKpiList: List[cdrxKpi0xB890List] = null

            if (slotVal != null) {
              cdrxKpiList = slotVal.map(x => cdrxKpi0xB890List(slotVal, frameVal, cdrxEventVal, null))
            }
            logRecordQComm5GObj = logRecordQComm5GObj.copy(cdrxKpi0xB890List = if (cdrxKpiList != null) cdrxKpiList.take(1) else cdrxKpiList, slot0xB890 = slotVal, sfn0xB890 = frameVal, numerology0xB890 = numerologyVal)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47248", hexLogCode = "0xB890", logRecordQComm5G = logRecordQComm5GObj)
          case 65537 =>
            var bean47248_65537 = process47248_65537(x, exceptionUseArray)
            val slotVal = bean47248_65537.sr_18522_inst_list.asScala.toList.map(obj => obj.Slot)
            val frameVal = bean47248_65537.sr_18522_inst_list.asScala.toList.map(obj => obj.Frame)
            val cdrxEventVal = bean47248_65537.sr_18522_inst_list.asScala.toList.map(_.CDRXEvent)
            val numerologyVal = bean47248_65537.sr_18522_inst_list.asScala.toList.map(obj => obj.Numerology)
            var cdrxKpiList: List[cdrxKpi0xB890List] = null

            if (slotVal != null) {
              cdrxKpiList = slotVal.map(x => cdrxKpi0xB890List(slotVal, frameVal, cdrxEventVal, null))
            }
            logRecordQComm5GObj = logRecordQComm5GObj.copy(cdrxKpi0xB890List = if (cdrxKpiList != null) cdrxKpiList.take(1) else cdrxKpiList, slot0xB890 = slotVal, sfn0xB890 = frameVal, numerology0xB890 = numerologyVal)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47248", hexLogCode = "0xB890", logRecordQComm5G = logRecordQComm5GObj)
          case 65538 =>
            var bean47248_65538 = process47248_65538(x, exceptionUseArray)
            val slotVal = bean47248_65538.sr_20631_inst_list.asScala.toList.map(obj => obj.Slot)
            val frameVal = bean47248_65538.sr_20631_inst_list.asScala.toList.map(obj => obj.Frame)
            val cdrxEventVal = bean47248_65538.sr_20631_inst_list.asScala.toList.map(_.CDRXEvent)
            val numerologyVal = bean47248_65538.sr_20631_inst_list.asScala.toList.map(obj => obj.Numerology)
            var cdrxKpiList: List[cdrxKpi0xB890List] = null

            if (slotVal != null) {
              cdrxKpiList = slotVal.map(x => cdrxKpi0xB890List(slotVal, frameVal, cdrxEventVal, null))
            }
            logRecordQComm5GObj = logRecordQComm5GObj.copy(cdrxKpi0xB890List = if (cdrxKpiList != null) cdrxKpiList.take(1) else cdrxKpiList, slot0xB890 = slotVal, sfn0xB890 = frameVal, numerology0xB890 = numerologyVal)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47248", hexLogCode = "0xB890", logRecordQComm5G = logRecordQComm5GObj)
          case 131072 =>
            var bean47248_131072 = process47248_131072(x, exceptionUseArray)
            val slotVal = bean47248_131072.sr_19640_inst_list.asScala.toList.map(_.Slot)
            val frameVal = bean47248_131072.sr_19640_inst_list.asScala.toList.map(_.SFN)
            //val cdrxEventVal = bean47248_131072.sr_19640_inst_list.asScala.toList.map(_.CDRXEvent)
            val prevStateVal = bean47248_131072.sr_19640_inst_list.asScala.toList.map(_.PrevState)
            //val numerologyVal = bean47248_131072.sr_19640_inst_list.asScala.toList.map(_.S)
            var cdrxKpiList: List[cdrxKpi0xB890List] = null
            if (slotVal != null) {
              cdrxKpiList = slotVal.map(x => cdrxKpi0xB890List(slotVal, frameVal, null, null))
            }
            logRecordQComm5GObj = logRecordQComm5GObj.copy(cdrxKpi0xB890List = if (cdrxKpiList != null) cdrxKpiList.take(1) else cdrxKpiList, slot0xB890 = slotVal, sfn0xB890 = frameVal, numerology0xB890 = null)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47248", hexLogCode = "0xB890", logRecordQComm5G = logRecordQComm5GObj)
          case 131073 =>
            var bean47248_131073 = process47248_131073(x, exceptionUseArray)
            val slotVal = bean47248_131073.sr_33992_inst_list.asScala.toList.map(_.sr_34037_inst_list.asScala.toList.map(_.SlotNumber)).flatten
            val frameVal = bean47248_131073.sr_33992_inst_list.asScala.toList.map(_.sr_34037_inst_list.asScala.toList.map(_.SystemFrameNumber)).flatten
            val prevStateVal = bean47248_131073.sr_33992_inst_list.asScala.toList.map(_.PrevState)
            val numerologyVal = bean47248_131073.sr_33992_inst_list.asScala.toList.map(_.sr_34037_inst_list.asScala.toList.map(_.SCS)).flatten
            var cdrxKpiList: List[cdrxKpi0xB890List] = null
            if (slotVal != null) {
              cdrxKpiList = slotVal.map(x => cdrxKpi0xB890List(slotVal, frameVal, null, prevStateVal))
            }
            logRecordQComm5GObj = logRecordQComm5GObj.copy(cdrxKpi0xB890List = if (cdrxKpiList != null) cdrxKpiList.take(1) else cdrxKpiList, slot0xB890 = slotVal, sfn0xB890 = frameVal, numerology0xB890 = numerologyVal)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47248", hexLogCode = "0xB890", logRecordQComm5G = logRecordQComm5GObj)
          case 131074 =>
            var bean47248_131074 = process47248_131074(x, exceptionUseArray)
            val slotVal = bean47248_131074.sr_46107_inst_list.asScala.toList.map(_.sr_46129_inst_list.asScala.toList.map(_.SlotNumber)).flatten
            val frameVal = bean47248_131074.sr_46107_inst_list.asScala.toList.map(_.sr_46129_inst_list.asScala.toList.map(_.SystemFrameNumber)).flatten
            val prevStateVal = bean47248_131074.sr_46107_inst_list.asScala.toList.map(_.PrevState)
            val numerologyVal = bean47248_131074.sr_46107_inst_list.asScala.toList.map(_.sr_46129_inst_list.asScala.toList.map(_.SCS)).flatten
            var cdrxKpiList: List[cdrxKpi0xB890List] = null
            if (slotVal != null) {
              cdrxKpiList = slotVal.map(x => cdrxKpi0xB890List(slotVal, frameVal, null, prevStateVal))
            }
            logRecordQComm5GObj = logRecordQComm5GObj.copy(cdrxKpi0xB890List = if (cdrxKpiList != null) cdrxKpiList.take(1) else cdrxKpiList, slot0xB890 = slotVal, sfn0xB890 = frameVal, numerology0xB890 = numerologyVal)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47248", hexLogCode = "0xB890", logRecordQComm5G = logRecordQComm5GObj)
          case 196612 =>
            var bean47248_196612 = process47248_196612(x, exceptionUseArray)
            val slotVal = bean47248_196612.sr_58619_inst_list.asScala.toList.flatMap(_.sr_58628_inst_list.asScala.toList.map(_.SlotNumber))
            val frameVal = bean47248_196612.sr_58619_inst_list.asScala.toList.flatMap(_.sr_58628_inst_list.asScala.toList.map(_.SystemFrameNumber))
            val prevStateVal = bean47248_196612.sr_58619_inst_list.asScala.toList.map(_.PrevState)
            val numerologyVal = bean47248_196612.sr_58619_inst_list.asScala.toList.flatMap(_.sr_58628_inst_list.asScala.toList.map(_.SCS))
            var cdrxKpiList: List[cdrxKpi0xB890List] = null
            if (slotVal != null) {
              cdrxKpiList = slotVal.map(x => cdrxKpi0xB890List(slotVal, frameVal, null, prevStateVal))
            }
            logRecordQComm5GObj = logRecordQComm5GObj.copy(cdrxKpi0xB890List = if (cdrxKpiList != null) cdrxKpiList.take(1) else cdrxKpiList, slot0xB890 = slotVal, sfn0xB890 = frameVal, numerology0xB890 = numerologyVal)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", version = logVersion, logCode = "47248", hexLogCode = "0xB890", logRecordQComm5G = logRecordQComm5GObj)
          case _ =>
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "47248", hexLogCode = "0xB890", missingVersion = logVersion)
        }
    }
    logRecord
  }

  def parseInteger(str: String) = Try(str.toInt).getOrElse(0)

  def parseLong(str: String) = Try(str.toLong).getOrElse(0L)

  def parseFloat(str: String) = Try(java.lang.Float.parseFloat(str)).getOrElse(0F)

  def parseDouble(str: String) = Try(java.lang.Double.parseDouble(str)).getOrElse(0D)

  def getNrBandType (NrBandIndVal:Integer):String={
    var NrBandType:String=""
    if (NrBandIndVal >= 257 && NrBandIndVal <= 261) {
      NrBandType = "mmW (FR2)"
    } else if (NrBandIndVal == 77) {
      NrBandType = "C-Band (FR1)"
    } else if (NrBandIndVal >= 1 && NrBandIndVal <= 95) {
      NrBandType = "DSS (FR1)"
    }
    NrBandType
  }
}
