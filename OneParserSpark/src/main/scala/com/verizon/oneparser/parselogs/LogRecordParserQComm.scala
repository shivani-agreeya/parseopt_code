package com.verizon.oneparser.parselogs

import java.time.ZonedDateTime
import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.ProcessLogCodes
import com.verizon.oneparser.common.{Constants, OPutil}
import com.verizon.oneparser.parselogs.LogRecordParserQComm.getSubJsonValueByKey
import com.verizon.oneparser.schema._
import org.apache.spark.sql.Row

import scala.collection.immutable.List
import scala.collection.mutable
import scala.util.Try

object LogRecordParserQComm extends ProcessLogCodes with LazyLogging{
  var prevTestName: String = ""
  var prevDeviceTime: ZonedDateTime = null

  def parseLogRecordQCOMM_Set1(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQCommObj:LogRecordQComm = LogRecordQComm()


    logCode match {
      case 45433 =>
        logVersion match {
          case 4 => val bean45433 = process45433_4(x, exceptionUseArray)
            //logger.info("0xB179 json >>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45433))
            val filteredRsrpFromBean: List[String] = bean45433.sr_18437_inst_list.asScala.toList.map(_.FilteredRSRP)
            logRecordQCommObj = logRecordQCommObj.copy(
              filteredRsrp = if (filteredRsrpFromBean.size >0) filteredRsrpFromBean.map(_.toString.toDouble) else null,
              numberofNeighborCells = bean45433.NumberofNeighborCells.toInt,
              servingFilteredRSRP = bean45433.getServingFilteredRSRP.toDouble)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45433",
              hexLogCode = "0xB179", logRecordQComm = logRecordQCommObj)
          case 3 => val bean45433 = process45433_3(x, exceptionUseArray)
            val filteredRsrpFromBean: List[String] = bean45433.sr_18414_inst_list.asScala.toList.map(_.FilteredRSRP)
            logRecordQCommObj = logRecordQCommObj.copy(
              filteredRsrp = if (filteredRsrpFromBean.size >0) filteredRsrpFromBean.map(_.toString.toDouble) else null,
              numberofNeighborCells = bean45433.NumberofNeighborCells.toInt,
              servingFilteredRSRP = bean45433.getServingFilteredRSRP.toDouble)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45433",
              hexLogCode = "0xB179", logRecordQComm = logRecordQCommObj)
          case 56 => val bean45433_56 = process45433_56(x, exceptionUseArray)
            val filteredRsrpFromBean: List[String] = bean45433_56.sr_125774_inst_list.asScala.toList.map(_.FilteredRSRP)
            logRecordQCommObj = logRecordQCommObj.copy(
              filteredRsrp = if (filteredRsrpFromBean.nonEmpty) filteredRsrpFromBean.map(_.toDouble) else null,
              numberofNeighborCells = bean45433_56.NumberofNeighborCells.toInt,
              servingFilteredRSRP = bean45433_56.ServingFilteredRSRP.toDouble)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45433",
              hexLogCode = "0xB179", logRecordQComm = logRecordQCommObj)
          case _ =>
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45433", hexLogCode = "0xB179", missingVersion = logVersion)
        }
      case 45450 =>
        logVersion match {
          case 1 => val bean45450 = process45450_1(x, exceptionUseArray)
            //logger.info("0xB18A json >>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45450))
            val InSyncCountFromBean: List[String] = bean45450.sr_19384_inst_list.asScala.toList.map(_.InSyncCount)
            //val OutSyncBlerFromBean: List[Double] = Try(bean45450.sr_19384_inst_list.asScala.toList.map(_.OutofSyncBLER).asInstanceOf[List[Double]]).getOrElse(List())
            //val OutSyncBlerFromBean: List[Double] = Try(bean45450.sr_19384_inst_list.asScala.toList.map(_.OutofSyncBLER).asInstanceOf[List[Double]]).getOrElse(List())
            val OutSyncBlerFromBean: List[String] = bean45450.sr_19384_inst_list.asScala.toList.map(_.OutofSyncBLER)
            val OutSyncCountFromBean: List[String] = bean45450.sr_19384_inst_list.asScala.toList.map(_.OutofSyncCount)
            val T310_Timer_StatusFromBean: List[String] = bean45450.sr_19384_inst_list.asScala.toList.map(_.T310TimerStatus)
            logRecordQCommObj = logRecordQCommObj.copy(
              numRecords0xB18A = bean45450.NumberofRecords.toInt,
              InSyncCount = InSyncCountFromBean.map(_.toString.toInt.asInstanceOf[Integer]),
              OutSyncBler = OutSyncBlerFromBean.map(_.toString.toDouble),
              OutSyncCount = OutSyncCountFromBean.map(_.toString.toInt.asInstanceOf[Integer]),
              T310_Timer_Status = T310_Timer_StatusFromBean.map(_.toString.toInt.asInstanceOf[Integer]))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45450",
              hexLogCode = "0xB18A", logRecordQComm = logRecordQCommObj)
          case _ =>
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45450", hexLogCode = "0xB18A", missingVersion = logVersion)
        }

      case 45249 =>
        logVersion match {
          case 1 =>
            var bean45249 = process45249_1(x, exceptionUseArray)
            logRecordQCommObj = logRecordQCommObj.copy(
              frequency = bean45249.FREQ.toInt,
              physicalCellID_QComm = bean45249.PhysicalcellID.toInt);
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45249",
              hexLogCode = "0xB0C1", logRecordQComm = logRecordQCommObj)

          case 2 =>
            var bean45249 = process45249_2(x, exceptionUseArray)
            logRecordQCommObj = logRecordQCommObj.copy(
              frequency = bean45249.FREQ.toInt,
              physicalCellID_QComm = bean45249.PhysicalcellID.toInt);
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45249",
              hexLogCode = "0xB0C1", logRecordQComm = logRecordQCommObj)

          case _ =>
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45249", hexLogCode = "0xB0C1", missingVersion = logVersion)

        }
      case 28981 => //While Pushing to ES, combine 'MCC/MNC' as string
        var bean28981 = process28981(x, exceptionUseArray)
        logRecordQCommObj = logRecordQCommObj.copy(mcc_QComm =  if (Try(bean28981.MCC.toInt).isSuccess) bean28981.MCC.toInt else 0, mnc_QComm = if (Try(bean28981.MNC.toInt).isSuccess) bean28981.MNC.toInt else 0)
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "28981",
          hexLogCode = "0x7135",  logRecordQComm = logRecordQCommObj)
    }
    logRecord
  }



  def parseLogRecordQCOMM_Set2(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQCommObj:LogRecordQComm = LogRecordQComm()
    logCode match {
      case 45250 =>
        logVersion match {
          case 2 => val bean45250 = process45250_2(x, exceptionUseArray)
            var cellIdVal = Try(bean45250.CellIdentity).getOrElse(null)
            var enbIdVal = 0
            if(cellIdVal!=null){
              enbIdVal = (cellIdVal & 0xFFFFFF00) >> 8
              cellIdVal = cellIdVal & 0xFF
            }
            logRecordQCommObj = logRecordQCommObj.copy( dlBandwidth = bean45250.DLBandwidth, ulBandwidth = bean45250.ULBandwidth, lteCellID = cellIdVal, lteeNBId = enbIdVal,
              physicalCellID_QComm = bean45250.PhysicalcellID, allowedAccess = bean45250.AllowedAccess, freqBandIndicator = bean45250.FreqBandIndicator,
              dlFreq = bean45250.DLFREQ, mcc_QComm =  bean45250.MCC, mnc_QComm = bean45250.MNC, trackingAreaCode = bean45250.Trackingareacode, ulFreq = bean45250.ULFREQ)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45250",
              hexLogCode = "0xB0C2",logRecordQComm = logRecordQCommObj)
          case 3 => val bean45250 = process45250_3(x, exceptionUseArray)
            //logger.info("0xB0C2 json: " + Constants.getJsonFromBean(bean45250))
            var cellIdVal = Try(bean45250.Cell_Identity.toInt.asInstanceOf[Integer]).getOrElse(null)
            var enbIdVal = 0
            if(cellIdVal!=null){
              enbIdVal = (cellIdVal & 0xFFFFFF00) >> 8
              cellIdVal = (cellIdVal & 0xFF)
            }
            logRecordQCommObj = logRecordQCommObj.copy(
              dlBandwidth = bean45250.DL_Bandwidth,
              ulBandwidth = bean45250.UL_Bandwidth,
              lteCellID = cellIdVal,
              physicalCellID_QComm = Try(bean45250.Physical_cell_ID.toInt.asInstanceOf[Integer]).getOrElse(null),
              allowedAccess = bean45250.Allowed_Access,
              freqBandIndicator = Try(bean45250.Freq_Band_Indicator.toInt.asInstanceOf[Integer]).getOrElse(null),
              dlFreq = Try(bean45250.DL_FREQ.toInt.asInstanceOf[Integer]).getOrElse(null),
              mcc_QComm =  Try(bean45250.MCC.toInt.asInstanceOf[Integer]).getOrElse(null),
              mnc_QComm = Try(bean45250.MNC.toInt.asInstanceOf[Integer]).getOrElse(null),
              trackingAreaCode = Try(bean45250.Tracking_area_code.toInt.asInstanceOf[Integer]).getOrElse(null),
              lteeNBId = enbIdVal,
              ulFreq = Try(bean45250.UL_FREQ.toInt.asInstanceOf[Integer]).getOrElse(null))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45250",
              hexLogCode = "0xB0C2",logRecordQComm = logRecordQCommObj)
          case _ =>
            //logger.info("Entered into Default Case for 45250(0xB0C2) for Version: " + logVersion)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45250", hexLogCode = "0xB0C2", missingVersion = logVersion)
        }
      case 45369 =>
        //logger.info("0xB139 version:"+logVersion);

        logVersion match {
          case 23 => val bean45369 = process45369_23(x, exceptionUseArray)
            val filteredPUSCHTxPowerFromBean: List[Integer] = bean45369.sr_4686_inst_list.asScala.toList.map(_.PUSCHTxPower.asInstanceOf[Integer])
            val prbVal: List[Integer] = bean45369.sr_4686_inst_list.asScala.toList.map(_.NumofRB)
            val modULVal: List[String] = bean45369.sr_4686_inst_list.asScala.toList.map(_.PUSCHModOrder)
            val retxIndexVal: List[String] = bean45369.sr_4686_inst_list.asScala.toList.map(_.RetxIndex)
            val CurrentSFNSFVal: List[Integer] = bean45369.sr_4686_inst_list.asScala.toList.map(_.CurrentSFNSF)
            val tbSizeVal: List[List[Integer]] = List(bean45369.sr_4686_inst_list.asScala.toList.map(_.PUSCHTBSize))
            logRecordQCommObj = logRecordQCommObj.copy(pUSCHTxPower = filteredPUSCHTxPowerFromBean, prb = prbVal, modUL = modULVal
              , numRecords0xB139 = bean45369.NumberofRecords, retxIndex = retxIndexVal, currentSFNSF = CurrentSFNSFVal, TB_Size0xB139 = tbSizeVal.flatten)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45369",
              hexLogCode = "0xB139", logRecordQComm = logRecordQCommObj)
          case 24 => val bean45369 = process45369_24(x, exceptionUseArray)
            val filteredPUSCHTxPowerFromBean: List[Integer] = bean45369.sr_4731_inst_list.asScala.toList.map(_.PUSCHTxPower.asInstanceOf[Integer])
            val prbVal: List[Integer] = bean45369.sr_4731_inst_list.asScala.toList.map(_.NumofRB)
            val modULVal: List[String] = bean45369.sr_4731_inst_list.asScala.toList.map(_.PUSCHModOrder)
            val retxIndexVal: List[String] = bean45369.sr_4731_inst_list.asScala.toList.map(_.RetxIndex)
            val CurrentSFNSFVal: List[Integer] = bean45369.sr_4731_inst_list.asScala.toList.map(_.CurrentSFNSF)
            val tbSizeVal: List[List[Integer]] = List(bean45369.sr_4731_inst_list.asScala.toList.map(_.PUSCHTBSize))
            logRecordQCommObj = logRecordQCommObj.copy( pUSCHTxPower = filteredPUSCHTxPowerFromBean, prb = prbVal, modUL = modULVal
              , numRecords0xB139 = bean45369.NumberofRecords, retxIndex = retxIndexVal, currentSFNSF = CurrentSFNSFVal, TB_Size0xB139 = tbSizeVal.flatten)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45369",
              hexLogCode = "0xB139", logRecordQComm = logRecordQCommObj)
          case 41 => val bean45369 = process45369_41(x, exceptionUseArray)
            val filteredPUSCHTxPowerFromBean: List[Integer] = bean45369.sr_4867_inst_list.asScala.toList.map(_.PUSCHTxPower.asInstanceOf[Integer])
            val prbVal: List[Integer] = bean45369.sr_4867_inst_list.asScala.toList.map(_.NumofRB)
            val modULVal: List[String] = bean45369.sr_4867_inst_list.asScala.toList.map(_.PUSCHModOrder)
            val retxIndexVal: List[String] = bean45369.sr_4867_inst_list.asScala.toList.map(_.RetxIndex)
            val CurrentSFNSFVal: List[Integer] = bean45369.sr_4867_inst_list.asScala.toList.map(_.CurrentSFNSF)
            val tbSizeVal: List[List[Integer]] = List(bean45369.sr_4867_inst_list.asScala.toList.map(_.PUSCHTBSize))
            logRecordQCommObj = logRecordQCommObj.copy( pUSCHTxPower = filteredPUSCHTxPowerFromBean, prb = prbVal, modUL = modULVal
              , numRecords0xB139 = bean45369.NumberofRecords, retxIndex = retxIndexVal, currentSFNSF = CurrentSFNSFVal, TB_Size0xB139 = tbSizeVal.flatten)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45369",
              hexLogCode = "0xB139", logRecordQComm = logRecordQCommObj)
          case 43 => val bean45369 = process45369_43(x, exceptionUseArray)
            val filteredPUSCHTxPowerFromBean: List[Integer] = bean45369.sr_4960_inst_list.asScala.toList.map(_.PUSCHTxPower.asInstanceOf[Integer])
            val prbVal: List[Integer] = bean45369.sr_4960_inst_list.asScala.toList.map(_.NumofRB)
            val modULVal: List[String] = bean45369.sr_4960_inst_list.asScala.toList.map(_.PUSCHModOrder)
            val retxIndexVal: List[String] = bean45369.sr_4960_inst_list.asScala.toList.map(_.RetxIndex)
            val CurrentSFNSFVal: List[Integer] = bean45369.sr_4960_inst_list.asScala.toList.map(_.CurrentSFNSF)
            val tbSizeVal: List[List[Integer]] = List(bean45369.sr_4960_inst_list.asScala.toList.map(_.PUSCHTBSize))
            logRecordQCommObj = logRecordQCommObj.copy( pUSCHTxPower = filteredPUSCHTxPowerFromBean, prb = prbVal, modUL = modULVal
              , numRecords0xB139 = bean45369.NumberofRecords, retxIndex = retxIndexVal, currentSFNSF = CurrentSFNSFVal, TB_Size0xB139 = tbSizeVal.flatten)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45369",
              hexLogCode = "0xB139", logRecordQComm = logRecordQCommObj)
          case 102 => val bean45369 = process45369_102(x, exceptionUseArray)
            //logger.info("0XB139 json: " + Constants.getJsonFromBean(bean45369))
            val filteredPUSCHTxPowerFromBean: List[Integer] = bean45369.sr_5066_inst_list.asScala.toList.map(_.PUSCHTxPower.asInstanceOf[Integer])
            val prbVal: List[Integer] = bean45369.sr_5066_inst_list.asScala.toList.map(_.NumofRB)
            val modULVal: List[String] = bean45369.sr_5066_inst_list.asScala.toList.map(_.PUSCHModOrder)
            val retxIndexVal: List[String] = bean45369.sr_5066_inst_list.asScala.toList.map(_.RetxIndex)
            val CurrentSFNSFVal: List[Integer] = bean45369.sr_5066_inst_list.asScala.toList.map(_.CurrentSFNSF)
            val tbSizeVal: List[List[Integer]] = List(bean45369.sr_5066_inst_list.asScala.toList.map(_.PUSCHTBSize))
            logRecordQCommObj = logRecordQCommObj.copy(pUSCHTxPower = filteredPUSCHTxPowerFromBean, prb = prbVal, modUL = modULVal
              , numRecords0xB139 = bean45369.NumberofRecords, retxIndex = retxIndexVal, currentSFNSF = CurrentSFNSFVal, TB_Size0xB139 = tbSizeVal.flatten)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45369",
              hexLogCode = "0xB139", logRecordQComm = logRecordQCommObj)
          case 103 => val bean45369 = process45369_103(x, exceptionUseArray)
            val filteredPUSCHTxPowerFromBean: List[Integer] = bean45369.sr_5123_inst_list.asScala.toList.map(_.PUSCHTxPower.asInstanceOf[Integer])
            val prbVal: List[Integer] = bean45369.sr_5123_inst_list.asScala.toList.map(_.NumofRB)
            val modULVal: List[String] = bean45369.sr_5123_inst_list.asScala.toList.map(_.PUSCHModOrder)
            val retxIndexVal: List[String] = bean45369.sr_5123_inst_list.asScala.toList.map(_.RetxIndex)
            val CurrentSFNSFVal: List[Integer] = bean45369.sr_5123_inst_list.asScala.toList.map(_.CurrentSFNSF)
            val tbSizeVal: List[List[Integer]] = List(bean45369.sr_5123_inst_list.asScala.toList.map(_.PUSCHTBSize))
            logRecordQCommObj = logRecordQCommObj.copy( pUSCHTxPower = filteredPUSCHTxPowerFromBean, prb = prbVal, modUL = modULVal
              , numRecords0xB139 = bean45369.NumberofRecords, retxIndex = retxIndexVal, currentSFNSF = CurrentSFNSFVal, TB_Size0xB139 = tbSizeVal.flatten)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45369",
              hexLogCode = "0xB139", logRecordQComm = logRecordQCommObj)
          case 122 => val bean45369 = process45369_122(x, exceptionUseArray)
            val filteredPUSCHTxPowerFromBean: List[Integer] = bean45369.sr_5230_inst_list.asScala.toList.map(_.PUSCHTxPower.asInstanceOf[Integer])
            val prbVal: List[Integer] = bean45369.sr_5230_inst_list.asScala.toList.map(_.NumofRB)
            val modULVal: List[String] = bean45369.sr_5230_inst_list.asScala.toList.map(_.PUSCHModOrder)
            val retxIndexVal: List[String] = bean45369.sr_5230_inst_list.asScala.toList.map(_.RetxIndex)
            val CurrentSFNSFVal: List[Integer] = bean45369.sr_5230_inst_list.asScala.toList.map(_.CurrentSFNSF)
            val ulCarrierIndexVal: List[String] = bean45369.sr_5230_inst_list.asScala.toList.map(_.ULCarrierIndex)
            val tbSizeVal: List[List[Integer]] = List(bean45369.sr_5230_inst_list.asScala.toList.map(_.PUSCHTBSize))
            logRecordQCommObj = logRecordQCommObj.copy( pUSCHTxPower = filteredPUSCHTxPowerFromBean, prb = prbVal, modUL = modULVal
              , numRecords0xB139 = bean45369.NumberofRecords, retxIndex = retxIndexVal, currentSFNSF = CurrentSFNSFVal, ulcarrierIndex = ulCarrierIndexVal, TB_Size0xB139 = tbSizeVal.flatten)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45369",
              hexLogCode = "0xB139", logRecordQComm = logRecordQCommObj)
          case 123 => val bean45369 = process45369_123(x, exceptionUseArray)
            val filteredPUSCHTxPowerFromBean: List[Integer] = bean45369.sr_5284_inst_list.asScala.toList.map(_.PUSCHTxPower.asInstanceOf[Integer])
            val prbVal: List[Integer] = bean45369.sr_5284_inst_list.asScala.toList.map(_.NumofRB)
            val modULVal: List[String] = bean45369.sr_5284_inst_list.asScala.toList.map(_.PUSCHModOrder)
            val retxIndexVal: List[String] = bean45369.sr_5284_inst_list.asScala.toList.map(_.RetxIndex)
            val CurrentSFNSFVal: List[Integer] = bean45369.sr_5284_inst_list.asScala.toList.map(_.CurrentSFNSF)
            val ulCarrierIndexVal: List[String] = bean45369.sr_5284_inst_list.asScala.toList.map(_.ULCarrierIndex)
            val tbSizeVal: List[List[Integer]] = List(bean45369.sr_5284_inst_list.asScala.toList.map(_.PUSCHTBSize))
            logRecordQCommObj = logRecordQCommObj.copy( pUSCHTxPower = filteredPUSCHTxPowerFromBean, prb = prbVal, modUL = modULVal
              , numRecords0xB139 = bean45369.NumberofRecords, retxIndex = retxIndexVal, currentSFNSF = CurrentSFNSFVal, ulcarrierIndex = ulCarrierIndexVal, TB_Size0xB139 = tbSizeVal.flatten)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45369",
              hexLogCode = "0xB139", logRecordQComm = logRecordQCommObj)
          case 124 => val bean45369 = process45369_124(x, exceptionUseArray)
            val filteredPUSCHTxPowerFromBean: List[Integer] = bean45369.sr_5341_inst_list.asScala.toList.map(_.PUSCHTxPower.asInstanceOf[Integer])
            val prbVal: List[Integer] = bean45369.sr_5341_inst_list.asScala.toList.map(_.NumofRB)
            val modULVal: List[String] = bean45369.sr_5341_inst_list.asScala.toList.map(_.PUSCHModOrder)
            val retxIndexVal: List[String] = bean45369.sr_5341_inst_list.asScala.toList.map(_.RetxIndex)
            val CurrentSFNSFVal: List[Integer] = bean45369.sr_5341_inst_list.asScala.toList.map(_.CurrentSFNSF)
            val ulCarrierIndexVal: List[String] = bean45369.sr_5341_inst_list.asScala.toList.map(_.ULCarrierIndex)
            val tbSizeVal: List[List[Integer]] = List(bean45369.sr_5341_inst_list.asScala.toList.map(_.PUSCHTBSize))
            logRecordQCommObj = logRecordQCommObj.copy( pUSCHTxPower = filteredPUSCHTxPowerFromBean, prb = prbVal, modUL = modULVal
              , numRecords0xB139 = bean45369.NumberofRecords, retxIndex = retxIndexVal, currentSFNSF = CurrentSFNSFVal, ulcarrierIndex = ulCarrierIndexVal, TB_Size0xB139 = tbSizeVal.flatten)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45369",
              hexLogCode = "0xB139", logRecordQComm = logRecordQCommObj)
          case 144 => val bean45369 = process45369_144(x, exceptionUseArray)

            val filteredPUSCHTxPowerFromBean: List[Integer] = bean45369.sr_88448_inst_list.asScala.toList.map(_.PUSCHTxPower.toInt.asInstanceOf[Integer])
            val prbVal: List[Integer] = bean45369.sr_88448_inst_list.asScala.toList.map(_.NumofRB.toInt.asInstanceOf[Integer])
            val modULVal: List[String] = bean45369.sr_88448_inst_list.asScala.toList.map(_.PUSCHModOrder)
            val retxIndexVal: List[String] = bean45369.sr_88448_inst_list.asScala.toList.map(_.RetxIndex)
            val CurrentSFNSFVal: List[Integer] = bean45369.sr_88448_inst_list.asScala.toList.map(_.CurrentSFNSF.toInt.asInstanceOf[Integer])
            val ulCarrierIndexVal: List[String] = bean45369.sr_88448_inst_list.asScala.toList.map(_.ULCarrierIndex)
            val tbSizeVal: List[List[Integer]] = List(bean45369.sr_88448_inst_list.asScala.toList.map(_.PUSCHTBSize.toInt.asInstanceOf[Integer]))
            logRecordQCommObj = logRecordQCommObj.copy(
              pUSCHTxPower = filteredPUSCHTxPowerFromBean,
              prb = prbVal,
              modUL = modULVal,
              numRecords0xB139 = bean45369.NumberofRecords.toInt,
              retxIndex = retxIndexVal,
              currentSFNSF = CurrentSFNSFVal,
              ulcarrierIndex = ulCarrierIndexVal,
              TB_Size0xB139 = tbSizeVal.flatten)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45369",
              hexLogCode = "0xB139", logRecordQComm = logRecordQCommObj)
          case 145 => val bean45369 = process45369_145(x, exceptionUseArray)

            val filteredPUSCHTxPowerFromBean: List[Integer] = bean45369.sr_97596_inst_list.asScala.toList.map(_.PUSCHTxPower.toInt.asInstanceOf[Integer])
            val prbVal: List[Integer] = bean45369.sr_97596_inst_list.asScala.toList.map(_.NumofRB.toInt.asInstanceOf[Integer])
            val modULVal: List[String] = bean45369.sr_97596_inst_list.asScala.toList.map(_.PUSCHModOrder)
            val retxIndexVal: List[String] = bean45369.sr_97596_inst_list.asScala.toList.map(_.RetxIndex)
            val CurrentSFNSFVal: List[Integer] = bean45369.sr_97596_inst_list.asScala.toList.map(_.CurrentSFNSF.toInt.asInstanceOf[Integer])
            val ulCarrierIndexVal: List[String] = bean45369.sr_97596_inst_list.asScala.toList.map(_.ULCarrierIndex)
            val tbSizeVal: List[List[Integer]] = List(bean45369.sr_97596_inst_list.asScala.toList.map(_.PUSCHTBSize.toInt.asInstanceOf[Integer]))
            logRecordQCommObj = logRecordQCommObj.copy(
              pUSCHTxPower = filteredPUSCHTxPowerFromBean,
              prb = prbVal,
              modUL = modULVal,
              numRecords0xB139 = bean45369.NumberofRecords.toInt,
              retxIndex = retxIndexVal,
              currentSFNSF = CurrentSFNSFVal,
              ulcarrierIndex = ulCarrierIndexVal,
              TB_Size0xB139 = tbSizeVal.flatten)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45369",
              hexLogCode = "0xB139", logRecordQComm = logRecordQCommObj)
          case 161 => val bean45369 = process45369_161(x, exceptionUseArray)

            val filteredPUSCHTxPowerFromBean: List[Integer] = bean45369.sr_101281_inst_list.asScala.toList.map(_.PUSCHTxPower.toInt.asInstanceOf[Integer])
            val prbVal: List[Integer] = bean45369.sr_101281_inst_list.asScala.toList.map(_.NumofRB.toInt.asInstanceOf[Integer])
            val modULVal: List[String] = bean45369.sr_101281_inst_list.asScala.toList.map(_.PUSCHModOrder)
            val retxIndexVal: List[String] = bean45369.sr_101281_inst_list.asScala.toList.map(_.RetxIndex)
            val CurrentSFNSFVal: List[Integer] = bean45369.sr_101281_inst_list.asScala.toList.map(_.CurrentSFNSF.toInt.asInstanceOf[Integer])
            val ulCarrierIndexVal: List[String] = bean45369.sr_101281_inst_list.asScala.toList.map(_.ULCarrierIndex)
            val tbSizeVal: List[List[Integer]] = List(bean45369.sr_101281_inst_list.asScala.toList.map(_.PUSCHTBSize.toInt.asInstanceOf[Integer]))
            logRecordQCommObj = logRecordQCommObj.copy(
              pUSCHTxPower = filteredPUSCHTxPowerFromBean,
              prb = prbVal,
              modUL = modULVal,
              numRecords0xB139 = bean45369.NumberofRecords.toInt,
              retxIndex = retxIndexVal,
              currentSFNSF = CurrentSFNSFVal,
              ulcarrierIndex = ulCarrierIndexVal,
              TB_Size0xB139 = tbSizeVal.flatten)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45369",
              hexLogCode = "0xB139", logRecordQComm = logRecordQCommObj)
          case 162 => val bean45369_162 = process45369_162(x, exceptionUseArray)
            val filteredPUSCHTxPowerFromBean: List[Integer] = bean45369_162.sr_125710_inst_list.asScala.toList.map(_.PUSCHTxPower.toInt.asInstanceOf[Integer])
            val prbVal: List[Integer] = bean45369_162.sr_125710_inst_list.asScala.toList.map(_.NumofRB.toInt.asInstanceOf[Integer])
            val modULVal: List[String] = bean45369_162.sr_125710_inst_list.asScala.toList.map(_.PUSCHModOrder)
            val retxIndexVal: List[String] = bean45369_162.sr_125710_inst_list.asScala.toList.map(_.RetxIndex)
            val CurrentSFNSFVal: List[Integer] = bean45369_162.sr_125710_inst_list.asScala.toList.map(_.CurrentSFNSF.toInt.asInstanceOf[Integer])
            val ulCarrierIndexVal: List[String] = bean45369_162.sr_125710_inst_list.asScala.toList.map(_.ULCarrierIndex)
            val tbSizeVal: List[List[Integer]] = List(bean45369_162.sr_125710_inst_list.asScala.toList.map(_.PUSCHTBSize.toInt.asInstanceOf[Integer]))
            logRecordQCommObj = logRecordQCommObj.copy(
              pUSCHTxPower = filteredPUSCHTxPowerFromBean,
              prb = prbVal,
              modUL = modULVal,
              numRecords0xB139 = bean45369_162.NumberofRecords.toInt,
              retxIndex = retxIndexVal,
              currentSFNSF = CurrentSFNSFVal,
              ulcarrierIndex = ulCarrierIndexVal,
              TB_Size0xB139 = tbSizeVal.flatten)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45369",
              hexLogCode = "0xB139", logRecordQComm = logRecordQCommObj)

          case _ =>
            //logger.info("Entered into Default Case for 45369(0xB139) for Version: " + logVersion)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45369", hexLogCode = "0xB139", missingVersion = logVersion)
        }
    }
    logRecord
  }

  def parseLogRecordQCOMM_Set3(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQCommObj:LogRecordQComm = LogRecordQComm()
    logCode match {

      case 45426 =>
        logVersion match {
          case 2 => val bean45426 = process45426_2(x, exceptionUseArray)
            val phrVal: List[Long] = bean45426.sr_11911_inst_list.asScala.toList.map(_.PowerHeadroom.asInstanceOf[Long])
            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB172 = bean45426.NumberofRecords, powerHeadRoom = phrVal)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45426",
              hexLogCode = "0xB172", logRecordQComm = logRecordQCommObj)
          case 24 => val bean45426 = process45426_24(x, exceptionUseArray)
            val phrVal: List[Long] = bean45426.sr_11925_inst_list.asScala.toList.map(_.PowerHeadroom.asInstanceOf[Long])
            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB172 = bean45426.NumberofRecords, powerHeadRoom = phrVal)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45426",
              hexLogCode = "0xB172", logRecordQComm = logRecordQCommObj)
          case 25 => val bean45426 = process45426_25(x, exceptionUseArray)
            var phrVal: List[String] = bean45426.sr_11941_inst_list.asScala.toList.map(_.PowerHeadroom)
            phrVal = phrVal.map(
              out => if(Constants.isValidNumeric(out, "NUMBER")) out.toString
            ).collect {case n:String => n}
            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB172 = bean45426.NumberofRecords.toInt, powerHeadRoom = phrVal.map(_.toLong))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45426",
              hexLogCode = "0xB172", logRecordQComm = logRecordQCommObj)

          case 32 => val bean45426 = process45426_32(x, exceptionUseArray)
            var phrVal: List[String] = bean45426.sr_11958_inst_list.asScala.toList.map(_.PowerHeadroom)
            phrVal = phrVal.map(
              out => if(Constants.isValidNumeric(out, "NUMBER")) out.toString
            ).collect {case n:String => n}
            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB172 = bean45426.NumberofRecords.toInt, powerHeadRoom = phrVal.map(_.toLong) )
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45426",
              hexLogCode = "0xB172", logRecordQComm = logRecordQCommObj)
          case _ =>
            //logger.info("Entered into Default Case for 45426(0xB172) for Version: " + logVersion)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45426", hexLogCode = "0xB172", missingVersion = logVersion)
        }

      case 45416 =>
        logVersion match {
          case 1 => val bean45416 = process45416_1(x, exceptionUseArray)
            logRecordQCommObj = logRecordQCommObj.copy(
              rachProcedureMode = bean45416.RACH_Procedure_Mode,
              RNTI_Type = bean45416.RNTI_Type,
              RNTI_Value = Try(bean45416.RNTI_Value.toInt.asInstanceOf[Integer]).getOrElse(null),
              timingAdvance = Try(bean45416.Timing_Advance.toInt.asInstanceOf[Integer]).getOrElse(null),
              timingAdvanceIncl = bean45416.Timing_Advance_Included)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45416",
              hexLogCode = "0xB168", logRecordQComm = logRecordQCommObj)

          case 24 => val bean45416 = process45416_24(x, exceptionUseArray)
            logRecordQCommObj = logRecordQCommObj.copy(
              rachProcedureMode = bean45416.RACH_Procedure_Mode,
              RNTI_Type = bean45416.RNTI_Type,
              RNTI_Value = Try(bean45416.RNTI_Value.toInt.asInstanceOf[Integer]).getOrElse(null),
              timingAdvance = Try(bean45416.Timing_Advance.toInt.asInstanceOf[Integer]).getOrElse(null),
              timingAdvanceIncl = bean45416.Timing_Advance_Included)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45416",
              hexLogCode = "0xB168", logRecordQComm = logRecordQCommObj)
          case _ =>
            //logger.info("Entered into Default Case for 45416(0xB168) for Version: " + logVersion)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45416", hexLogCode = "0xB168", missingVersion = logVersion)
        }

    }
    logRecord
  }

  def parseLogRecordQCOMM_Set4(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQCommObj:LogRecordQComm = LogRecordQComm()
    logCode match {

      case 4119 => val bean4119 = process4119(x, exceptionUseArray)
        logRecordQCommObj = logRecordQCommObj.copy( bandClass_QComm =bean4119.bandClass, channel0x1017 = bean4119.channel)
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "4119",
          hexLogCode = "0x1017", logRecordQComm = logRecordQCommObj)
      case 4201 => val bean4201 = process4201(x, exceptionUseArray)
        logRecordQCommObj = logRecordQCommObj.copy(evdoRxPwr = bean4201.records.asScala.toList.map(_.rxAGC0), evdoTxPwr = bean4201.records.asScala.toList.map(_.txTotalPower))
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "4201",
          hexLogCode = "0x1069",  logRecordQComm = logRecordQCommObj)
      case 4238 => val bean4238 = process4238(x, exceptionUseArray)
        logRecordQCommObj = logRecordQCommObj.copy( pilotPN = bean4238.pilotPN)
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "4238",
          hexLogCode = "0x108E", logRecordQComm = logRecordQCommObj)
      case 4506 =>
        val bean4506 = process4506(x, exceptionUseArray)

        var rfTxPowerVal: java.lang.Float = null
        var txGainAdjVal: Integer = null
        var txStatusVal: String = null


        if(bean4506.txGainAdj != 0x7F) {
          txStatusVal = "ON"
          rfTxPowerVal = bean4506.rfTxPower / 10f
        }else{
          txStatusVal = "OFF"
        }

        logRecordQCommObj = logRecordQCommObj.copy(
          pilotPNasList = bean4506.fingerInfos.asScala.toList.map(_.pilotPN),
          cdmaTxStatus = txStatusVal,
          txPower = bean4506.txPowerLimit,
          cdmaRfTxPower = rfTxPowerVal,
          txGainAdj = bean4506.txGainAdj,
          channel0x119A = bean4506.rx0CDMAChannel)
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "4506",
          hexLogCode = "0x119A", logRecordQComm = logRecordQCommObj)
      case 5435 => val bean5435 = process5435(x, exceptionUseArray)
        logRecordQCommObj = logRecordQCommObj.copy( hrpdState = bean5435.connection_state_string, pdnRat = bean5435.curr_Rat_string)
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "5435",
          hexLogCode = "0x153B",logRecordQComm = logRecordQCommObj)
      case 16679 => val bean16679 = process16679(x, exceptionUseArray)
        //logger.info("0x4127 json:"+Constants.getJsonFromBean(bean16679))
        logRecordQCommObj = logRecordQCommObj.copy(lacId = bean16679.lacId, racId = bean16679.racId,
          psc = bean16679.psc, ulFreq = bean16679.ulUarfcn, dlFreq = bean16679.dlUarfcn, wcdmaCellID = Integer.parseInt(bean16679.cellIdentity))
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "16679",
          hexLogCode = "0x4127", logRecordQComm = logRecordQCommObj)
      case 20680 => val bean20680 = process20680(x, exceptionUseArray)
        val l2StateMap = Map(0 -> "NULL", 1 -> "CONNECT_PEND", 2 -> "IDLE", 3 -> "ESTABLISHMENT_PENDING", 4 -> "RELEASE_PENDING", 5 -> "LINK_ESTABLISHED", 6 -> "TIMER_RECOVERY", 7 -> "LINK_SUSPENDED (only for SAPIO)")
        var gsmL2State: String = if (bean20680.l2State <= 7) l2StateMap(bean20680.l2State) else "Unknown"
        logRecordQCommObj = logRecordQCommObj.copy(l2state = gsmL2State)
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "20680",
          hexLogCode = "0x50C8", logRecordQComm = logRecordQCommObj)
      case 20780 => val bean20780 = process20780(x, exceptionUseArray)
        val rrStateStringsMap = Map(0 -> "RR_INACTIVE", 1 -> "RR_GOING_ACTIVE", 2 -> "RR_GOING_INACTIVE", 3 -> "RR_CELL_SELECTION", 4 -> "RR_PLMN_LIST_CONSTRUCTION", 5 -> "RR_IDLE", 6 -> "RR_CELL_RESELECTION", 7 -> "RR_CONNECTION_PENDING", 8 -> "RR_CHOOSE_CELL", 9 -> "RR_DATA_TRANSFER", 10 -> "RR_NO_CHANNELS", 11 -> "RR_CONNECTION_RELEASE", 12 -> "RR_EARLY_CAMPED_WAIT_FOR_SI", 13 -> "RR_W2G_INTERRAT_HANDOVER_PROGRESS", 14 -> "RR_W2G_INTERRAT_RESELECTION_PROGRESS", 15 -> "RR_WAIT_FOR_EARLY_PSCAN", 16 -> "GRR51", 17 -> "GRR52", 18 -> "RR_G2W_INTERRAT_RESELECTION_PROGRESS", 19 -> "RR_G2W_INTERRAT_HANDOVER_PROGRESS", 20 -> "RR_CELL_REEST", 21 -> "RR_W2G_INTERRAT_CC_ORDER_PROGRESS")
        var gsmRrState: String = if (bean20780.rrState <= 21) rrStateStringsMap(bean20780.rrState) else "Unknown"
        logRecordQCommObj = logRecordQCommObj.copy(rrstate = gsmRrState)
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "20780",
          hexLogCode = "0x512C", logRecordQComm = logRecordQCommObj)
      case 20784 => val bean20784 = process20784(x, exceptionUseArray)
        logRecordQCommObj = logRecordQCommObj.copy(txPower = bean20784.msTxpwrMaxCch, rxLev = bean20784.rxlevAccessMin)
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "20784",
          hexLogCode = "0x5130", logRecordQComm = logRecordQCommObj)
      case 20788 => val bean20788 = process20788(x, exceptionUseArray)
        //        logger.info("0x5134 json:"+Constants.getJsonFromBean(bean20788));
        logRecordQCommObj = logRecordQCommObj.copy(gsmCellID = bean20788.cellId, bsicBcc = bean20788.bsicBcc, bsicNcc = bean20788.bsicNcc, Lai = bean20788.lai.toList, bcchArfcn = bean20788.bcchArfcn.AbsoluteRfChannelNumber)
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "20788",
          hexLogCode = "0x5134", logRecordQComm = logRecordQCommObj)
      case 23240 => val bean23240 = process23240(x, exceptionUseArray)
        val l2StateMap = Map(0 -> "NULL", 1 -> "CONNECT_PEND", 2 -> "IDLE", 3 -> "ESTABLISHMENT_PENDING", 4 -> "RELEASE_PENDING", 5 -> "LINK_ESTABLISHED", 6 -> "TIMER_RECOVERY", 7 -> "LINK_SUSPENDED (only for SAPIO)")
        var gsmL2State: String = if (bean23240.l2State <= 7) l2StateMap(bean23240.l2State) else "Unknown"
        logRecordQCommObj = logRecordQCommObj.copy(l2state = gsmL2State)
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "23240",
          hexLogCode = "0x5AC8", logRecordQComm = logRecordQCommObj)
      case 23340 => val bean23340 = process23340(x, exceptionUseArray)
        val rrStateStringsMap = Map(0 -> "RR_INACTIVE", 1 -> "RR_GOING_ACTIVE", 2 -> "RR_GOING_INACTIVE", 3 -> "RR_CELL_SELECTION", 4 -> "RR_PLMN_LIST_CONSTRUCTION", 5 -> "RR_IDLE", 6 -> "RR_CELL_RESELECTION", 7 -> "RR_CONNECTION_PENDING", 8 -> "RR_CHOOSE_CELL", 9 -> "RR_DATA_TRANSFER", 10 -> "RR_NO_CHANNELS", 11 -> "RR_CONNECTION_RELEASE", 12 -> "RR_EARLY_CAMPED_WAIT_FOR_SI", 13 -> "RR_W2G_INTERRAT_HANDOVER_PROGRESS", 14 -> "RR_W2G_INTERRAT_RESELECTION_PROGRESS", 15 -> "RR_WAIT_FOR_EARLY_PSCAN", 16 -> "GRR51", 17 -> "GRR52", 18 -> "RR_G2W_INTERRAT_RESELECTION_PROGRESS", 19 -> "RR_G2W_INTERRAT_HANDOVER_PROGRESS", 20 -> "RR_CELL_REEST", 21 -> "RR_W2G_INTERRAT_CC_ORDER_PROGRESS")
        var gsmRrState: String = if (bean23340.rrState <= 21) rrStateStringsMap(bean23340.rrState) else "Unknown"
        logRecordQCommObj = logRecordQCommObj.copy(rrstate = gsmRrState)
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "23340",
          hexLogCode = "0x5B2C", logRecordQComm = logRecordQCommObj)
      case 23344 => val bean23344 = process23344(x, exceptionUseArray)
        logRecordQCommObj = logRecordQCommObj.copy( txPower = bean23344.msTxpwrMaxCch, rxLev = bean23344.rxlevAccessMin)
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "23344",
          hexLogCode = "0x5B30", logRecordQComm = logRecordQCommObj)
      case 23348 => val bean23348 = process23348(x, exceptionUseArray)
        logRecordQCommObj = logRecordQCommObj.copy( gsmCellID = bean23348.cellId, bsicBcc = bean23348.bsicBcc, bsicNcc = bean23348.bsicNcc, Lai = bean23348.lai.toList, bcchArfcn = bean23348.bcchArfcn.AbsoluteRfChannelNumber)
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "23348",
          hexLogCode = "0x5B34", logRecordQComm = logRecordQCommObj)
      case 65521 => val bean65521 = process65521(x, exceptionUseArray).toString
        logRecordQCommObj = logRecordQCommObj.copy( vvq = getJsonValueByKey(bean65521, "vvq"), vvqJitter = getJsonValueByKey(bean65521, "vvqJitter"), rtpLoss = getJsonValueByKey(bean65521, "rtpLoss"))
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "65521",
          hexLogCode = "0xFFF1", logRecordQComm = logRecordQCommObj)
    }
    logRecord
  }

  def parseLogRecordQCOMM_Set5(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQCommObj:LogRecordQComm = LogRecordQComm()
    logCode match {
      case 45422 =>
        logVersion match {
          case 4 => val bean45422 = process45422_4(x, exceptionUseArray)
            val FiFromBean: List[String] = getFilteredData(bean45422.sr_11634_inst_list.asScala.toList.map(_.Fi),Constants.CONST_NUMBER)
            val PuschTxFromBean: List[String] = getFilteredData(bean45422.sr_11634_inst_list.asScala.toList.map(_.PUSCHTxPower),Constants.CONST_NUMBER)
            val dlPathLossFromBean: List[String] = getFilteredData(bean45422.sr_11634_inst_list.asScala.toList.map(_.DLPathLoss),Constants.CONST_NUMBER)

            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB16E = bean45422.NumberofRecords.toInt,
              Fi = FiFromBean.map(_.toInt.asInstanceOf[Integer]),
              PUCCH_Tx_Power0xB16E = PuschTxFromBean.map(_.toInt.asInstanceOf[Integer]),
              DLPathLoss0xB16E = dlPathLossFromBean.map(_.toInt.asInstanceOf[Integer]))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45422", hexLogCode = "0xB16E", logRecordQComm = logRecordQCommObj)

          case 5 => val bean45422 = process45422_5(x, exceptionUseArray)
            val FiFromBean: List[String] = getFilteredData(bean45422.sr_11647_inst_list.asScala.toList.map(_.Fi),Constants.CONST_NUMBER)
            val PuschTxFromBean: List[String] = getFilteredData(bean45422.sr_11647_inst_list.asScala.toList.map(_.PUSCHTxPower),Constants.CONST_NUMBER)
            val dlPathLossFromBean: List[String] = getFilteredData(bean45422.sr_11647_inst_list.asScala.toList.map(_.DLPathLoss),Constants.CONST_NUMBER)

            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB16E = bean45422.NumberofRecords.toInt,
              Fi = FiFromBean.map(_.toInt.asInstanceOf[Integer]),
              PUCCH_Tx_Power0xB16E = PuschTxFromBean.map(_.toInt.asInstanceOf[Integer]),
              DLPathLoss0xB16E = dlPathLossFromBean.map(_.toInt.asInstanceOf[Integer]))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45422", hexLogCode = "0xB16E", logRecordQComm = logRecordQCommObj)

          case 25 => val bean45422 = process45422_25(x, exceptionUseArray)
            val FiFromBean: List[String] = getFilteredData(bean45422.sr_11683_inst_list.asScala.toList.map(_.Fi),Constants.CONST_NUMBER)
            val PuschTxFromBean: List[String] = getFilteredData(bean45422.sr_11683_inst_list.asScala.toList.map(_.PUSCHTxPower),Constants.CONST_NUMBER)
            val dlPathLossFromBean: List[String] = getFilteredData(bean45422.sr_11683_inst_list.asScala.toList.map(_.DLPathLoss),Constants.CONST_NUMBER)

            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB16E = bean45422.NumberofRecords.toInt,
              Fi = FiFromBean.map(_.toInt.asInstanceOf[Integer]),
              PUCCH_Tx_Power0xB16E = PuschTxFromBean.map(_.toInt.asInstanceOf[Integer]),
              DLPathLoss0xB16E = dlPathLossFromBean.map(_.toInt.asInstanceOf[Integer]))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45422", hexLogCode = "0xB16E", logRecordQComm = logRecordQCommObj)
          case 26 =>
            val bean45422 = process45422_26(x, exceptionUseArray)
            //logger.info("0xB16E json >>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45422))
            val FiFromBean: List[String] = getFilteredData(bean45422.sr_92562_inst_list.asScala.toList.map(_.Fi),Constants.CONST_NUMBER)
            val PuschTxFromBean: List[String] = getFilteredData(bean45422.sr_92562_inst_list.asScala.toList.map(_.PUSCHTxPower),Constants.CONST_NUMBER)
            val dlPathLossFromBean: List[String] = getFilteredData(bean45422.sr_92562_inst_list.asScala.toList.map(_.DLPathLoss),Constants.CONST_NUMBER)

            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB16E = bean45422.NumberofRecords.toInt,
              Fi = FiFromBean.map(_.toInt.asInstanceOf[Integer]),
              PUCCH_Tx_Power0xB16E = PuschTxFromBean.map(_.toInt.asInstanceOf[Integer]),
              DLPathLoss0xB16E = dlPathLossFromBean.map(_.toInt.asInstanceOf[Integer]))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45422", hexLogCode = "0xB16E", logRecordQComm = logRecordQCommObj)

          case 48 =>
            val bean45422 = process45422_48(x, exceptionUseArray)
            //logger.info("0xB16E json >>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45422))
            val FiFromBean: List[String] = getFilteredData(bean45422.sr_100636_inst_list.asScala.toList.map(_.Fi),Constants.CONST_NUMBER)
            val PuschTxFromBean: List[String] = getFilteredData(bean45422.sr_100636_inst_list.asScala.toList.map(_.PUSCHTxPower),Constants.CONST_NUMBER)
            val dlPathLossFromBean: List[String] = getFilteredData(bean45422.sr_100636_inst_list.asScala.toList.map(_.DLPathLoss),Constants.CONST_NUMBER)

            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB16E = bean45422.NumberofRecords.toInt,
              Fi = FiFromBean.map(_.toInt.asInstanceOf[Integer]),
              PUCCH_Tx_Power0xB16E = PuschTxFromBean.map(_.toInt.asInstanceOf[Integer]),
              DLPathLoss0xB16E = dlPathLossFromBean.map(_.toInt.asInstanceOf[Integer]))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45422", hexLogCode = "0xB16E", logRecordQComm = logRecordQCommObj)


          case _ =>
            //logger.info("Missing Version for logcode:45422, hexlogCode:0xB16E, Version:" + logVersion)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45422", hexLogCode = "0xB16E", missingVersion = logVersion)
        }
      case 45423 =>
        logVersion match {
          case 4 => val bean45423 = process45423_4(x, exceptionUseArray)
            val GiFromBean: List[Integer] = bean45423.sr_11744_inst_list.asScala.toList.map(_.gi)
            val PucchTxFromBean: List[Integer] = bean45423.sr_11744_inst_list.asScala.toList.map(_.PUCCHTxPower)
            val dlPathLossFromBean: List[Integer] = bean45423.sr_11744_inst_list.asScala.toList.map(_.DLPathLoss)
            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB16F = bean45423.NumberofRecords, Gi = GiFromBean,
              PUCCH_Tx_Power0xB16F = PucchTxFromBean, DLPathLoss0xB16F = dlPathLossFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45423", hexLogCode = "0xB16F", logRecordQComm = logRecordQCommObj)

          case 24 => val bean45423 = process45423_24(x, exceptionUseArray)
            val GiFromBean: List[Integer] = bean45423.sr_11761_inst_list.asScala.toList.map(_.gi)
            val PucchTxFromBean: List[Integer] = bean45423.sr_11761_inst_list.asScala.toList.map(_.PUCCHTxPower)
            val dlPathLossFromBean: List[Integer] = bean45423.sr_11761_inst_list.asScala.toList.map(_.DLPathLoss)
            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB16F = bean45423.NumberofRecords, Gi = GiFromBean,
              PUCCH_Tx_Power0xB16F = PucchTxFromBean, DLPathLoss0xB16F = dlPathLossFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45423", hexLogCode = "0xB16F", logRecordQComm = logRecordQCommObj)

          case 42 => val bean45423 = process45423_42(x, exceptionUseArray)
            val GiFromBean: List[Integer] = bean45423.sr_103789_inst_list.asScala.toList.map(_.gi.toInt.asInstanceOf[Integer])
            val PucchTxFromBean: List[Integer] = bean45423.sr_103789_inst_list.asScala.toList.map(_.PUCCHTxPower.toInt.asInstanceOf[Integer])
            val dlPathLossFromBean: List[Integer] = bean45423.sr_103789_inst_list.asScala.toList.map(_.DLPathLoss.toInt.asInstanceOf[Integer])
            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB16F = bean45423.NumberofRecords.toInt.asInstanceOf[Integer], Gi = GiFromBean,
              PUCCH_Tx_Power0xB16F = PucchTxFromBean, DLPathLoss0xB16F = dlPathLossFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45423", hexLogCode = "0xB16F", logRecordQComm = logRecordQCommObj)

          case 40 => val bean45423 = process45423_40(x, exceptionUseArray)
            val GiFromBean: List[Integer] = bean45423.sr_97651_inst_list.asScala.toList.map(_.gi.toInt.asInstanceOf[Integer])
            val PucchTxFromBean: List[Integer] = bean45423.sr_97651_inst_list.asScala.toList.map(_.PUCCHTxPower.toInt.asInstanceOf[Integer])
            val dlPathLossFromBean: List[Integer] = bean45423.sr_97651_inst_list.asScala.toList.map(_.DLPathLoss.toInt.asInstanceOf[Integer])
            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB16F = bean45423.NumberofRecords.toInt.asInstanceOf[Integer], Gi = GiFromBean,
              PUCCH_Tx_Power0xB16F = PucchTxFromBean, DLPathLoss0xB16F = dlPathLossFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45423", hexLogCode = "0xB16F", logRecordQComm = logRecordQCommObj)

          case 48 => val bean45423 = process45423_48(x, exceptionUseArray)
            val GiFromBean: List[Integer] = bean45423.sr_100440_inst_list.asScala.toList.map(_.gi.toInt.asInstanceOf[Integer])
            val PucchTxFromBean: List[Integer] = bean45423.sr_100440_inst_list.asScala.toList.map(_.PUCCHTxPower.toInt.asInstanceOf[Integer])
            val dlPathLossFromBean: List[Integer] = bean45423.sr_100440_inst_list.asScala.toList.map(_.DLPathLoss.toInt.asInstanceOf[Integer])
            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB16F = bean45423.NumberofRecords.toInt.asInstanceOf[Integer], Gi = GiFromBean,
              PUCCH_Tx_Power0xB16F = PucchTxFromBean, DLPathLoss0xB16F = dlPathLossFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45423", hexLogCode = "0xB16F", logRecordQComm = logRecordQCommObj)

          case 49 => val bean45423 = process45423_49(x, exceptionUseArray)
            val GiFromBean: List[Integer] = bean45423.sr_117921_inst_list.asScala.toList.map(_.gi.toInt.asInstanceOf[Integer])
            val PucchTxFromBean: List[Integer] = bean45423.sr_117921_inst_list.asScala.toList.map(_.PUCCHTxPower.toInt.asInstanceOf[Integer])
            val dlPathLossFromBean: List[Integer] = bean45423.sr_117921_inst_list.asScala.toList.map(_.DLPathLoss.toInt.asInstanceOf[Integer])
            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB16F = bean45423.NumberofRecords.toInt.asInstanceOf[Integer], Gi = GiFromBean,
              PUCCH_Tx_Power0xB16F = PucchTxFromBean, DLPathLoss0xB16F = dlPathLossFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45423", hexLogCode = "0xB16F", logRecordQComm = logRecordQCommObj)

          case _ =>
            //logger.info("Missing Version for logcode:45423, hexlogCode:0xB16F, Version:" + logVersion)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45423", hexLogCode = "0xB16F", missingVersion = logVersion)
        }
      case 65535 => val bean65535 = process65535(x, exceptionUseArray)
        logRecordQCommObj = logRecordQCommObj.copy(Latitude = getJsonValueByKey(bean65535.toString, "lat"), Longitude = getJsonValueByKey(bean65535.toString, "lng"), hasGps = 1)
        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "65535",
          hexLogCode = "0xFFFF",  logRecordQComm = logRecordQCommObj)
      case 4116 =>
        //logger.info("0x1014(4116) Raw Payload for Timestamp : "+logRecord.dmTimeStamp+" ........"+OPutil.printHexBinary(x,true))
        val bean4116 = process4116(x, exceptionUseArray)
        //logger.info("0x1014(4116) Raw Payload for Timestamp : "+logRecord.dmTimeStamp+" with File : "+logRecord.fileName+" >>>>>>> " + Constants.getJsonFromBean(bean4116))
        if (bean4116 != null) {
          logRecordQCommObj = logRecordQCommObj.copy(Latitude2 = bean4116.lat.toString, Longitude2 = bean4116.lng.toString, hasGps = 1)
          logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "4116",
            hexLogCode = "0x1014", logRecordQComm = logRecordQCommObj)
        }
      case 5238 =>
        //logger.info("0x1476(5238) Raw Payload for Timestamp : "+logRecord.dmTimeStamp+" ........"+OPutil.printHexBinary(x,true))
        val bean5238 = process5238(x, exceptionUseArray)
        //logger.info("0x1476(5238) Raw Payload for Timestamp : "+logRecord.dmTimeStamp+" with File : "+logRecord.fileName+" >>>>>>> " + Constants.getJsonFromBean(bean5238))
        if (bean5238 != null) {
          logRecordQCommObj = logRecordQCommObj.copy(Latitude2 = bean5238.lat.toString, Longitude2 = bean5238.lng.toString, hasGps = 1)
          logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "5238",
            hexLogCode = "0x1476", logRecordQComm = logRecordQCommObj)
        }
    }
    logRecord
  }

  def parseLogRecordQCOMM_Set6(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQCommObj:LogRecordQComm = LogRecordQComm()
    logCode match {
      case 45427 =>
        logVersion match {

          case 5 => val bean45427 = process45427_5(x, exceptionUseArray)
            //logger.info("0xB173 json:" + Constants.getJsonFromBean(bean45427))
            val CrcResultFromBean: List[String] = bean45427.sr_36823_inst_list.asScala.toList.map(_.sr_36831_inst_list.asScala.toList.map(_.CRCResult)).flatten
            val DiscardedreTxPresentFromBean: List[String] = bean45427.sr_36823_inst_list.asScala.toList.map(_.sr_36831_inst_list.asScala.toList.map(_.DiscardedreTxPresent)).flatten
            val HarqIDFromBean: List[Integer] = bean45427.sr_36823_inst_list.asScala.toList.map(_.sr_36831_inst_list.asScala.toList.map(_.HARQID)).flatten
            val NdiFromBean: List[Integer] = bean45427.sr_36823_inst_list.asScala.toList.map(_.sr_36831_inst_list.asScala.toList.map(_.NDI)).flatten
            val RvFromBean: List[Integer] = bean45427.sr_36823_inst_list.asScala.toList.map(_.sr_36831_inst_list.asScala.toList.map(_.RV)).flatten
            val TB_IndexFromBean: List[Integer] = bean45427.sr_36823_inst_list.asScala.toList.map(_.sr_36831_inst_list.asScala.toList.map(_.TBIndex)).flatten
            val TB_SizeFromBean: List[List[Integer]] = bean45427.sr_36823_inst_list.asScala.toList.map(_.sr_36831_inst_list.asScala.toList.map(_.TBSize))
            val ModulationTypeFromBean: List[String] = bean45427.sr_36823_inst_list.asScala.toList.map(_.sr_36831_inst_list.asScala.toList.map(_.ModulationType)).flatten
            val prbs: List[List[Integer]] = bean45427.sr_36823_inst_list.asScala.toList.map(_.sr_36831_inst_list.asScala.toList.map(_.NumRBs))
            val TB_Size = mutable.MutableList[Integer]()
            val frameNumList: List[Integer] = bean45427.sr_36823_inst_list.asScala.toList.map(_.FrameNum)
            val subFrameNumList: List[Integer] = bean45427.sr_36823_inst_list.asScala.toList.map(_.SubframeNum)
            val distServingCellIndex: Integer = bean45427.sr_36823_inst_list.asScala.toList.map(_.ServingCellIndex).distinct.size
            var firstSfn = 0
            var lastSfn = 0
            var firstFn = 0
            var lastFn = 0
            if(frameNumList.size > 0){
              firstFn = frameNumList(0)
              lastFn = frameNumList(frameNumList.size - 1)
            }
            if(subFrameNumList.size > 0){
              firstSfn = subFrameNumList(0)
              lastSfn = subFrameNumList(subFrameNumList.size - 1)
            }

            if (TB_SizeFromBean != null) {

              for (i <- 0 to TB_SizeFromBean.size - 1) {
                val TB_SizeTemp: List[Integer] = TB_SizeFromBean(i)
                var tbSizeSum: Integer = 0

                for (j <- 0 to TB_SizeTemp.size - 1) {
                  if(CrcResultFromBean(j).equalsIgnoreCase("pass")) {
                    tbSizeSum += TB_SizeTemp(j)
                  }
                  else{
                    tbSizeSum += 0
                  }
                }
                TB_Size += tbSizeSum
              }
            }

            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB173 = bean45427.NumRecords, CrcResult = CrcResultFromBean,
              DiscardedreTxPresent = List(DiscardedreTxPresentFromBean), HarqID = List(HarqIDFromBean), NDI = List(NdiFromBean), RV = List(RvFromBean), TB_Index = List(TB_IndexFromBean),
              modulationType = ModulationTypeFromBean, numPRB0xB173 = prbs.flatten, TB_Size0xB173 = TB_Size.toList,
              firstFN_0xB173 = firstFn, lastFN_0xB173 = lastFn, firstSFN_0xB173 = firstSfn, lastSFN_0xB173 = lastSfn, distServingCellIndex_0xB173 = distServingCellIndex)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45427", hexLogCode = "0xB173", logRecordQComm = logRecordQCommObj)

          case 24 =>
            val bean45427 = process45427_24(x, exceptionUseArray)
            //logger.info("0xB173 json:" + Constants.getJsonFromBean(bean45427))
            val FrameNumFromBean: List[Integer] = bean45427.sr_12120_inst_list.asScala.toList.map(_.FrameNum)
            val NumLayersFromBean: List[Integer] = bean45427.sr_12120_inst_list.asScala.toList.map(_.NumLayers)
            val NumTransportBlocksPresentFromBean: List[Integer] = bean45427.sr_12120_inst_list.asScala.toList.map(_.NumTransportBlocksPresent)
            val ServingCellIndexFromBean: List[String] = bean45427.sr_12120_inst_list.asScala.toList.map(_.ServingCellIndex)
            val CrcResultFromBean: List[String] = bean45427.sr_12120_inst_list.asScala.toList.map(_.sr_12129_inst_list.asScala.toList.map(_.CRCResult)).flatten
            val DiscardedreTxPresentFromBean: List[List[String]] = bean45427.sr_12120_inst_list.asScala.toList.map(_.sr_12129_inst_list.asScala.toList.map(_.DiscardedreTxPresent))
            val HarqIDFromBean: List[List[Integer]] = bean45427.sr_12120_inst_list.asScala.toList.map(_.sr_12129_inst_list.asScala.toList.map(_.HARQID))
            val NdiFromBean: List[List[Integer]] = bean45427.sr_12120_inst_list.asScala.toList.map(_.sr_12129_inst_list.asScala.toList.map(_.NDI))
            val RvFromBean: List[List[Integer]] = bean45427.sr_12120_inst_list.asScala.toList.map(_.sr_12129_inst_list.asScala.toList.map(_.RV))
            val TB_IndexFromBean: List[List[Integer]] = bean45427.sr_12120_inst_list.asScala.toList.map(_.sr_12129_inst_list.asScala.toList.map(_.TBIndex))
            val TB_SizeFromBean: List[List[Integer]] = bean45427.sr_12120_inst_list.asScala.toList.map(_.sr_12129_inst_list.asScala.toList.map(_.TBSize))
            val McsFromBean: List[List[Integer]] = bean45427.sr_12120_inst_list.asScala.toList.map(_.sr_12129_inst_list.asScala.toList.map(_.MCS))
            val ModulationTypeFromBean: List[String] = bean45427.sr_12120_inst_list.asScala.toList.map(_.sr_12129_inst_list.asScala.toList.map(_.ModulationType)).flatten
            val DidRecombiningFromBean: List[List[String]] = bean45427.sr_12120_inst_list.asScala.toList.map(_.sr_12129_inst_list.asScala.toList.map(_.DidRecombining))
            val SubFrameNumVal: List[Integer] = bean45427.sr_12120_inst_list.asScala.toList.map(_.SubframeNum)
            val prbs: List[List[Integer]] = bean45427.sr_12120_inst_list.asScala.toList.map(_.sr_12129_inst_list.asScala.toList.map(_.NumRBs))
            val TB_Size = mutable.MutableList[Integer]()
            val distServingCellIndex: Integer = bean45427.sr_12120_inst_list.asScala.toList.map(_.ServingCellIndex).distinct.size
            var firstSfn = 0
            var lastSfn = 0
            var firstFn = 0
            var lastFn = 0
            if(FrameNumFromBean.size > 0){
              firstFn = FrameNumFromBean(0)
              lastFn = FrameNumFromBean(FrameNumFromBean.size - 1)
            }
            if(SubFrameNumVal.size > 0){
              firstSfn = SubFrameNumVal(0)
              lastSfn = SubFrameNumVal(SubFrameNumVal.size - 1)
            }
            if (TB_SizeFromBean != null) {

              for (i <- 0 to TB_SizeFromBean.size - 1) {
                val TB_SizeTemp: List[Integer] = TB_SizeFromBean(i)
                var tbSizeSum: Integer = 0

                for (j <- 0 to TB_SizeTemp.size - 1) {
                  if(CrcResultFromBean(j).equalsIgnoreCase("pass")) {
                    tbSizeSum += TB_SizeTemp(j)
                  }
                  else{
                    tbSizeSum += 0
                  }
                }
                TB_Size += tbSizeSum
              }
            }
            var pcellTBSizeAggr = 0
            var scc1TBSizeAggr = 0
            var scc2TBSizeAggr = 0
            var scc3TBSizeAggr = 0
            var scc4TBSizeAggr = 0
            var scc5TBSizeAggr = 0
            var scc6TBSizeAggr = 0
            var scc7TBSizeAggr = 0
            var tBSizeAggr = 0

            for(i <- 0 to ServingCellIndexFromBean.size - 1){
              if(ServingCellIndexFromBean(i)!=null){
                ServingCellIndexFromBean(i).toLowerCase match {
                  case "pcell" =>
                    pcellTBSizeAggr += TB_Size(i)
                  case "scc1" =>
                    scc1TBSizeAggr += TB_Size(i)
                  case "scc2" =>
                    scc2TBSizeAggr += TB_Size(i)
                  case "scc3" =>
                    scc3TBSizeAggr += TB_Size(i)
                  case "scc4" =>
                    scc4TBSizeAggr += TB_Size(i)
                  case "scc5" =>
                    scc5TBSizeAggr += TB_Size(i)
                  case "scc6" =>
                    scc6TBSizeAggr += TB_Size(i)
                  case "scc7" =>
                    scc7TBSizeAggr += TB_Size(i)
                }
              }
            }
            tBSizeAggr = pcellTBSizeAggr + scc1TBSizeAggr + scc2TBSizeAggr + scc3TBSizeAggr + scc4TBSizeAggr + scc5TBSizeAggr + scc6TBSizeAggr + scc7TBSizeAggr
            logRecordQCommObj = logRecordQCommObj.copy( numRecords0xB173 = bean45427.NumRecords, frameNum = FrameNumFromBean, numLayers = NumLayersFromBean, numTransportBlocksPresent = NumTransportBlocksPresentFromBean,
              CrcResult = CrcResultFromBean, DiscardedreTxPresent = DiscardedreTxPresentFromBean, HarqID = HarqIDFromBean, NDI = NdiFromBean, RV = RvFromBean, TB_Index = TB_IndexFromBean,
              ServingCellIndex = ServingCellIndexFromBean, TB_Size0xB173 = TB_Size.toList, numPRB0xB173 = prbs.flatten, SubFrameNum = SubFrameNumVal, modulationType = ModulationTypeFromBean,
              firstFN_0xB173 = firstFn, lastFN_0xB173 = lastFn, firstSFN_0xB173 = firstSfn, lastSFN_0xB173 = lastSfn, pcellTBSize_0xB173 = pcellTBSizeAggr, scc1TBSize_0xB173 = scc1TBSizeAggr,
              scc2TBSize_0xB173 = scc2TBSizeAggr,  scc3TBSize_0xB173 = scc3TBSizeAggr,  scc4TBSize_0xB173 = scc4TBSizeAggr,  scc5TBSize_0xB173 = scc5TBSizeAggr,  scc6TBSize_0xB173 = scc6TBSizeAggr,
              scc7TBSize_0xB173 = scc7TBSizeAggr, totalTBSize_0xB173 = tBSizeAggr,distServingCellIndex_0xB173 = distServingCellIndex
            )
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45427", hexLogCode = "0xB173", logRecordQComm = logRecordQCommObj)
          case 32 => val bean45427 = process45427_32(x, exceptionUseArray)
            //logger.info("0xB173 json:" + Constants.getJsonFromBean(bean45427))
            val FrameNumFromBean: List[Integer] = bean45427.sr_12150_inst_list.asScala.toList.map(_.FrameNum)
            val NumLayersFromBean: List[Integer] = bean45427.sr_12150_inst_list.asScala.toList.map(_.NumLayers)
            val NumTransportBlocksPresentFromBean: List[Integer] = bean45427.sr_12150_inst_list.asScala.toList.map(_.NumTransportBlocksPresent)
            val ServingCellIndexFromBean: List[String] = bean45427.sr_12150_inst_list.asScala.toList.map(_.ServingCellIndex)
            val CrcResultFromBean: List[String] = bean45427.sr_12150_inst_list.asScala.toList.map(_.sr_12158_inst_list.asScala.toList.map(_.CRCResult)).flatten
            val DiscardedreTxPresentFromBean: List[List[String]] = bean45427.sr_12150_inst_list.asScala.toList.map(_.sr_12158_inst_list.asScala.toList.map(_.DiscardedreTxPresent))
            val HarqIDFromBean: List[List[Integer]] = bean45427.sr_12150_inst_list.asScala.toList.map(_.sr_12158_inst_list.asScala.toList.map(_.HARQID))
            val NdiFromBean: List[List[Integer]] = bean45427.sr_12150_inst_list.asScala.toList.map(_.sr_12158_inst_list.asScala.toList.map(_.NDI))
            val RvFromBean: List[List[Integer]] = bean45427.sr_12150_inst_list.asScala.toList.map(_.sr_12158_inst_list.asScala.toList.map(_.RV))
            val TB_IndexFromBean: List[List[Integer]] = bean45427.sr_12150_inst_list.asScala.toList.map(_.sr_12158_inst_list.asScala.toList.map(_.TBIndex))
            val TB_SizeFromBean: List[List[Integer]] = bean45427.sr_12150_inst_list.asScala.toList.map(_.sr_12158_inst_list.asScala.toList.map(_.TBSize))
            val McsFromBean: List[List[Integer]] = bean45427.sr_12150_inst_list.asScala.toList.map(_.sr_12158_inst_list.asScala.toList.map(_.MCS))
            val ModulationTypeFromBean: List[String] = bean45427.sr_12150_inst_list.asScala.toList.map(_.sr_12158_inst_list.asScala.toList.map(_.ModulationType)).flatten
            val DidRecombiningFromBean: List[List[String]] = bean45427.sr_12150_inst_list.asScala.toList.map(_.sr_12158_inst_list.asScala.toList.map(_.DidRecombining))
            val SubFrameNum: List[Integer] = bean45427.sr_12150_inst_list.asScala.toList.map(_.SubframeNum)
            val prbs: List[List[Integer]] = bean45427.sr_12150_inst_list.asScala.toList.map(_.sr_12158_inst_list.asScala.toList.map(_.NumRBs))
            val TB_Size = mutable.MutableList[Integer]()
            val distServingCellIndex: Integer = bean45427.sr_12150_inst_list.asScala.toList.map(_.ServingCellIndex).distinct.size
            var firstSfn = 0
            var lastSfn = 0
            var firstFn = 0
            var lastFn = 0
            if(FrameNumFromBean.size > 0){
              firstFn = FrameNumFromBean(0)
              lastFn = FrameNumFromBean(FrameNumFromBean.size - 1)
            }
            if(SubFrameNum.size > 0){
              firstSfn = SubFrameNum(0)
              lastSfn = SubFrameNum(SubFrameNum.size - 1)
            }
            if (TB_SizeFromBean != null) {

              for (i <- 0 to TB_SizeFromBean.size - 1) {
                val TB_SizeTemp: List[Integer] = TB_SizeFromBean(i)
                var tbSizeSum: Integer = 0

                for (j <- 0 to TB_SizeTemp.size - 1) {
                  if(CrcResultFromBean(j).equalsIgnoreCase("pass")) {
                    tbSizeSum += TB_SizeTemp(j)
                  }
                  else{
                    tbSizeSum += 0
                  }
                }
                TB_Size += tbSizeSum
              }
            }
            var pcellTBSizeAggr = 0
            var scc1TBSizeAggr = 0
            var scc2TBSizeAggr = 0
            var scc3TBSizeAggr = 0
            var scc4TBSizeAggr = 0
            var scc5TBSizeAggr = 0
            var scc6TBSizeAggr = 0
            var scc7TBSizeAggr = 0

            for(i <- 0 to ServingCellIndexFromBean.size - 1){
              if(ServingCellIndexFromBean(i)!=null){
                ServingCellIndexFromBean(i).toLowerCase match {
                  case "pcell" =>
                    pcellTBSizeAggr += TB_Size(i)
                  case "scc1" =>
                    scc1TBSizeAggr += TB_Size(i)
                  case "scc2" =>
                    scc2TBSizeAggr += TB_Size(i)
                  case "scc3" =>
                    scc3TBSizeAggr += TB_Size(i)
                  case "scc4" =>
                    scc4TBSizeAggr += TB_Size(i)
                  case "scc5" =>
                    scc5TBSizeAggr += TB_Size(i)
                  case "scc6" =>
                    scc6TBSizeAggr += TB_Size(i)
                  case "scc7" =>
                    scc7TBSizeAggr += TB_Size(i)
                }
              }
            }

            logRecordQCommObj = logRecordQCommObj.copy( numRecords0xB173 = bean45427.NumRecords, frameNum = FrameNumFromBean, numLayers = NumLayersFromBean, numTransportBlocksPresent = NumTransportBlocksPresentFromBean,
              CrcResult = CrcResultFromBean, DiscardedreTxPresent = DiscardedreTxPresentFromBean, HarqID = HarqIDFromBean, NDI = NdiFromBean, RV = RvFromBean, TB_Index = TB_IndexFromBean, ServingCellIndex = ServingCellIndexFromBean,
              TB_Size0xB173 = TB_Size.toList, modulationType = ModulationTypeFromBean, numPRB0xB173 = prbs.flatten,
              firstFN_0xB173 = firstFn, lastFN_0xB173 = lastFn, firstSFN_0xB173 = firstSfn, lastSFN_0xB173 = lastSfn, pcellTBSize_0xB173 = pcellTBSizeAggr, scc1TBSize_0xB173 = scc1TBSizeAggr,
              scc2TBSize_0xB173 = scc2TBSizeAggr,  scc3TBSize_0xB173 = scc3TBSizeAggr,  scc4TBSize_0xB173 = scc4TBSizeAggr,  scc5TBSize_0xB173 = scc5TBSizeAggr,  scc6TBSize_0xB173 = scc6TBSizeAggr,
              scc7TBSize_0xB173 = scc7TBSizeAggr, distServingCellIndex_0xB173 = distServingCellIndex
            )
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45427", hexLogCode = "0xB173", logRecordQComm = logRecordQCommObj)
          case 34 => val bean45427 = process45427_34(x, exceptionUseArray)
            //logger.info("0xB173 json:" + Constants.getJsonFromBean(bean45427))
            val FrameNumFromBean: List[Integer] = bean45427.sr_12205_inst_list.asScala.toList.map(_.FrameNum.asInstanceOf[Integer])
            val NumLayersFromBean: List[Integer] = bean45427.sr_12205_inst_list.asScala.toList.map(_.NumLayers.asInstanceOf[Integer])
            val NumTransportBlocksPresentFromBean: List[Integer] = bean45427.sr_12205_inst_list.asScala.toList.map(_.NumTransportBlocksPresent.asInstanceOf[Integer])
            val ServingCellIndexFromBean: List[String] = bean45427.sr_12205_inst_list.asScala.toList.map(_.ServingCellIndex)
            val CrcResultFromBean: List[String] = bean45427.sr_12205_inst_list.asScala.toList.map(_.sr_12213_inst_list.asScala.toList.map(_.CRCResult)).flatten
            val DiscardedreTxPresentFromBean: List[List[String]] = bean45427.sr_12205_inst_list.asScala.toList.map(_.sr_12213_inst_list.asScala.toList.map(_.DiscardedReTx))
            val HarqIDFromBean: List[List[Integer]] = bean45427.sr_12205_inst_list.asScala.toList.map(_.sr_12213_inst_list.asScala.toList.map(_.HARQID.asInstanceOf[Integer]))
            val NdiFromBean: List[List[Integer]] = bean45427.sr_12205_inst_list.asScala.toList.map(_.sr_12213_inst_list.asScala.toList.map(_.NDI.asInstanceOf[Integer]))
            val RvFromBean: List[List[Integer]] = bean45427.sr_12205_inst_list.asScala.toList.map(_.sr_12213_inst_list.asScala.toList.map(_.RV.asInstanceOf[Integer]))
            val TB_IndexFromBean: List[List[Integer]] = bean45427.sr_12205_inst_list.asScala.toList.map(_.sr_12213_inst_list.asScala.toList.map(_.TBIndex.asInstanceOf[Integer]))
            val TB_SizeFromBean: List[List[Integer]] = bean45427.sr_12205_inst_list.asScala.toList.map(_.sr_12213_inst_list.asScala.toList.map(_.TBSize.asInstanceOf[Integer]))
            val McsFromBean: List[List[Integer]] = bean45427.sr_12205_inst_list.asScala.toList.map(_.sr_12213_inst_list.asScala.toList.map(_.MCS.asInstanceOf[Integer]))
            val ModulationTypeFromBean: List[String] = bean45427.sr_12205_inst_list.asScala.toList.map(_.sr_12213_inst_list.asScala.toList.map(_.ModulationType)).flatten
            val DidRecombiningFromBean: List[List[String]] = bean45427.sr_12205_inst_list.asScala.toList.map(_.sr_12213_inst_list.asScala.toList.map(_.DidRecombining))
            val SubFrameNumVal: List[Integer] = bean45427.sr_12205_inst_list.asScala.toList.map(_.SubframeNum.asInstanceOf[Integer])
            val prbs: List[List[Integer]] = bean45427.sr_12205_inst_list.asScala.toList.map(_.sr_12213_inst_list.asScala.toList.map(_.NumRBs.asInstanceOf[Integer]))
            val TB_Size = mutable.MutableList[Integer]()
            val distServingCellIndex: Integer = bean45427.sr_12205_inst_list.asScala.toList.map(_.ServingCellIndex).distinct.size
            var firstSfn = 0
            var lastSfn = 0
            var firstFn = 0
            var lastFn = 0
            if(FrameNumFromBean.size > 0){
              firstFn = FrameNumFromBean(0)
              lastFn = FrameNumFromBean(FrameNumFromBean.size - 1)
            }
            if(SubFrameNumVal.size > 0){
              firstSfn = SubFrameNumVal(0)
              lastSfn = SubFrameNumVal(SubFrameNumVal.size - 1)
            }
            if (TB_SizeFromBean != null) {

              for (i <- 0 to TB_SizeFromBean.size - 1) {
                val TB_SizeTemp: List[Integer] = TB_SizeFromBean(i)
                var tbSizeSum: Integer = 0

                for (j <- 0 to TB_SizeTemp.size - 1) {
                  if(CrcResultFromBean(j).equalsIgnoreCase("pass")) {
                    tbSizeSum += TB_SizeTemp(j)
                  }
                  else{
                    tbSizeSum += 0
                  }
                }
                TB_Size += tbSizeSum
              }
            }
            var pcellTBSizeAggr = 0
            var scc1TBSizeAggr = 0
            var scc2TBSizeAggr = 0
            var scc3TBSizeAggr = 0
            var scc4TBSizeAggr = 0
            var scc5TBSizeAggr = 0
            var scc6TBSizeAggr = 0
            var scc7TBSizeAggr = 0

            for(i <- 0 to ServingCellIndexFromBean.size - 1){
              ServingCellIndexFromBean(i).toLowerCase match {
                case "pcell" =>
                  pcellTBSizeAggr += TB_Size(i)
                case "scc1" =>
                  scc1TBSizeAggr += TB_Size(i)
                case "scc2" =>
                  scc2TBSizeAggr += TB_Size(i)
                case "scc3" =>
                  scc3TBSizeAggr += TB_Size(i)
                case "scc4" =>
                  scc4TBSizeAggr += TB_Size(i)
                case "scc5" =>
                  scc5TBSizeAggr += TB_Size(i)
                case "scc6" =>
                  scc6TBSizeAggr += TB_Size(i)
                case "scc7" =>
                  scc7TBSizeAggr += TB_Size(i)
              }
            }

            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB173 = bean45427.NumRecords.asInstanceOf[Integer], frameNum = FrameNumFromBean, numLayers = NumLayersFromBean, numTransportBlocksPresent = NumTransportBlocksPresentFromBean,
              CrcResult = CrcResultFromBean, DiscardedreTxPresent = DiscardedreTxPresentFromBean, HarqID = HarqIDFromBean, NDI = NdiFromBean, RV = RvFromBean, TB_Index = TB_IndexFromBean,
              ServingCellIndex = ServingCellIndexFromBean, TB_Size0xB173 = TB_Size.toList, SubFrameNum = SubFrameNumVal, modulationType = ModulationTypeFromBean, numPRB0xB173 = prbs.flatten,
              firstFN_0xB173 = firstFn, lastFN_0xB173 = lastFn, firstSFN_0xB173 = firstSfn, lastSFN_0xB173 = lastSfn, pcellTBSize_0xB173 = pcellTBSizeAggr, scc1TBSize_0xB173 = scc1TBSizeAggr,
              scc2TBSize_0xB173 = scc2TBSizeAggr,  scc3TBSize_0xB173 = scc3TBSizeAggr,  scc4TBSize_0xB173 = scc4TBSizeAggr,  scc5TBSize_0xB173 = scc5TBSizeAggr,  scc6TBSize_0xB173 = scc6TBSizeAggr,
              scc7TBSize_0xB173 = scc7TBSizeAggr, distServingCellIndex_0xB173 = distServingCellIndex
            )
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45427", hexLogCode = "0xB173", logRecordQComm = logRecordQCommObj)

          case 35 => val bean45427 = process45427_35(x, exceptionUseArray)
            //logger.info("0xB173 json:" + Constants.getJsonFromBean(bean45427))
            val FrameNumFromBean: List[Integer] = bean45427.sr_12234_inst_list.asScala.toList.map(_.FrameNum)
            val NumLayersFromBean: List[Integer] = bean45427.sr_12234_inst_list.asScala.toList.map(_.NumLayers)
            val NumTransportBlocksPresentFromBean: List[Integer] = bean45427.sr_12234_inst_list.asScala.toList.map(_.NumTransportBlocksPresent)
            val ServingCellIndexFromBean: List[String] = bean45427.sr_12234_inst_list.asScala.toList.map(_.ServingCellIndex)
            val CrcResultFromBean: List[String] = bean45427.sr_12234_inst_list.asScala.toList.map(_.sr_12243_inst_list.asScala.toList.map(_.CRCResult)).flatten
            val DiscardedreTxPresentFromBean: List[List[String]] = bean45427.sr_12234_inst_list.asScala.toList.map(_.sr_12243_inst_list.asScala.toList.map(_.DiscardedReTx))
            val HarqIDFromBean: List[List[Integer]] = bean45427.sr_12234_inst_list.asScala.toList.map(_.sr_12243_inst_list.asScala.toList.map(_.HARQID))
            val NdiFromBean: List[List[Integer]] = bean45427.sr_12234_inst_list.asScala.toList.map(_.sr_12243_inst_list.asScala.toList.map(_.NDI))
            val RvFromBean: List[List[Integer]] = bean45427.sr_12234_inst_list.asScala.toList.map(_.sr_12243_inst_list.asScala.toList.map(_.RV))
            val TB_IndexFromBean: List[List[Integer]] = bean45427.sr_12234_inst_list.asScala.toList.map(_.sr_12243_inst_list.asScala.toList.map(_.TBIndex))
            val TB_SizeFromBean: List[List[Integer]] = bean45427.sr_12234_inst_list.asScala.toList.map(_.sr_12243_inst_list.asScala.toList.map(_.TBSize))
            val McsFromBean: List[List[Integer]] = bean45427.sr_12234_inst_list.asScala.toList.map(_.sr_12243_inst_list.asScala.toList.map(_.MCS))
            val ModulationTypeFromBean: List[String] = bean45427.sr_12234_inst_list.asScala.toList.map(_.sr_12243_inst_list.asScala.toList.map(_.ModulationType)).flatten
            val DidRecombiningFromBean: List[List[String]] = bean45427.sr_12234_inst_list.asScala.toList.map(_.sr_12243_inst_list.asScala.toList.map(_.DidRecombining))
            val SubFrameNumVal: List[Integer] = bean45427.sr_12234_inst_list.asScala.toList.map(_.SubframeNum)
            val prbs: List[List[Integer]] = bean45427.sr_12234_inst_list.asScala.toList.map(_.sr_12243_inst_list.asScala.toList.map(_.NumRBs))
            val TB_Size = mutable.MutableList[Integer]()
            val distServingCellIndex: Integer = bean45427.sr_12234_inst_list.asScala.toList.map(_.ServingCellIndex).distinct.size
            var firstSfn = 0
            var lastSfn = 0
            var firstFn = 0
            var lastFn = 0
            if(FrameNumFromBean.size > 0){
              firstFn = FrameNumFromBean(0)
              lastFn = FrameNumFromBean(FrameNumFromBean.size - 1)
            }
            if(SubFrameNumVal.size > 0){
              firstSfn = SubFrameNumVal(0)
              lastSfn = SubFrameNumVal(SubFrameNumVal.size - 1)
            }
            if (TB_SizeFromBean != null) {

              for (i <- 0 to TB_SizeFromBean.size - 1) {
                val TB_SizeTemp: List[Integer] = TB_SizeFromBean(i)
                var tbSizeSum: Integer = 0

                for (j <- 0 to TB_SizeTemp.size - 1) {
                  if(CrcResultFromBean(j).equalsIgnoreCase("pass")) {
                    tbSizeSum += TB_SizeTemp(j)
                  }
                  else{
                    tbSizeSum += 0
                  }
                }
                TB_Size += tbSizeSum
              }
            }
            var pcellTBSizeAggr = 0
            var scc1TBSizeAggr = 0
            var scc2TBSizeAggr = 0
            var scc3TBSizeAggr = 0
            var scc4TBSizeAggr = 0
            var scc5TBSizeAggr = 0
            var scc6TBSizeAggr = 0
            var scc7TBSizeAggr = 0

            for(i <- 0 to ServingCellIndexFromBean.size - 1){
              ServingCellIndexFromBean(i).toLowerCase match {
                case "pcell" =>
                  pcellTBSizeAggr += TB_Size(i)
                case "scc1" =>
                  scc1TBSizeAggr += TB_Size(i)
                case "scc2" =>
                  scc2TBSizeAggr += TB_Size(i)
                case "scc3" =>
                  scc3TBSizeAggr += TB_Size(i)
                case "scc4" =>
                  scc4TBSizeAggr += TB_Size(i)
                case "scc5" =>
                  scc5TBSizeAggr += TB_Size(i)
                case "scc6" =>
                  scc6TBSizeAggr += TB_Size(i)
                case "scc7" =>
                  scc7TBSizeAggr += TB_Size(i)
              }
            }

            logRecordQCommObj = logRecordQCommObj.copy(numRecords0xB173 = bean45427.NumRecords, frameNum = FrameNumFromBean, numLayers = NumLayersFromBean, numTransportBlocksPresent = NumTransportBlocksPresentFromBean,
              CrcResult = CrcResultFromBean, DiscardedreTxPresent = DiscardedreTxPresentFromBean, HarqID = HarqIDFromBean, NDI = NdiFromBean, RV = RvFromBean, TB_Index = TB_IndexFromBean,
              ServingCellIndex = ServingCellIndexFromBean, TB_Size0xB173 = TB_Size.toList, SubFrameNum = SubFrameNumVal, modulationType = ModulationTypeFromBean, numPRB0xB173 = prbs.flatten,
              firstFN_0xB173 = firstFn, lastFN_0xB173 = lastFn, firstSFN_0xB173 = firstSfn, lastSFN_0xB173 = lastSfn, pcellTBSize_0xB173 = pcellTBSizeAggr, scc1TBSize_0xB173 = scc1TBSizeAggr,
              scc2TBSize_0xB173 = scc2TBSizeAggr,  scc3TBSize_0xB173 = scc3TBSizeAggr,  scc4TBSize_0xB173 = scc4TBSizeAggr,  scc5TBSize_0xB173 = scc5TBSizeAggr,  scc6TBSize_0xB173 = scc6TBSizeAggr,
              scc7TBSize_0xB173 = scc7TBSizeAggr, distServingCellIndex_0xB173 = distServingCellIndex
            )
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45427", hexLogCode = "0xB173", logRecordQComm = logRecordQCommObj)
          case 36 => val bean45427 = process45427_36(x, exceptionUseArray)
            //logger.info("0xB173 json:" + Constants.getJsonFromBean(bean45427))
            val FrameNumFromBean: List[Integer] = bean45427.sr_12265_inst_list.asScala.toList.map(_.FrameNum)
            val NumLayersFromBean: List[Integer] = bean45427.sr_12265_inst_list.asScala.toList.map(_.NumLayers)
            val NumTransportBlocksPresentFromBean: List[Integer] = bean45427.sr_12265_inst_list.asScala.toList.map(_.NumTransportBlocksPresent)
            val ServingCellIndexFromBean: List[String] = bean45427.sr_12265_inst_list.asScala.toList.map(_.ServingCellIndex)
            val CrcResultFromBean: List[String] = bean45427.sr_12265_inst_list.asScala.toList.map(_.sr_12274_inst_list.asScala.toList.map(_.CRCResult)).flatten
            val DiscardedreTxPresentFromBean: List[List[String]] = bean45427.sr_12265_inst_list.asScala.toList.map(_.sr_12274_inst_list.asScala.toList.map(_.DiscardedReTx))
            val HarqIDFromBean: List[List[Integer]] = bean45427.sr_12265_inst_list.asScala.toList.map(_.sr_12274_inst_list.asScala.toList.map(_.HARQID))
            val NdiFromBean: List[List[Integer]] = bean45427.sr_12265_inst_list.asScala.toList.map(_.sr_12274_inst_list.asScala.toList.map(_.NDI))
            val RvFromBean: List[List[Integer]] = bean45427.sr_12265_inst_list.asScala.toList.map(_.sr_12274_inst_list.asScala.toList.map(_.RV))
            val TB_IndexFromBean: List[List[Integer]] = bean45427.sr_12265_inst_list.asScala.toList.map(_.sr_12274_inst_list.asScala.toList.map(_.TBIndex))
            val TB_SizeFromBean: List[List[Integer]] = bean45427.sr_12265_inst_list.asScala.toList.map(_.sr_12274_inst_list.asScala.toList.map(_.TBSize))
            val McsFromBean: List[List[Integer]] = bean45427.sr_12265_inst_list.asScala.toList.map(_.sr_12274_inst_list.asScala.toList.map(_.MCS))
            val ModulationTypeFromBean: List[String] = bean45427.sr_12265_inst_list.asScala.toList.map(_.sr_12274_inst_list.asScala.toList.map(_.ModulationType)).flatten
            val DidRecombiningFromBean: List[List[String]] = bean45427.sr_12265_inst_list.asScala.toList.map(_.sr_12274_inst_list.asScala.toList.map(_.DidRecombining))
            val SubFrameNumVal: List[Integer] = bean45427.sr_12265_inst_list.asScala.toList.map(_.SubframeNum)
            val prbs: List[List[Integer]] = bean45427.sr_12265_inst_list.asScala.toList.map(_.sr_12274_inst_list.asScala.toList.map(_.NumRBs))
            val TB_Size = mutable.MutableList[Integer]()
            val distServingCellIndex: Integer = bean45427.sr_12265_inst_list.asScala.toList.map(_.ServingCellIndex).distinct.size
            var firstSfn = 0
            var lastSfn = 0
            var firstFn = 0
            var lastFn = 0
            if(FrameNumFromBean.size > 0){
              firstFn = FrameNumFromBean(0)
              lastFn = FrameNumFromBean(FrameNumFromBean.size - 1)
            }
            if(SubFrameNumVal.size > 0){
              firstSfn = SubFrameNumVal(0)
              lastSfn = SubFrameNumVal(SubFrameNumVal.size - 1)
            }
            if (TB_SizeFromBean != null) {

              for (i <- 0 to TB_SizeFromBean.size - 1) {
                val TB_SizeTemp: List[Integer] = TB_SizeFromBean(i)
                var tbSizeSum: Integer = 0

                for (j <- 0 to TB_SizeTemp.size - 1) {
                  if(CrcResultFromBean(j).equalsIgnoreCase("pass")) {
                    tbSizeSum += TB_SizeTemp(j)
                  }
                  else{
                    tbSizeSum += 0
                  }
                }
                TB_Size += tbSizeSum
              }
            }
            var pcellTBSizeAggr = 0
            var scc1TBSizeAggr = 0
            var scc2TBSizeAggr = 0
            var scc3TBSizeAggr = 0
            var scc4TBSizeAggr = 0
            var scc5TBSizeAggr = 0
            var scc6TBSizeAggr = 0
            var scc7TBSizeAggr = 0

            for(i <- 0 to ServingCellIndexFromBean.size - 1){
              if(ServingCellIndexFromBean(i)!=null){
                ServingCellIndexFromBean(i).toLowerCase match {
                  case "pcell" =>
                    pcellTBSizeAggr += TB_Size(i)
                  case "scc1" =>
                    scc1TBSizeAggr += TB_Size(i)
                  case "scc2" =>
                    scc2TBSizeAggr += TB_Size(i)
                  case "scc3" =>
                    scc3TBSizeAggr += TB_Size(i)
                  case "scc4" =>
                    scc4TBSizeAggr += TB_Size(i)
                  case "scc5" =>
                    scc5TBSizeAggr += TB_Size(i)
                  case "scc6" =>
                    scc6TBSizeAggr += TB_Size(i)
                  case "scc7" =>
                    scc7TBSizeAggr += TB_Size(i)
                }
              }
            }

            logRecordQCommObj = logRecordQCommObj.copy( numRecords0xB173 = bean45427.NumRecords, frameNum = FrameNumFromBean, numLayers = NumLayersFromBean, numTransportBlocksPresent = NumTransportBlocksPresentFromBean,
              CrcResult = CrcResultFromBean, DiscardedreTxPresent = DiscardedreTxPresentFromBean, HarqID = HarqIDFromBean, NDI = NdiFromBean, RV = RvFromBean, TB_Index = TB_IndexFromBean,
              ServingCellIndex = ServingCellIndexFromBean, TB_Size0xB173 = TB_Size.toList, SubFrameNum = SubFrameNumVal, modulationType = ModulationTypeFromBean, numPRB0xB173 = prbs.flatten,
              firstFN_0xB173 = firstFn, lastFN_0xB173 = lastFn, firstSFN_0xB173 = firstSfn, lastSFN_0xB173 = lastSfn, pcellTBSize_0xB173 = pcellTBSizeAggr, scc1TBSize_0xB173 = scc1TBSizeAggr,
              scc2TBSize_0xB173 = scc2TBSizeAggr,  scc3TBSize_0xB173 = scc3TBSizeAggr,  scc4TBSize_0xB173 = scc4TBSizeAggr,  scc5TBSize_0xB173 = scc5TBSizeAggr,  scc6TBSize_0xB173 = scc6TBSizeAggr,
              scc7TBSize_0xB173 = scc7TBSizeAggr, distServingCellIndex_0xB173 = distServingCellIndex
            )
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45427", hexLogCode = "0xB173", logRecordQComm = logRecordQCommObj)
          case 37 => val bean45427 = process45427_37(x, exceptionUseArray)
            //logger.info("0xB173 json:" + Constants.getJsonFromBean(bean45427))
            //logger.info("0xB173 FrameNum:" + bean45427.sr_89874_inst_list.asScala.toList.map(_.FrameNum).toString)
            val FrameNumFromBean: List[Integer] = bean45427.sr_89874_inst_list.asScala.toList.map(_.FrameNum.toInt.asInstanceOf[Integer])
            val NumLayersFromBean: List[Integer] = bean45427.sr_89874_inst_list.asScala.toList.map(_.NumLayers.toInt.asInstanceOf[Integer])
            val NumTransportBlocksPresentFromBean: List[Integer] = bean45427.sr_89874_inst_list.asScala.toList.map(_.NumTransportBlocksPresent.toInt.asInstanceOf[Integer])
            val ServingCellIndexFromBean: List[String] = bean45427.sr_89874_inst_list.asScala.toList.map(_.ServingCellIndex)
            val CrcResultFromBean: List[String] = bean45427.sr_89874_inst_list.asScala.toList.map(_.sr_89883_inst_list.asScala.toList.map(_.CRCResult)).flatten
            val DiscardedreTxPresentFromBean: List[List[String]] = bean45427.sr_89874_inst_list.asScala.toList.map(_.sr_89883_inst_list.asScala.toList.map(_.DiscardedReTx))
            val HarqIDFromBean: List[List[Integer]] = bean45427.sr_89874_inst_list.asScala.toList.map(_.sr_89883_inst_list.asScala.toList.map(_.HARQID.toInt.asInstanceOf[Integer]))
            val NdiFromBean: List[List[Integer]] = bean45427.sr_89874_inst_list.asScala.toList.map(_.sr_89883_inst_list.asScala.toList.map(_.NDI.toInt.asInstanceOf[Integer]))
            val RvFromBean: List[List[Integer]] = bean45427.sr_89874_inst_list.asScala.toList.map(_.sr_89883_inst_list.asScala.toList.map(_.RV.toInt.asInstanceOf[Integer]))
            val TB_IndexFromBean: List[List[Integer]] = bean45427.sr_89874_inst_list.asScala.toList.map(_.sr_89883_inst_list.asScala.toList.map(_.TBIndex.toInt.asInstanceOf[Integer]))
            val TB_SizeFromBean: List[List[Integer]] = bean45427.sr_89874_inst_list.asScala.toList.map(_.sr_89883_inst_list.asScala.toList.map(_.TBSize.toInt.asInstanceOf[Integer]))
            val McsFromBean: List[List[String]] = bean45427.sr_89874_inst_list.asScala.toList.map(_.sr_89883_inst_list.asScala.toList.map(_.MCS))
            val ModulationTypeFromBean: List[String] = bean45427.sr_89874_inst_list.asScala.toList.map(_.sr_89883_inst_list.asScala.toList.map(_.ModulationType)).flatten
            val DidRecombiningFromBean: List[List[String]] = bean45427.sr_89874_inst_list.asScala.toList.map(_.sr_89883_inst_list.asScala.toList.map(_.DidRecombining))
            val SubFrameNumVal: List[Integer] = bean45427.sr_89874_inst_list.asScala.toList.map(_.SubframeNum.toInt.asInstanceOf[Integer])
            val prbs: List[List[Integer]] = bean45427.sr_89874_inst_list.asScala.toList.map(_.sr_89883_inst_list.asScala.toList.map(_.NumRBs.toInt.asInstanceOf[Integer]))
            val TB_Size = mutable.MutableList[Integer]()
            val distServingCellIndex: Integer = bean45427.sr_89874_inst_list.asScala.toList.map(_.ServingCellIndex).distinct.size
            var firstSfn = 0
            var lastSfn = 0
            var firstFn = 0
            var lastFn = 0
            if(FrameNumFromBean.size > 0){
              firstFn = FrameNumFromBean(0)
              lastFn = FrameNumFromBean(FrameNumFromBean.size - 1)
            }
            if(SubFrameNumVal.size > 0){
              firstSfn = SubFrameNumVal(0)
              lastSfn = SubFrameNumVal(SubFrameNumVal.size - 1)
            }
            if (TB_SizeFromBean != null) {

              for (i <- 0 to TB_SizeFromBean.size - 1) {
                val TB_SizeTemp: List[Integer] = TB_SizeFromBean(i)
                var tbSizeSum: Integer = 0

                for (j <- 0 to TB_SizeTemp.size - 1) {
                  if(CrcResultFromBean(j).equalsIgnoreCase("pass")) {
                    tbSizeSum += TB_SizeTemp(j)
                  }
                  else{
                    tbSizeSum += 0
                  }
                }
                TB_Size += tbSizeSum
              }
            }
            var pcellTBSizeAggr = 0
            var scc1TBSizeAggr = 0
            var scc2TBSizeAggr = 0
            var scc3TBSizeAggr = 0
            var scc4TBSizeAggr = 0
            var scc5TBSizeAggr = 0
            var scc6TBSizeAggr = 0
            var scc7TBSizeAggr = 0

            for(i <- 0 to ServingCellIndexFromBean.size - 1){
              if(ServingCellIndexFromBean(i)!=null){
                ServingCellIndexFromBean(i).toLowerCase match {
                  case "pcell" =>
                    pcellTBSizeAggr += TB_Size(i)
                  case "scc1" =>
                    scc1TBSizeAggr += TB_Size(i)
                  case "scc2" =>
                    scc2TBSizeAggr += TB_Size(i)
                  case "scc3" =>
                    scc3TBSizeAggr += TB_Size(i)
                  case "scc4" =>
                    scc4TBSizeAggr += TB_Size(i)
                  case "scc5" =>
                    scc5TBSizeAggr += TB_Size(i)
                  case "scc6" =>
                    scc6TBSizeAggr += TB_Size(i)
                  case "scc7" =>
                    scc7TBSizeAggr += TB_Size(i)
                }
              }
            }

            logRecordQCommObj = logRecordQCommObj.copy(
              numRecords0xB173 = bean45427.NumRecords.toInt,
              frameNum = FrameNumFromBean,
              numLayers = NumLayersFromBean,
              numTransportBlocksPresent = NumTransportBlocksPresentFromBean,
              CrcResult = CrcResultFromBean,
              DiscardedreTxPresent = DiscardedreTxPresentFromBean,
              HarqID = HarqIDFromBean,
              NDI = NdiFromBean,
              RV = RvFromBean,
              TB_Index = TB_IndexFromBean,
              ServingCellIndex = ServingCellIndexFromBean,
              TB_Size0xB173 = TB_Size.toList,
              SubFrameNum = SubFrameNumVal,
              modulationType = ModulationTypeFromBean,
              numPRB0xB173 = prbs.flatten,
              firstFN_0xB173 = firstFn, lastFN_0xB173 = lastFn, firstSFN_0xB173 = firstSfn, lastSFN_0xB173 = lastSfn, pcellTBSize_0xB173 = pcellTBSizeAggr, scc1TBSize_0xB173 = scc1TBSizeAggr,
              scc2TBSize_0xB173 = scc2TBSizeAggr,  scc3TBSize_0xB173 = scc3TBSizeAggr,  scc4TBSize_0xB173 = scc4TBSizeAggr,  scc5TBSize_0xB173 = scc5TBSizeAggr,  scc6TBSize_0xB173 = scc6TBSizeAggr,
              scc7TBSize_0xB173 = scc7TBSizeAggr, distServingCellIndex_0xB173 = distServingCellIndex
            )
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45427", hexLogCode = "0xB173", logRecordQComm = logRecordQCommObj)
          case 39 => val bean45427 = process45427_39(x, exceptionUseArray)
            //logger.info("0xB173 json:" + Constants.getJsonFromBean(bean45427))
            //logger.info("0xB173 FrameNum:" + bean45427.sr_89874_inst_list.asScala.toList.map(_.FrameNum).toString)
            val FrameNumFromBean: List[Integer] = bean45427.sr_104389_inst_list.asScala.toList.map(_.FrameNum.toInt.asInstanceOf[Integer])
            val NumLayersFromBean: List[Integer] = bean45427.sr_104389_inst_list.asScala.toList.map(_.NumLayers.toInt.asInstanceOf[Integer])
            val NumTransportBlocksPresentFromBean: List[Integer] = bean45427.sr_104389_inst_list.asScala.toList.map(_.NumTransportBlocksPresent.toInt.asInstanceOf[Integer])
            val ServingCellIndexFromBean: List[String] = bean45427.sr_104389_inst_list.asScala.toList.map(_.ServingCellIndex)
            val CrcResultFromBean: List[String] = bean45427.sr_104389_inst_list.asScala.toList.map(_.sr_104377_inst_list.asScala.toList.map(_.CRCResult)).flatten
            val DiscardedreTxPresentFromBean: List[List[String]] = bean45427.sr_104389_inst_list.asScala.toList.map(_.sr_104377_inst_list.asScala.toList.map(_.DiscardedReTx))
            val HarqIDFromBean: List[List[Integer]] = bean45427.sr_104389_inst_list.asScala.toList.map(_.sr_104377_inst_list.asScala.toList.map(_.HARQID.toInt.asInstanceOf[Integer]))
            val NdiFromBean: List[List[Integer]] = bean45427.sr_104389_inst_list.asScala.toList.map(_.sr_104377_inst_list.asScala.toList.map(_.NDI.toInt.asInstanceOf[Integer]))
            val RvFromBean: List[List[Integer]] = bean45427.sr_104389_inst_list.asScala.toList.map(_.sr_104377_inst_list.asScala.toList.map(_.RV.toInt.asInstanceOf[Integer]))
            val TB_IndexFromBean: List[List[Integer]] = bean45427.sr_104389_inst_list.asScala.toList.map(_.sr_104377_inst_list.asScala.toList.map(_.TBIndex.toInt.asInstanceOf[Integer]))
            val TB_SizeFromBean: List[List[Integer]] = bean45427.sr_104389_inst_list.asScala.toList.map(_.sr_104377_inst_list.asScala.toList.map(_.TBSize.toInt.asInstanceOf[Integer]))
            val McsFromBean: List[List[String]] = bean45427.sr_104389_inst_list.asScala.toList.map(_.sr_104377_inst_list.asScala.toList.map(_.MCS))
            val ModulationTypeFromBean: List[String] = bean45427.sr_104389_inst_list.asScala.toList.map(_.sr_104377_inst_list.asScala.toList.map(_.ModulationType)).flatten
            val DidRecombiningFromBean: List[List[String]] = bean45427.sr_104389_inst_list.asScala.toList.map(_.sr_104377_inst_list.asScala.toList.map(_.DidRecombining))
            val SubFrameNumVal: List[Integer] = bean45427.sr_104389_inst_list.asScala.toList.map(_.SubframeNum.toInt.asInstanceOf[Integer])
            val prbs: List[List[Integer]] = bean45427.sr_104389_inst_list.asScala.toList.map(_.sr_104377_inst_list.asScala.toList.map(_.NumRBs.toInt.asInstanceOf[Integer]))
            val TB_Size = mutable.MutableList[Integer]()
            val distServingCellIndex: Integer = bean45427.sr_104389_inst_list.asScala.toList.map(_.ServingCellIndex).distinct.size
            var firstSfn = 0
            var lastSfn = 0
            var firstFn = 0
            var lastFn = 0
            if(FrameNumFromBean.size > 0){
              firstFn = FrameNumFromBean(0)
              lastFn = FrameNumFromBean(FrameNumFromBean.size - 1)
            }
            if(SubFrameNumVal.size > 0){
              firstSfn = SubFrameNumVal(0)
              lastSfn = SubFrameNumVal(SubFrameNumVal.size - 1)
            }
            if (TB_SizeFromBean != null) {

              for (i <- 0 to TB_SizeFromBean.size - 1) {
                val TB_SizeTemp: List[Integer] = TB_SizeFromBean(i)
                var tbSizeSum: Integer = 0

                for (j <- 0 to TB_SizeTemp.size - 1) {
                  if(CrcResultFromBean(j).equalsIgnoreCase("pass")) {
                    tbSizeSum += TB_SizeTemp(j)
                  }
                  else{
                    tbSizeSum += 0
                  }
                }
                TB_Size += tbSizeSum
              }
            }
            var pcellTBSizeAggr = 0
            var scc1TBSizeAggr = 0
            var scc2TBSizeAggr = 0
            var scc3TBSizeAggr = 0
            var scc4TBSizeAggr = 0
            var scc5TBSizeAggr = 0
            var scc6TBSizeAggr = 0
            var scc7TBSizeAggr = 0

            for(i <- 0 to ServingCellIndexFromBean.size - 1){
              if(ServingCellIndexFromBean(i)!=null){
                ServingCellIndexFromBean(i).toLowerCase match {
                  case "pcell" =>
                    pcellTBSizeAggr += TB_Size(i)
                  case "scc1" =>
                    scc1TBSizeAggr += TB_Size(i)
                  case "scc2" =>
                    scc2TBSizeAggr += TB_Size(i)
                  case "scc3" =>
                    scc3TBSizeAggr += TB_Size(i)
                  case "scc4" =>
                    scc4TBSizeAggr += TB_Size(i)
                  case "scc5" =>
                    scc5TBSizeAggr += TB_Size(i)
                  case "scc6" =>
                    scc6TBSizeAggr += TB_Size(i)
                  case "scc7" =>
                    scc7TBSizeAggr += TB_Size(i)
                }
              }
            }

            logRecordQCommObj = logRecordQCommObj.copy(
              numRecords0xB173 = bean45427.NumRecords.toInt,
              frameNum = FrameNumFromBean,
              numLayers = NumLayersFromBean,
              numTransportBlocksPresent = NumTransportBlocksPresentFromBean,
              CrcResult = CrcResultFromBean,
              DiscardedreTxPresent = DiscardedreTxPresentFromBean,
              HarqID = HarqIDFromBean,
              NDI = NdiFromBean,
              RV = RvFromBean,
              TB_Index = TB_IndexFromBean,
              ServingCellIndex = ServingCellIndexFromBean,
              TB_Size0xB173 = TB_Size.toList,
              SubFrameNum = SubFrameNumVal,
              modulationType = ModulationTypeFromBean,
              numPRB0xB173 = prbs.flatten,
              firstFN_0xB173 = firstFn, lastFN_0xB173 = lastFn, firstSFN_0xB173 = firstSfn, lastSFN_0xB173 = lastSfn, pcellTBSize_0xB173 = pcellTBSizeAggr, scc1TBSize_0xB173 = scc1TBSizeAggr,
              scc2TBSize_0xB173 = scc2TBSizeAggr,  scc3TBSize_0xB173 = scc3TBSizeAggr,  scc4TBSize_0xB173 = scc4TBSizeAggr,  scc5TBSize_0xB173 = scc5TBSizeAggr,  scc6TBSize_0xB173 = scc6TBSizeAggr,
              scc7TBSize_0xB173 = scc7TBSizeAggr, distServingCellIndex_0xB173 = distServingCellIndex
            )
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45427", hexLogCode = "0xB173", logRecordQComm = logRecordQCommObj)
          case 40 => val bean45427 = process45427_40(x, exceptionUseArray)
            val FrameNumFromBean: List[Integer] = bean45427.sr_88411_inst_list.asScala.toList.map(_.FrameNum.toInt.asInstanceOf[Integer])
            val NumLayersFromBean: List[Integer] = bean45427.sr_88411_inst_list.asScala.toList.map(_.NumLayers.toInt.asInstanceOf[Integer])
            val NumTransportBlocksPresentFromBean: List[Integer] = bean45427.sr_88411_inst_list.asScala.toList.map(_.NumTransportBlocksPresent.toInt.asInstanceOf[Integer])
            val ServingCellIndexFromBean: List[String] = bean45427.sr_88411_inst_list.asScala.toList.map(_.ServingCellIndex)
            val CrcResultFromBean: List[String] = bean45427.sr_88411_inst_list.asScala.toList.map(_.sr_88420_inst_list.asScala.toList.map(_.CRCResult)).flatten
            val DiscardedreTxPresentFromBean: List[List[String]] = bean45427.sr_88411_inst_list.asScala.toList.map(_.sr_88420_inst_list.asScala.toList.map(_.DiscardedReTx))
            val HarqIDFromBean: List[List[Integer]] = bean45427.sr_88411_inst_list.asScala.toList.map(_.sr_88420_inst_list.asScala.toList.map(_.HARQID.toInt.asInstanceOf[Integer]))
            val NdiFromBean: List[List[Integer]] = bean45427.sr_88411_inst_list.asScala.toList.map(_.sr_88420_inst_list.asScala.toList.map(_.NDI.toInt.asInstanceOf[Integer]))
            val RvFromBean: List[List[Integer]] = bean45427.sr_88411_inst_list.asScala.toList.map(_.sr_88420_inst_list.asScala.toList.map(_.RV.toInt.asInstanceOf[Integer]))
            val TB_IndexFromBean: List[List[Integer]] = bean45427.sr_88411_inst_list.asScala.toList.map(_.sr_88420_inst_list.asScala.toList.map(_.TBIndex.toInt.asInstanceOf[Integer]))
            val TB_SizeFromBean: List[List[Integer]] = bean45427.sr_88411_inst_list.asScala.toList.map(_.sr_88420_inst_list.asScala.toList.map(_.TBSize.toInt.asInstanceOf[Integer]))
            val McsFromBean: List[List[String]] = bean45427.sr_88411_inst_list.asScala.toList.map(_.sr_88420_inst_list.asScala.toList.map(_.MCS))
            val ModulationTypeFromBean: List[String] = bean45427.sr_88411_inst_list.asScala.toList.map(_.sr_88420_inst_list.asScala.toList.map(_.ModulationType)).flatten
            val DidRecombiningFromBean: List[List[String]] = bean45427.sr_88411_inst_list.asScala.toList.map(_.sr_88420_inst_list.asScala.toList.map(_.DidRecombining))
            val SubFrameNumVal: List[Integer] = bean45427.sr_88411_inst_list.asScala.toList.map(_.SubframeNum.toInt.asInstanceOf[Integer])
            val prbs: List[List[Integer]] = bean45427.sr_88411_inst_list.asScala.toList.map(_.sr_88420_inst_list.asScala.toList.map(_.NumRBs.toInt.asInstanceOf[Integer]))
            val TB_Size = mutable.MutableList[Integer]()
            val distServingCellIndex: Integer = bean45427.sr_88411_inst_list.asScala.toList.map(_.ServingCellIndex).distinct.size
            var firstSfn = 0
            var lastSfn = 0
            var firstFn = 0
            var lastFn = 0
            if(FrameNumFromBean.size > 0){
              firstFn = FrameNumFromBean(0)
              lastFn = FrameNumFromBean(FrameNumFromBean.size - 1)
            }
            if(SubFrameNumVal.size > 0){
              firstSfn = SubFrameNumVal(0)
              lastSfn = SubFrameNumVal(SubFrameNumVal.size - 1)
            }
            if (TB_SizeFromBean != null) {

              for (i <- 0 to TB_SizeFromBean.size - 1) {
                val TB_SizeTemp: List[Integer] = TB_SizeFromBean(i)
                var tbSizeSum: Integer = 0

                for (j <- 0 to TB_SizeTemp.size - 1) {
                  if(CrcResultFromBean(j).equalsIgnoreCase("pass")) {
                    tbSizeSum += TB_SizeTemp(j)
                  }
                  else{
                    tbSizeSum += 0
                  }
                }
                TB_Size += tbSizeSum
              }
            }
            var pcellTBSizeAggr = 0
            var scc1TBSizeAggr = 0
            var scc2TBSizeAggr = 0
            var scc3TBSizeAggr = 0
            var scc4TBSizeAggr = 0
            var scc5TBSizeAggr = 0
            var scc6TBSizeAggr = 0
            var scc7TBSizeAggr = 0

            for(i <- 0 to ServingCellIndexFromBean.size - 1){
              if(ServingCellIndexFromBean(i)!=null){
                ServingCellIndexFromBean(i).toLowerCase match {
                  case "pcell" =>
                    pcellTBSizeAggr += TB_Size(i)
                  case "scc1" =>
                    scc1TBSizeAggr += TB_Size(i)
                  case "scc2" =>
                    scc2TBSizeAggr += TB_Size(i)
                  case "scc3" =>
                    scc3TBSizeAggr += TB_Size(i)
                  case "scc4" =>
                    scc4TBSizeAggr += TB_Size(i)
                  case "scc5" =>
                    scc5TBSizeAggr += TB_Size(i)
                  case "scc6" =>
                    scc6TBSizeAggr += TB_Size(i)
                  case "scc7" =>
                    scc7TBSizeAggr += TB_Size(i)
                }
              }
            }

            logRecordQCommObj = logRecordQCommObj.copy(
              numRecords0xB173 = bean45427.NumRecords.toInt,
              frameNum = FrameNumFromBean,
              numLayers = NumLayersFromBean,
              numTransportBlocksPresent = NumTransportBlocksPresentFromBean,
              CrcResult = CrcResultFromBean,
              DiscardedreTxPresent = DiscardedreTxPresentFromBean,
              HarqID = HarqIDFromBean,
              NDI = NdiFromBean,
              RV = RvFromBean,
              TB_Index = TB_IndexFromBean,
              ServingCellIndex = ServingCellIndexFromBean,
              TB_Size0xB173 = TB_Size.toList,
              SubFrameNum = SubFrameNumVal,
              modulationType = ModulationTypeFromBean,
              numPRB0xB173 = prbs.flatten,
              firstFN_0xB173 = firstFn, lastFN_0xB173 = lastFn, firstSFN_0xB173 = firstSfn, lastSFN_0xB173 = lastSfn, pcellTBSize_0xB173 = pcellTBSizeAggr, scc1TBSize_0xB173 = scc1TBSizeAggr,
              scc2TBSize_0xB173 = scc2TBSizeAggr,  scc3TBSize_0xB173 = scc3TBSizeAggr,  scc4TBSize_0xB173 = scc4TBSizeAggr,  scc5TBSize_0xB173 = scc5TBSizeAggr,  scc6TBSize_0xB173 = scc6TBSizeAggr,
              scc7TBSize_0xB173 = scc7TBSizeAggr, distServingCellIndex_0xB173 = distServingCellIndex
            )
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45427", hexLogCode = "0xB173", logRecordQComm = logRecordQCommObj)

          case 48 => val bean45427 = process45427_48(x, exceptionUseArray)
            val FrameNumFromBean: List[Integer] = bean45427.sr_117974_inst_list.asScala.toList.map(_.FrameNum.toInt.asInstanceOf[Integer])
            val NumLayersFromBean: List[Integer] = bean45427.sr_117974_inst_list.asScala.toList.map(_.NumLayers.toInt.asInstanceOf[Integer])
            val NumTransportBlocksPresentFromBean: List[Integer] = bean45427.sr_117974_inst_list.asScala.toList.map(_.NumTransportBlocksPresent.toInt.asInstanceOf[Integer])
            val ServingCellIndexFromBean: List[String] = bean45427.sr_117974_inst_list.asScala.toList.map(_.ServingCellIndex)
            val CrcResultFromBean: List[String] = bean45427.sr_117974_inst_list.asScala.toList.map(_.sr_117988_inst_list.asScala.toList.map(_.CRCResult)).flatten
            val DiscardedreTxPresentFromBean: List[List[String]] = bean45427.sr_117974_inst_list.asScala.toList.map(_.sr_117988_inst_list.asScala.toList.map(_.DiscardedReTx))
            val HarqIDFromBean: List[List[Integer]] = bean45427.sr_117974_inst_list.asScala.toList.map(_.sr_117988_inst_list.asScala.toList.map(_.HARQID.toInt.asInstanceOf[Integer]))
            val NdiFromBean: List[List[Integer]] = bean45427.sr_117974_inst_list.asScala.toList.map(_.sr_117988_inst_list.asScala.toList.map(_.NDI.toInt.asInstanceOf[Integer]))
            val RvFromBean: List[List[Integer]] = bean45427.sr_117974_inst_list.asScala.toList.map(_.sr_117988_inst_list.asScala.toList.map(_.RV.toInt.asInstanceOf[Integer]))
            val TB_IndexFromBean: List[List[Integer]] = bean45427.sr_117974_inst_list.asScala.toList.map(_.sr_117988_inst_list.asScala.toList.map(_.TBIndex.toInt.asInstanceOf[Integer]))
            val TB_SizeFromBean: List[List[Integer]] = bean45427.sr_117974_inst_list.asScala.toList.map(_.sr_117988_inst_list.asScala.toList.map(_.TBSize.toInt.asInstanceOf[Integer]))
            val McsFromBean: List[List[String]] = bean45427.sr_117974_inst_list.asScala.toList.map(_.sr_117988_inst_list.asScala.toList.map(_.MCS))
            val ModulationTypeFromBean: List[String] = bean45427.sr_117974_inst_list.asScala.toList.map(_.sr_117988_inst_list.asScala.toList.map(_.ModulationType)).flatten
            val DidRecombiningFromBean: List[List[String]] = bean45427.sr_117974_inst_list.asScala.toList.map(_.sr_117988_inst_list.asScala.toList.map(_.DidRecombining))
            val SubFrameNumVal: List[Integer] = bean45427.sr_117974_inst_list.asScala.toList.map(_.SubframeNum.toInt.asInstanceOf[Integer])
            val prbs: List[List[Integer]] = bean45427.sr_117974_inst_list.asScala.toList.map(_.sr_117988_inst_list.asScala.toList.map(_.NumRBs.toInt.asInstanceOf[Integer]))
            val TB_Size = mutable.MutableList[Integer]()
            val distServingCellIndex: Integer = bean45427.sr_117974_inst_list.asScala.toList.map(_.ServingCellIndex).distinct.size
            var firstSfn = 0
            var lastSfn = 0
            var firstFn = 0
            var lastFn = 0
            if(FrameNumFromBean.size > 0){
              firstFn = FrameNumFromBean(0)
              lastFn = FrameNumFromBean(FrameNumFromBean.size - 1)
            }
            if(SubFrameNumVal.size > 0){
              firstSfn = SubFrameNumVal(0)
              lastSfn = SubFrameNumVal(SubFrameNumVal.size - 1)
            }
            if (TB_SizeFromBean != null) {

              for (i <- 0 to TB_SizeFromBean.size - 1) {
                val TB_SizeTemp: List[Integer] = TB_SizeFromBean(i)
                var tbSizeSum: Integer = 0

                for (j <- 0 to TB_SizeTemp.size - 1) {
                  if(CrcResultFromBean(j).equalsIgnoreCase("pass")) {
                    tbSizeSum += TB_SizeTemp(j)
                  }
                  else{
                    tbSizeSum += 0
                  }
                }
                TB_Size += tbSizeSum
              }
            }
            var pcellTBSizeAggr = 0
            var scc1TBSizeAggr = 0
            var scc2TBSizeAggr = 0
            var scc3TBSizeAggr = 0
            var scc4TBSizeAggr = 0
            var scc5TBSizeAggr = 0
            var scc6TBSizeAggr = 0
            var scc7TBSizeAggr = 0

            for(i <- 0 to ServingCellIndexFromBean.size - 1){
              if(ServingCellIndexFromBean(i)!=null){
                ServingCellIndexFromBean(i).toLowerCase match {
                  case "pcell" =>
                    pcellTBSizeAggr += TB_Size(i)
                  case "scc1" =>
                    scc1TBSizeAggr += TB_Size(i)
                  case "scc2" =>
                    scc2TBSizeAggr += TB_Size(i)
                  case "scc3" =>
                    scc3TBSizeAggr += TB_Size(i)
                  case "scc4" =>
                    scc4TBSizeAggr += TB_Size(i)
                  case "scc5" =>
                    scc5TBSizeAggr += TB_Size(i)
                  case "scc6" =>
                    scc6TBSizeAggr += TB_Size(i)
                  case "scc7" =>
                    scc7TBSizeAggr += TB_Size(i)
                }
              }
            }

            logRecordQCommObj = logRecordQCommObj.copy(
              numRecords0xB173 = bean45427.NumRecords.toInt,
              frameNum = FrameNumFromBean,
              numLayers = NumLayersFromBean,
              numTransportBlocksPresent = NumTransportBlocksPresentFromBean,
              CrcResult = CrcResultFromBean,
              DiscardedreTxPresent = DiscardedreTxPresentFromBean,
              HarqID = HarqIDFromBean,
              NDI = NdiFromBean,
              RV = RvFromBean,
              TB_Index = TB_IndexFromBean,
              ServingCellIndex = ServingCellIndexFromBean,
              TB_Size0xB173 = TB_Size.toList,
              SubFrameNum = SubFrameNumVal,
              modulationType = ModulationTypeFromBean,
              numPRB0xB173 = prbs.flatten,
              firstFN_0xB173 = firstFn, lastFN_0xB173 = lastFn, firstSFN_0xB173 = firstSfn, lastSFN_0xB173 = lastSfn, pcellTBSize_0xB173 = pcellTBSizeAggr, scc1TBSize_0xB173 = scc1TBSizeAggr,
              scc2TBSize_0xB173 = scc2TBSizeAggr,  scc3TBSize_0xB173 = scc3TBSizeAggr,  scc4TBSize_0xB173 = scc4TBSizeAggr,  scc5TBSize_0xB173 = scc5TBSizeAggr,  scc6TBSize_0xB173 = scc6TBSizeAggr,
              scc7TBSize_0xB173 = scc7TBSizeAggr, distServingCellIndex_0xB173 = distServingCellIndex
            )
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45427", hexLogCode = "0xB173", logRecordQComm = logRecordQCommObj)

          case _ =>
            //logger.info("Entered into 45427, 0xB173 DEFAULT CASE with version : " + logVersion)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45427", hexLogCode = "0xB173", missingVersion = logVersion)
        }


      case 45425 =>
        //logger.info("0xB171 version:"+logVersion);
        logVersion match {
          case 3 => val bean45425 = process45425_3(x, exceptionUseArray)
            //logger.info("0xB171 json:"+Constants.getJsonFromBean(bean45425));
            val srsTxPwrVal: List[Integer] = bean45425.sr_11875_inst_list.asScala.toList.map(_.SRSTxPower)
            logRecordQCommObj = logRecordQCommObj.copy(srsTxPower = srsTxPwrVal)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45425",
              hexLogCode = "0xB171", logRecordQComm = logRecordQCommObj)
          case 24 => val bean45425 = process45425_24(x, exceptionUseArray)
            //logger.info("0xB171 json:"+Constants.getJsonFromBean(bean45425));
            var srsTxPwrVal: List[String] = bean45425.sr_11886_inst_list.asScala.toList.map(_.SRSTxPower)
            srsTxPwrVal = srsTxPwrVal.map(
              out => if(Constants.isValidNumeric(out, Constants.CONST_NUMBER)) out.toString
            ).collect {case n:String => n}
            //val srsTxPwrVal: List[Integer] = bean45425.sr_11886_inst_list.asScala.toList.map(_.SRSTxPower.asInstanceOf[Integer])
            logRecordQCommObj = logRecordQCommObj.copy(srsTxPower = srsTxPwrVal.map(_.toInt.asInstanceOf[Integer]))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45425",
              hexLogCode = "0xB171", logRecordQComm = logRecordQCommObj)
          case _ =>
            //logger.info("Entered into Default Case for 45425(0xB171) for Version: " + logVersion)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45425", hexLogCode = "0xB171", missingVersion = logVersion)
        }
    }
    logRecord
  }

  def parseLogRecordQCOMM_Set7(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQCommObj:LogRecordQComm = LogRecordQComm()
    logCode match {
      case 45350 =>
        logVersion match {
          case 23 => val bean45350 = process45350_23(x, exceptionUseArray)
            logRecordQCommObj = logRecordQCommObj.copy(countLogcodeB126 = 1, modStream0 = List(bean45350.ModulationStream0), modStream1 = List(bean45350.ModulationStream1),
              freqSelPMI = List(bean45350.FrequencySelectivePMI), pmiIndex = bean45350.PMIIndex, spatialRank = List(bean45350.SpatialRank), carrierIndex0xB126 = bean45350.CarrierIndex,
              pDSCHRNTIlID = bean45350.PDSCHRNTIlID, pDSCHRNTIType = List(bean45350.PDSCHRNTIType), servingCellID = bean45350.ServingCellID,
              transmissionScheme = List(bean45350.TransmissionScheme), numTxAntennas = List(bean45350.NumberofTxAntennasM), numRxAntennas = List(bean45350.NumberofRxAntennasN))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45350",
              hexLogCode = "0xB126", logRecordQComm = logRecordQCommObj)
          case 103 => val bean45350 = process45350_103(x, exceptionUseArray)
            logRecordQCommObj = logRecordQCommObj.copy(countLogcodeB126 = 1, modStream0 = List(bean45350.ModulationStream0), modStream1 = List(bean45350.ModulationStream1),
              freqSelPMI = List(bean45350.FrequencySelectivePMI), pmiIndex = bean45350.PMIIndex, spatialRank = List(bean45350.SpatialRank), carrierIndex0xB126 = bean45350.CarrierIndex,
              pDSCHRNTIlID = bean45350.PDSCHRNTIlID, pDSCHRNTIType = List(bean45350.PDSCHRNTIType), servingCellID = bean45350.ServingCellID,
              transmissionScheme = List(bean45350.TransmissionScheme), numTxAntennas = List(bean45350.NumberofTxAntennasM), numRxAntennas = List(bean45350.NumberofRxAntennasN))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45350",
              hexLogCode = "0xB126", logRecordQComm = logRecordQCommObj)
          case 104 => val bean45350 = process45350_104(x, exceptionUseArray)

            logRecordQCommObj = logRecordQCommObj.copy(countLogcodeB126 = 1, modStream0 = List(bean45350.ModulationStream0), modStream1 = List(bean45350.ModulationStream1),
              freqSelPMI = List(bean45350.FrequencySelectivePMI), pmiIndex = bean45350.PMIIndex, spatialRank = List(bean45350.SpatialRank), carrierIndex0xB126 = bean45350.CarrierIndex,
              pDSCHRNTIlID = bean45350.PDSCHRNTIlID, pDSCHRNTIType = List(bean45350.PDSCHRNTIType), servingCellID = bean45350.ServingCellID,
              transmissionScheme = List(bean45350.TransmissionScheme), numTxAntennas = List(bean45350.NumberofTxAntennasM), numRxAntennas = List(bean45350.NumberofRxAntennasN))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45350",
              hexLogCode = "0xB126", logRecordQComm = logRecordQCommObj)
          case 122 => val bean45350 = process45350_122(x, exceptionUseArray)
            logRecordQCommObj = logRecordQCommObj.copy(countLogcodeB126 = 1, modStream0 = List(bean45350.ModulationStream0), modStream1 = List(bean45350.ModulationStream1),
              freqSelPMI = List(bean45350.FrequencySelectivePMI), pmiIndex = bean45350.PMIIndex, spatialRank = List(bean45350.SpatialRank), carrierIndex0xB126 = bean45350.CarrierIndex,
              pDSCHRNTIlID = bean45350.PDSCHRNTIlID, pDSCHRNTIType = List(bean45350.PDSCHRNTIType), servingCellID = bean45350.ServingCellID,
              transmissionScheme = List(bean45350.TransmissionScheme), numTxAntennas = List(bean45350.NumberofTxAntennasM), numRxAntennas = List(bean45350.NumberofRxAntennasN))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45350",
              hexLogCode = "0xB126", logRecordQComm = logRecordQCommObj)
          case 123 => val bean45350 = process45350_123(x, exceptionUseArray)
            logRecordQCommObj = logRecordQCommObj.copy(countLogcodeB126 = 1, modStream0 = List(bean45350.ModulationStream0), modStream1 = List(bean45350.ModulationStream1),
              freqSelPMI = List(bean45350.FrequencySelectivePMI), spatialRank = List(bean45350.SpatialRank), carrierIndex0xB126 = bean45350.CarrierIndex,
              pDSCHRNTIType = List(bean45350.PDSCHRNTIType), transmissionScheme = List(bean45350.TransmissionScheme), numTxAntennas = List(bean45350.NumberofTxAntennasM),
              numRxAntennas = List(bean45350.NumberofRxAntennasN))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45350",
              hexLogCode = "0xB126", logRecordQComm = logRecordQCommObj)
          case 124 => val bean45350 = process45350_124(x, exceptionUseArray)
            logRecordQCommObj = logRecordQCommObj.copy(countLogcodeB126 = 1, modStream0 = List(bean45350.ModulationStream0), modStream1 = List(bean45350.ModulationStream1),
              freqSelPMI = List(bean45350.FrequencySelectivePMI), spatialRank = List(bean45350.SpatialRank), carrierIndex0xB126 = bean45350.CarrierIndex,
              pDSCHRNTIType = List(bean45350.PDSCHRNTIType), transmissionScheme = List(bean45350.TransmissionScheme), numTxAntennas = List(bean45350.NumberofTxAntennasM),
              numRxAntennas = List(bean45350.NumberofRxAntennasN))
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45350",
              hexLogCode = "0xB126", logRecordQComm = logRecordQCommObj)
          case 144 => val bean45350 = process45350_144(x, exceptionUseArray)
            val ModulationStream0: List[String] = bean45350.sr_88556_inst_list.asScala.toList.map(_.ModulationStream0)
            val ModulationStream1: List[String] = bean45350.sr_88556_inst_list.asScala.toList.map(_.ModulationStream1)
            val FrequencySelectivePMI: List[String] = bean45350.sr_88556_inst_list.asScala.toList.map(_.FrequencySelectivePMI)
            val SpatialRank: List[String] = bean45350.sr_88556_inst_list.asScala.toList.map(_.SpatialRank)
            val PDSCHRNTIType: List[String] = bean45350.sr_88556_inst_list.asScala.toList.map(_.PDSCHRNTIType)
            val TransmissionScheme: List[String] = bean45350.sr_88556_inst_list.asScala.toList.map(_.TransmissionScheme)
            val NumberofTxAntennasM: List[String] = bean45350.sr_88556_inst_list.asScala.toList.map(_.NumberofTxAntennasM)
            val NumberofRxAntennasN: List[String] = bean45350.sr_88556_inst_list.asScala.toList.map(_.NumberofRxAntennasN)

            logRecordQCommObj = logRecordQCommObj.copy(countLogcodeB126 = 1,
              modStream0 = ModulationStream0,
              modStream1 = ModulationStream1,
              freqSelPMI = FrequencySelectivePMI,
              spatialRank = SpatialRank,
              carrierIndex0xB126 = bean45350.CarrierIndex,
              pDSCHRNTIType = PDSCHRNTIType,
              transmissionScheme = TransmissionScheme,
              numTxAntennas = NumberofTxAntennasM,
              numRxAntennas = NumberofRxAntennasN)
          case 163 => val bean45350 = process45350_163(x, exceptionUseArray)
            val ModulationStream0: List[String] = null //bean45350.sr_117897_inst_list.asScala.toList.map(_.)
            val ModulationStream1: List[String] = null //bean45350.sr_117897_inst_list.asScala.toList.map(_.ModulationStream1)
            val FrequencySelectivePMI: List[String] = bean45350.sr_117897_inst_list.asScala.toList.map(_.FrequencySelectivePMI)
            val SpatialRank: List[String] = bean45350.sr_117897_inst_list.asScala.toList.map(_.SpatialRank)
            val PDSCHRNTIType: List[String] = bean45350.sr_117897_inst_list.asScala.toList.map(_.PDSCHRNTIType)
            val TransmissionScheme: List[String] = bean45350.sr_117897_inst_list.asScala.toList.map(_.TransmissionScheme)
            val NumberofTxAntennasM: List[String] = bean45350.sr_117897_inst_list.asScala.toList.map(_.NumberofTxAntennasM)
            val NumberofRxAntennasN: List[String] = bean45350.sr_117897_inst_list.asScala.toList.map(_.NumberofRxAntennasN)

            logRecordQCommObj = logRecordQCommObj.copy(countLogcodeB126 = 1,
              modStream0 = ModulationStream0,
              modStream1 = ModulationStream1,
              freqSelPMI = FrequencySelectivePMI,
              spatialRank = SpatialRank,
              carrierIndex0xB126 = bean45350.CarrierIndex,
              pDSCHRNTIType = PDSCHRNTIType,
              transmissionScheme = TransmissionScheme,
              numTxAntennas = NumberofTxAntennasM,
              numRxAntennas = NumberofRxAntennasN)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45350",
              hexLogCode = "0xB126", logRecordQComm = logRecordQCommObj)

          case _ =>
            //logger.info("Entered into Default Case for 45362(0xB132) for Version: " + logVersion)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45350", hexLogCode = "0xB126", missingVersion = logVersion)
        }
      case 45362 =>
        logVersion match {
          case 24 => val bean45362 = process45362_24(x, exceptionUseArray)
            val ReTranmissionNumFromBean: List[List[String]] = bean45362.sr_2970_inst_list.asScala.toList.map(_.sr_2982_inst_list.asScala.toList.map(_.RetransmissionNumber))
            val TransportBlockCrcFromBean: List[List[String]] = bean45362.sr_2970_inst_list.asScala.toList.map(_.sr_2982_inst_list.asScala.toList.map(_.TransportBlockCRC))
            logRecordQCommObj = logRecordQCommObj.copy( //numOfStreams = NumberOfStreamsFromBean,
              retransmissionNumber = ReTranmissionNumFromBean, transportBlockCrc = TransportBlockCrcFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45362", hexLogCode = "0xB132", logRecordQComm = logRecordQCommObj)
          case 44 => val bean45362 = process45362_44(x, exceptionUseArray)
            val ReTranmissionNumFromBean: List[List[String]] = bean45362.sr_3312_inst_list.asScala.toList.map(_.sr_3326_inst_list.asScala.toList.map(_.Retransmission_Number))
            val TransportBlockCrcFromBean: List[List[String]] = bean45362.sr_3312_inst_list.asScala.toList.map(_.sr_3326_inst_list.asScala.toList.map(_.Transport_Block_CRC))
            logRecordQCommObj = logRecordQCommObj.copy( //numOfStreams = NumberOfStreamsFromBean,
              retransmissionNumber = ReTranmissionNumFromBean, transportBlockCrc = TransportBlockCrcFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45362", hexLogCode = "0xB132", logRecordQComm = logRecordQCommObj)
          case 105 => val bean45362 = process45362_105(x, exceptionUseArray)
            val ReTranmissionNumFromBean: List[List[String]] = bean45362.sr_3605_inst_list.asScala.toList.map(_.sr_3619_inst_list.asScala.toList.map(_.RetransmissionNumber))
            val TransportBlockCrcFromBean: List[List[String]] = bean45362.sr_3605_inst_list.asScala.toList.map(_.sr_3619_inst_list.asScala.toList.map(_.TransportBlockCRC))
            logRecordQCommObj = logRecordQCommObj.copy( //numOfStreams = NumberOfStreamsFromBean,
              retransmissionNumber = ReTranmissionNumFromBean, transportBlockCrc = TransportBlockCrcFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45362", hexLogCode = "0xB132", logRecordQComm = logRecordQCommObj)
          case 106 => val bean45362 = process45362_106(x, exceptionUseArray)
            val ReTranmissionNumFromBean: List[List[String]] = bean45362.sr_95664_inst_list.asScala.toList.map(_.sr_95672_inst_list.asScala.toList.map(_.RetransmissionNumber))
            val TransportBlockCrcFromBean: List[List[String]] = bean45362.sr_95664_inst_list.asScala.toList.map(_.sr_95672_inst_list.asScala.toList.map(_.TransportBlockCRC))
            logRecordQCommObj = logRecordQCommObj.copy( //numOfStreams = NumberOfStreamsFromBean,
              retransmissionNumber = ReTranmissionNumFromBean, transportBlockCrc = TransportBlockCrcFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45362", hexLogCode = "0xB132", logRecordQComm = logRecordQCommObj)
          case 124 => val bean45362 = process45362_124(x, exceptionUseArray)
            val ReTranmissionNumFromBean: List[List[String]] = bean45362.sr_4037_inst_list.asScala.toList.map(_.sr_4048_inst_list.asScala.toList.map(_.RetransmissionNumber))
            val TransportBlockCrcFromBean: List[List[String]] = bean45362.sr_4037_inst_list.asScala.toList.map(_.sr_4048_inst_list.asScala.toList.map(_.TransportBlockCRC))
            logRecordQCommObj = logRecordQCommObj.copy( //numOfStreams = NumberOfStreamsFromBean,
              retransmissionNumber = ReTranmissionNumFromBean, transportBlockCrc = TransportBlockCrcFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45362", hexLogCode = "0xB132", logRecordQComm = logRecordQCommObj)
          case 125 => val bean45362 = process45362_125(x, exceptionUseArray)
            val ReTranmissionNumFromBean: List[List[String]] = bean45362.sr_4120_inst_list.asScala.toList.map(_.sr_4131_inst_list.asScala.toList.map(_.RetransmissionNumber))
            val TransportBlockCrcFromBean: List[List[String]] = bean45362.sr_4120_inst_list.asScala.toList.map(_.sr_4131_inst_list.asScala.toList.map(_.TransportBlockCRC))
            logRecordQCommObj = logRecordQCommObj.copy( //numOfStreams = NumberOfStreamsFromBean,
              retransmissionNumber = ReTranmissionNumFromBean, transportBlockCrc = TransportBlockCrcFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45362", hexLogCode = "0xB132", logRecordQComm = logRecordQCommObj)
          case 126 => val bean45362 = process45362_126(x, exceptionUseArray)
            //val NumberOfStreamsFromBean: List[Long]  = bean45362.sr_4203_inst_list.asScala.toList.map(_.NumberofStreams).asInstanceOf[List[Long]]
            val ReTranmissionNumFromBean: List[List[String]] = bean45362.sr_97208_inst_list.asScala.toList.map(_.sr_97217_inst_list.asScala.toList.map(_.RetransmissionNumber))
            val TransportBlockCrcFromBean: List[List[String]] = bean45362.sr_97208_inst_list.asScala.toList.map(_.sr_97217_inst_list.asScala.toList.map(_.TransportBlockCRC))
            logRecordQCommObj = logRecordQCommObj.copy( //numOfStreams = NumberOfStreamsFromBean,
              retransmissionNumber = ReTranmissionNumFromBean, transportBlockCrc = TransportBlockCrcFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45362", hexLogCode = "0xB132", logRecordQComm = logRecordQCommObj)
          case 127 => val bean45362 = process45362_127(x, exceptionUseArray)
            //val NumberOfStreamsFromBean: List[Long]  = bean45362.sr_4203_inst_list.asScala.toList.map(_.NumberofStreams).asInstanceOf[List[Long]]
            val ReTranmissionNumFromBean: List[List[String]] = bean45362.sr_97481_inst_list.asScala.toList.map(_.sr_97491_inst_list.asScala.toList.map(_.RetransmissionNumber))
            val TransportBlockCrcFromBean: List[List[String]] = bean45362.sr_97481_inst_list.asScala.toList.map(_.sr_97491_inst_list.asScala.toList.map(_.TransportBlockCRC))
            logRecordQCommObj = logRecordQCommObj.copy( //numOfStreams = NumberOfStreamsFromBean,
              retransmissionNumber = ReTranmissionNumFromBean, transportBlockCrc = TransportBlockCrcFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45362", hexLogCode = "0xB132", logRecordQComm = logRecordQCommObj)
          case 143 => val bean45362 = process45362_143(x, exceptionUseArray)
            val ReTranmissionNumFromBean: List[List[String]] = bean45362.sr_90596_inst_list.asScala.toList.map(_.sr_90609_inst_list.asScala.toList.map(_.RetransmissionNumber))
            val TransportBlockCrcFromBean: List[List[String]] = bean45362.sr_90596_inst_list.asScala.toList.map(_.sr_90609_inst_list.asScala.toList.map(_.TransportBlockCRC))
            logRecordQCommObj = logRecordQCommObj.copy( //numOfStreams = NumberOfStreamsFromBean,
              retransmissionNumber = ReTranmissionNumFromBean, transportBlockCrc = TransportBlockCrcFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45362", hexLogCode = "0xB132", logRecordQComm = logRecordQCommObj)

          case 144 | 166 => val bean45362 = process45362_144(x, exceptionUseArray)
            val ReTranmissionNumFromBean: List[List[String]] = bean45362.sr_95235_inst_list.asScala.toList.map(_.sr_95208_inst_list.asScala.toList.map(_.RetransmissionNumber))
            val TransportBlockCrcFromBean: List[List[String]] = bean45362.sr_95235_inst_list.asScala.toList.map(_.sr_95208_inst_list.asScala.toList.map(_.TransportBlockCRC))
            logRecordQCommObj = logRecordQCommObj.copy( //numOfStreams = NumberOfStreamsFromBean,
              retransmissionNumber = ReTranmissionNumFromBean, transportBlockCrc = TransportBlockCrcFromBean)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45362", hexLogCode = "0xB132", logRecordQComm = logRecordQCommObj)
          case _ =>
            //logger.info("Entered into Default Case for 45362(0xB132) for Version: " + logVersion)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45362", hexLogCode = "0xB132", missingVersion = logVersion)
        }
    }

    logRecord
  }

  def parseLogRecordQCOMM_Set8(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQCommObj:LogRecordQComm = LogRecordQComm()

    logCode match {
      case 61668=>
        val adbMsgJsonStr: String = getAdbMsgBeanAsJSON(x)
        var rmTestName =  getJsonValueByKey(adbMsgJsonStr,"test_name")
        var rmTestState = getJsonValueByKey(adbMsgJsonStr,"test_state")
        var deviceTimeString =  getJsonValueByKey(adbMsgJsonStr,"timestamp")

        deviceTimeString = deviceTimeString.replace(" ", "T")

        if(rmTestState=="start") {
          prevTestName = rmTestName
          prevDeviceTime = ZonedDateTime.parse(deviceTimeString + 'Z')
          logRecordQCommObj = getKpisFrom61668(adbMsgJsonStr,null)
        } else if(rmTestState=="end") {
          if (rmTestName == prevTestName) {
            logRecordQCommObj = getKpisFrom61668(adbMsgJsonStr,prevDeviceTime)
          } else {
            logRecordQCommObj = getKpisFrom61668(adbMsgJsonStr,null)
          }
        } else  {
          logRecordQCommObj = getKpisFrom61668(adbMsgJsonStr,null)
        }

        logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "61668", hexLogCode = "0xF0E4", logRecordQComm = logRecordQCommObj)

      case 45390=>
        logVersion match {
          case 163 =>
            val bean45390 = process45390_163(x, exceptionUseArray)
            var pccCQICW0: Integer = null
            var scc1CQICW0: Integer = null
            var scc2CQICW0: Integer = null
            var scc3CQICW0: Integer = null
            var scc4CQICW0: Integer = null
            var scc5CQICW0: Integer = null
            var pccCQICW1: Integer = null
            var scc1CQICW1: Integer = null
            var scc2CQICW1: Integer = null
            var scc3CQICW1: Integer = null
            var scc4CQICW1: Integer = null
            var scc5CQICW1: Integer = null

            var pccRi: Integer = null
            var scc1Ri: Integer = null
            var scc2Ri: Integer = null
            var scc3Ri: Integer = null
            var scc4Ri: Integer = null
            var scc5Ri: Integer = null
            var Ri: Integer = null

            if(bean45390.RankIndex != null && bean45390.RankIndex.contains("Rank ")){
              Ri = bean45390.RankIndex.split("Rank ")(1).toInt
            }

            bean45390.CarrierIndex match {
              case "PCC" =>
                pccCQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  pccCQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                pccRi = Ri
              case "SCC1" =>
                scc1CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc1CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc1Ri = Ri
              case "SCC2" =>
                scc2CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc2CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc2Ri = Ri
              case "SCC3" =>
                scc3CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc3CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc3Ri = Ri
              case "SCC4" =>
                scc4CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc4CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc4Ri = Ri
              case "SCC5" =>
                scc5CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc5CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc5Ri = Ri
              case _ =>
            }

            logRecordQCommObj = logRecordQCommObj.copy(
              carrierIndex0xB14E = bean45390.CarrierIndex,
              cQICW1Show0xB14E = if (bean45390.WideBandCQICW0 !=null) 1 else 0,
              cQICW00xB14E = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null,
              cQICW10xB14E = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null,
              rankIndexStr0xB14E = bean45390.RankIndex,
              scc1CQICW0 = scc1CQICW0,
              scc2CQICW0 = scc2CQICW0,
              scc3CQICW0 = scc3CQICW0,
              scc4CQICW0 = scc4CQICW0,
              scc5CQICW0 = scc5CQICW0,
              scc1CQICW1 = scc1CQICW1,
              scc2CQICW1 = scc2CQICW1,
              scc3CQICW1 = scc3CQICW1,
              scc4CQICW1 = scc4CQICW1,
              scc5CQICW1 = scc5CQICW1,
              pccRankIndex = pccRi,
              scc1RankIndex = scc1Ri,
              scc2RankIndex = scc2Ri,
              scc3RankIndex = scc3Ri,
              scc4RankIndex = scc4Ri,
              scc5RankIndex = scc5Ri)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45390",
              hexLogCode = "0xB14E", logRecordQComm = logRecordQCommObj)
          case 142 =>
            val bean45390 = process45390_142(x, exceptionUseArray)
            var pccCQICW0: Integer = null
            var scc1CQICW0: Integer = null
            var scc2CQICW0: Integer = null
            var scc3CQICW0: Integer = null
            var scc4CQICW0: Integer = null
            var scc5CQICW0: Integer = null
            var pccCQICW1: Integer = null
            var scc1CQICW1: Integer = null
            var scc2CQICW1: Integer = null
            var scc3CQICW1: Integer = null
            var scc4CQICW1: Integer = null
            var scc5CQICW1: Integer = null

            var pccRi: Integer = null
            var scc1Ri: Integer = null
            var scc2Ri: Integer = null
            var scc3Ri: Integer = null
            var scc4Ri: Integer = null
            var scc5Ri: Integer = null
            var Ri: Integer = null

            if(bean45390.RankIndex != null && bean45390.RankIndex.contains("Rank ")){
              Ri = bean45390.RankIndex.split("Rank ")(1).toInt
            }

            bean45390.CarrierIndex match {
              case "PCC" =>
                pccCQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  pccCQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                pccRi = Ri
              case "SCC1" =>
                scc1CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc1CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc1Ri = Ri
              case "SCC2" =>
                scc2CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc2CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc2Ri = Ri
              case "SCC3" =>
                scc3CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc3CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc3Ri = Ri
              case "SCC4" =>
                scc4CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc4CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc4Ri = Ri
              case "SCC5" =>
                scc5CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc5CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc5Ri = Ri
              case _ =>
            }

            logRecordQCommObj = logRecordQCommObj.copy(
              carrierIndex0xB14E = bean45390.CarrierIndex,
              cQICW1Show0xB14E = if (bean45390.WideBandCQICW0 !=null) 1 else 0,
              cQICW00xB14E = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null,
              cQICW10xB14E = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null,
              rankIndexStr0xB14E = bean45390.RankIndex,
              scc1CQICW0 = scc1CQICW0,
              scc2CQICW0 = scc2CQICW0,
              scc3CQICW0 = scc3CQICW0,
              scc4CQICW0 = scc4CQICW0,
              scc5CQICW0 = scc5CQICW0,
              scc1CQICW1 = scc1CQICW1,
              scc2CQICW1 = scc2CQICW1,
              scc3CQICW1 = scc3CQICW1,
              scc4CQICW1 = scc4CQICW1,
              scc5CQICW1 = scc5CQICW1,
              pccRankIndex = pccRi,
              scc1RankIndex = scc1Ri,
              scc2RankIndex = scc2Ri,
              scc3RankIndex = scc3Ri,
              scc4RankIndex = scc4Ri,
              scc5RankIndex = scc5Ri)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45390",
              hexLogCode = "0xB14E", logRecordQComm = logRecordQCommObj)
          case 42 =>
            val bean45390 = process45390_42(x, exceptionUseArray)
            var pccCQICW0: Integer = null
            var scc1CQICW0: Integer = null
            var scc2CQICW0: Integer = null
            var scc3CQICW0: Integer = null
            var scc4CQICW0: Integer = null
            var scc5CQICW0: Integer = null
            var pccCQICW1: Integer = null
            var scc1CQICW1: Integer = null
            var scc2CQICW1: Integer = null
            var scc3CQICW1: Integer = null
            var scc4CQICW1: Integer = null
            var scc5CQICW1: Integer = null

            var pccRi: Integer = null
            var scc1Ri: Integer = null
            var scc2Ri: Integer = null
            var scc3Ri: Integer = null
            var scc4Ri: Integer = null
            var scc5Ri: Integer = null
            var Ri: Integer = null

            if(bean45390.RankIndex != null && bean45390.RankIndex.contains("Rank ")){
              Ri = bean45390.RankIndex.split("Rank ")(1).toInt
            }

            bean45390.CarrierIndex match {
              case "PCC" =>
                pccCQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  pccCQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                pccRi = Ri
              case "SCC1" =>
                scc1CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc1CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc1Ri = Ri
              case "SCC2" =>
                scc2CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc2CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc2Ri = Ri
              case "SCC3" =>
                scc3CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc3CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc3Ri = Ri
              case "SCC4" =>
                scc4CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc4CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc4Ri = Ri
              case "SCC5" =>
                scc5CQICW0 = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null
                if (bean45390.WideBandCQICW1 !=null) {
                  scc5CQICW1 = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null
                }
                scc5Ri = Ri
              case _ =>
            }

            logRecordQCommObj = logRecordQCommObj.copy(
              carrierIndex0xB14E = bean45390.CarrierIndex,
              cQICW1Show0xB14E = if (bean45390.WideBandCQICW0 !=null) 1 else 0,
              cQICW00xB14E = if (Constants.isValidNumeric(bean45390.WideBandCQICW0,"NUMBER")) bean45390.WideBandCQICW0.toInt else null,
              cQICW10xB14E = if (Constants.isValidNumeric(bean45390.WideBandCQICW1,"NUMBER")) bean45390.WideBandCQICW1.toInt else null,
              rankIndexStr0xB14E = bean45390.RankIndex,
              scc1CQICW0 = scc1CQICW0,
              scc2CQICW0 = scc2CQICW0,
              scc3CQICW0 = scc3CQICW0,
              scc4CQICW0 = scc4CQICW0,
              scc5CQICW0 = scc5CQICW0,
              scc1CQICW1 = scc1CQICW1,
              scc2CQICW1 = scc2CQICW1,
              scc3CQICW1 = scc3CQICW1,
              scc4CQICW1 = scc4CQICW1,
              scc5CQICW1 = scc5CQICW1,
              pccRankIndex = pccRi,
              scc1RankIndex = scc1Ri,
              scc2RankIndex = scc2Ri,
              scc3RankIndex = scc3Ri,
              scc4RankIndex = scc4Ri,
              scc5RankIndex = scc5Ri)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45390",
              hexLogCode = "0xB14E", logRecordQComm = logRecordQCommObj)
          case _ =>
            val bean45390 = process45390(x, exceptionUseArray)
            var pccCQICW0: Integer = null
            var scc1CQICW0: Integer = null
            var scc2CQICW0: Integer = null
            var scc3CQICW0: Integer = null
            var scc4CQICW0: Integer = null
            var scc5CQICW0: Integer = null
            var pccCQICW1: Integer = null
            var scc1CQICW1: Integer = null
            var scc2CQICW1: Integer = null
            var scc3CQICW1: Integer = null
            var scc4CQICW1: Integer = null
            var scc5CQICW1: Integer = null

            var pccRi: Integer = null
            var scc1Ri: Integer = null
            var scc2Ri: Integer = null
            var scc3Ri: Integer = null
            var scc4Ri: Integer = null
            var scc5Ri: Integer = null
            var Ri: Integer = null

            if(bean45390.rankIndex != null && bean45390.rankIndex.contains("Rank ")){
              Ri = bean45390.rankIndex.split("Rank ")(1).toInt
            }

            bean45390.CarrierIndex match {
              case "PCC" =>
                pccCQICW0 = bean45390.cqiCw0
                if (bean45390.cqiCw1Show == true) {
                  pccCQICW1 = bean45390.cqiCw1
                }
                pccRi = Ri
              case "SCC1" =>
                scc1CQICW0 = bean45390.cqiCw0
                if (bean45390.cqiCw1Show == true) {
                  scc1CQICW1 = bean45390.cqiCw1
                }
                scc1Ri = Ri
              case "SCC2" =>
                scc2CQICW0 = bean45390.cqiCw0
                if (bean45390.cqiCw1Show == true) {
                  scc2CQICW1 = bean45390.cqiCw1
                }
                scc2Ri = Ri
              case "SCC3" =>
                scc3CQICW0 = bean45390.cqiCw0
                if (bean45390.cqiCw1Show == true) {
                  scc3CQICW1 = bean45390.cqiCw1
                }
                scc3Ri = Ri
              case "SCC4" =>
                scc4CQICW0 = bean45390.cqiCw0
                if (bean45390.cqiCw1Show == true) {
                  scc4CQICW1 = bean45390.cqiCw1
                }
                scc4Ri = Ri
              case "SCC5" =>
                scc5CQICW0 = bean45390.cqiCw0
                if (bean45390.cqiCw1Show == true) {
                  scc5CQICW1 = bean45390.cqiCw1
                }
                scc5Ri = Ri
              case _ =>
            }

            logRecordQCommObj = logRecordQCommObj.copy(
              carrierIndex0xB14E = bean45390.CarrierIndex,
              cQICW1Show0xB14E = if (bean45390.cqiCw1Show == true) 1 else 0,
              cQICW00xB14E = bean45390.cqiCw0,
              cQICW10xB14E = bean45390.cqiCw1,
              rankIndexStr0xB14E = bean45390.rankIndex,
              scc1CQICW0 = scc1CQICW0,
              scc2CQICW0 = scc2CQICW0,
              scc3CQICW0 = scc3CQICW0,
              scc4CQICW0 = scc4CQICW0,
              scc5CQICW0 = scc5CQICW0,
              scc1CQICW1 = scc1CQICW1,
              scc2CQICW1 = scc2CQICW1,
              scc3CQICW1 = scc3CQICW1,
              scc4CQICW1 = scc4CQICW1,
              scc5CQICW1 = scc5CQICW1,
              pccRankIndex = pccRi,
              scc1RankIndex = scc1Ri,
              scc2RankIndex = scc2Ri,
              scc3RankIndex = scc3Ri,
              scc4RankIndex = scc4Ri,
              scc5RankIndex = scc5Ri)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45390",
              hexLogCode = "0xB14E", logRecordQComm = logRecordQCommObj)
        }

      case 45389 =>
        logVersion match {
          case 22 =>
            val bean45389 = process45389_22(x, exceptionUseArray)
            var pccCQICW0: Integer = null
            var scc1CQICW0: Integer = null
            var scc2CQICW0: Integer = null
            var scc3CQICW0: Integer = null
            var scc4CQICW0: Integer = null
            var scc5CQICW0: Integer = null
            var pccCQICW1: Integer = null
            var scc1CQICW1: Integer = null
            var scc2CQICW1: Integer = null
            var scc3CQICW1: Integer = null
            var scc4CQICW1: Integer = null
            var scc5CQICW1: Integer = null

            var CQICW0Show: Boolean = false
            var CQICW1Show: Boolean = false
            var rankIndexShow: Boolean = false
            var RankIndex: String = null

            var pccRi: Integer = null
            var scc1Ri: Integer = null
            var scc2Ri: Integer = null
            var scc3Ri: Integer = null
            var scc4Ri: Integer = null
            var scc5Ri: Integer = null
            var Ri: Integer = null

            var pucchReportType: String = bean45389.getPUCCHReportType()
            var pucchReportMode: String = bean45389.getPUCCHReportingMode()

            if (pucchReportType.contains("Type 1") || pucchReportType.contains("Type 2")) {
              CQICW0Show = true

              if (pucchReportMode.contains("1_1") || pucchReportMode.contains("2_1")) {
                CQICW1Show = true
              }
            }

            if (pucchReportType.contains("Type 3")) {
              rankIndexShow = true
              RankIndex = bean45389.RankIndex
              if(RankIndex != null && RankIndex.contains("Rank ")){
                Ri = RankIndex.split("Rank ")(1).toInt
              }
            }

            bean45389.CarrierIndex match {

              case "PCC" =>
                if (CQICW0Show) {
                  pccCQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  pccCQICW1 = bean45389.CQICW1.toInt
                }
                pccRi = Ri
              case "SCC1" =>
                if (CQICW0Show) {
                  scc1CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc1CQICW1 = bean45389.CQICW1.toInt
                }
                scc1Ri = Ri
              case "SCC2" =>
                if (CQICW0Show) {
                  scc2CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc2CQICW1 = bean45389.CQICW1.toInt
                }
                scc2Ri = Ri
              case "SCC3" =>
                if (CQICW0Show) {
                  scc3CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc3CQICW1 = bean45389.CQICW1.toInt
                }
                scc3Ri = Ri
              case "SCC4" =>
                if (CQICW0Show) {
                  scc4CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc4CQICW1 = bean45389.CQICW1.toInt
                }
                scc4Ri = Ri
              case "SCC5" =>
                if (CQICW0Show) {
                  scc5CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc5CQICW1 = bean45389.CQICW1.toInt
                }
                scc5Ri = Ri
              case _ =>

            }
            logRecordQCommObj = logRecordQCommObj.copy(
              carrierIndex0xB14D = bean45389.CarrierIndex,
              cQICW0 = pccCQICW0,
              cQICW1 = pccCQICW1,
              cSFTxMode = bean45389.CSFTxMode,
              pUCCHReportType = bean45389.PUCCHReportType,
              rankIndexStr = RankIndex,
              startSystemFrameNumber = bean45389.StartSystemFrameNumber.toInt,
              startSystemSubframeNumber = bean45389.StartSystemSubframeNumber.toInt,
              scc1CQICW0 = scc1CQICW0,
              scc2CQICW0 = scc2CQICW0,
              scc3CQICW0 = scc3CQICW0,
              scc4CQICW0 = scc4CQICW0,
              scc5CQICW0 = scc5CQICW0,
              scc1CQICW1 = scc1CQICW1,
              scc2CQICW1 = scc2CQICW1,
              scc3CQICW1 = scc3CQICW1,
              scc4CQICW1 = scc4CQICW1,
              scc5CQICW1 = scc5CQICW1
            )
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45389",
              hexLogCode = "0xB14D", logRecordQComm = logRecordQCommObj)
          case 24 => val bean45389 = process45389_24(x, exceptionUseArray)
            var pccCQICW0: Integer = null
            var scc1CQICW0: Integer = null
            var scc2CQICW0: Integer = null
            var scc3CQICW0: Integer = null
            var scc4CQICW0: Integer = null
            var scc5CQICW0: Integer = null
            var pccCQICW1: Integer = null
            var scc1CQICW1: Integer = null
            var scc2CQICW1: Integer = null
            var scc3CQICW1: Integer = null
            var scc4CQICW1: Integer = null
            var scc5CQICW1: Integer = null

            var CQICW0Show: Boolean = false
            var CQICW1Show: Boolean = false
            var rankIndexShow: Boolean = false
            var RankIndex: String = null

            var pccRi: Integer = null
            var scc1Ri: Integer = null
            var scc2Ri: Integer = null
            var scc3Ri: Integer = null
            var scc4Ri: Integer = null
            var scc5Ri: Integer = null
            var Ri: Integer = null

            var pucchReportType: String = bean45389.getPUCCHReportType()
            var pucchReportMode: String = bean45389.getPUCCHReportingMode()

            if (pucchReportType.contains("Type 1") || pucchReportType.contains("Type 2")) {
              CQICW0Show = true

              if (pucchReportMode.contains("1_1") || pucchReportMode.contains("2_1")) {
                CQICW1Show = true
              }
            }

            if (pucchReportType.contains("Type 3")) {
              rankIndexShow = true
              RankIndex = bean45389.RankIndex
              if(RankIndex != null && RankIndex.contains("Rank ")){
                Ri = RankIndex.split("Rank ")(1).toInt
              }
            }

            bean45389.CarrierIndex match {

              case "PCC" =>
                if (CQICW0Show) {
                  pccCQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  pccCQICW1 = bean45389.CQICW1.toInt
                }
                pccRi = Ri
              case "SCC1" =>
                if (CQICW0Show) {
                  scc1CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc1CQICW1 = bean45389.CQICW1.toInt
                }
                scc1Ri = Ri
              case "SCC2" =>
                if (CQICW0Show) {
                  scc2CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc2CQICW1 = bean45389.CQICW1.toInt
                }
                scc2Ri = Ri
              case "SCC3" =>
                if (CQICW0Show) {
                  scc3CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc3CQICW1 = bean45389.CQICW1.toInt
                }
                scc3Ri = Ri
              case "SCC4" =>
                if (CQICW0Show) {
                  scc4CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc4CQICW1 = bean45389.CQICW1.toInt
                }
                scc4Ri = Ri
              case "SCC5" =>
                if (CQICW0Show) {
                  scc5CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc5CQICW1 = bean45389.CQICW1.toInt
                }
                scc5Ri = Ri
              case _ =>

            }
            logRecordQCommObj = logRecordQCommObj.copy(
              carrierIndex0xB14D = bean45389.CarrierIndex,
              cQICW0 = pccCQICW0,
              cQICW1 = pccCQICW1,
              cSFTxMode = bean45389.CSFTxMode,
              pUCCHReportType = bean45389.PUCCHReportType,
              rankIndexStr = RankIndex,
              startSystemFrameNumber = bean45389.StartSystemFrameNumber.toInt,
              startSystemSubframeNumber = bean45389.StartSystemSubframeNumber.toInt,
              scc1CQICW0 = scc1CQICW0,
              scc2CQICW0 = scc2CQICW0,
              scc3CQICW0 = scc3CQICW0,
              scc4CQICW0 = scc4CQICW0,
              scc5CQICW0 = scc5CQICW0,
              scc1CQICW1 = scc1CQICW1,
              scc2CQICW1 = scc2CQICW1,
              scc3CQICW1 = scc3CQICW1,
              scc4CQICW1 = scc4CQICW1,
              scc5CQICW1 = scc5CQICW1
            )
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45389",
              hexLogCode = "0xB14D", logRecordQComm = logRecordQCommObj)
          case 43 => val bean45389 = process45389_43(x, exceptionUseArray)
            var pccCQICW0: Integer = null
            var scc1CQICW0: Integer = null
            var scc2CQICW0: Integer = null
            var scc3CQICW0: Integer = null
            var scc4CQICW0: Integer = null
            var scc5CQICW0: Integer = null
            var pccCQICW1: Integer = null
            var scc1CQICW1: Integer = null
            var scc2CQICW1: Integer = null
            var scc3CQICW1: Integer = null
            var scc4CQICW1: Integer = null
            var scc5CQICW1: Integer = null

            var CQICW0Show: Boolean = false
            var CQICW1Show: Boolean = false
            var rankIndexShow: Boolean = false
            var RankIndex: String = null

            var pccRi: Integer = null
            var scc1Ri: Integer = null
            var scc2Ri: Integer = null
            var scc3Ri: Integer = null
            var scc4Ri: Integer = null
            var scc5Ri: Integer = null
            var Ri: Integer = null

            var pucchReportType: String = bean45389.getPUCCHReportType()
            var pucchReportMode: String = bean45389.getPUCCHReportingMode()

            if (pucchReportType.contains("Type 1") || pucchReportType.contains("Type 2")) {
              CQICW0Show = true

              if (pucchReportMode.contains("1_1") || pucchReportMode.contains("2_1")) {
                CQICW1Show = true
              }
            }

            if (pucchReportType.contains("Type 3")) {
              rankIndexShow = true
              RankIndex = bean45389.RankIndex
              if(RankIndex != null && RankIndex.contains("Rank ")){
                Ri = RankIndex.split("Rank ")(1).toInt
              }
            }

            bean45389.CarrierIndex match {

              case "PCC" =>
                if (CQICW0Show) {
                  pccCQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  pccCQICW1 = bean45389.CQICW1.toInt
                }
                pccRi = Ri
              case "SCC1" =>
                if (CQICW0Show) {
                  scc1CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc1CQICW1 = bean45389.CQICW1.toInt
                }
                scc1Ri = Ri
              case "SCC2" =>
                if (CQICW0Show) {
                  scc2CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc2CQICW1 = bean45389.CQICW1.toInt
                }
                scc2Ri = Ri
              case "SCC3" =>
                if (CQICW0Show) {
                  scc3CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc3CQICW1 = bean45389.CQICW1.toInt
                }
                scc3Ri = Ri
              case "SCC4" =>
                if (CQICW0Show) {
                  scc4CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc4CQICW1 = bean45389.CQICW1.toInt
                }
                scc4Ri = Ri
              case "SCC5" =>
                if (CQICW0Show) {
                  scc5CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc5CQICW1 = bean45389.CQICW1.toInt
                }
                scc5Ri = Ri
              case _ =>

            }
            logRecordQCommObj = logRecordQCommObj.copy(
              carrierIndex0xB14D = bean45389.CarrierIndex,
              cQICW0 = pccCQICW0,
              cQICW1 = pccCQICW1,
              cSFTxMode = bean45389.CSFTxMode,
              pUCCHReportType = bean45389.PUCCHReportType,
              rankIndexStr = RankIndex,
              startSystemFrameNumber = bean45389.StartSystemFrameNumber.toInt,
              startSystemSubframeNumber = bean45389.StartSystemSubframeNumber.toInt,
              scc1CQICW0 = scc1CQICW0,
              scc2CQICW0 = scc2CQICW0,
              scc3CQICW0 = scc3CQICW0,
              scc4CQICW0 = scc4CQICW0,
              scc5CQICW0 = scc5CQICW0,
              scc1CQICW1 = scc1CQICW1,
              scc2CQICW1 = scc2CQICW1,
              scc3CQICW1 = scc3CQICW1,
              scc4CQICW1 = scc4CQICW1,
              scc5CQICW1 = scc5CQICW1,
              pccRankIndex = pccRi,
              scc1RankIndex = scc1Ri,
              scc2RankIndex = scc2Ri,
              scc3RankIndex = scc3Ri,
              scc4RankIndex = scc4Ri,
              scc5RankIndex = scc5Ri)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45389",
              hexLogCode = "0xB14D", logRecordQComm = logRecordQCommObj)
          case 101 => val bean45389 = process45389_101(x, exceptionUseArray)
            var pccCQICW0: Integer = null
            var scc1CQICW0: Integer = null
            var scc2CQICW0: Integer = null
            var scc3CQICW0: Integer = null
            var scc4CQICW0: Integer = null
            var scc5CQICW0: Integer = null
            var pccCQICW1: Integer = null
            var scc1CQICW1: Integer = null
            var scc2CQICW1: Integer = null
            var scc3CQICW1: Integer = null
            var scc4CQICW1: Integer = null
            var scc5CQICW1: Integer = null

            var CQICW0Show: Boolean = false
            var CQICW1Show: Boolean = false
            var rankIndexShow: Boolean = false
            var RankIndex: String = null

            var pccRi: Integer = null
            var scc1Ri: Integer = null
            var scc2Ri: Integer = null
            var scc3Ri: Integer = null
            var scc4Ri: Integer = null
            var scc5Ri: Integer = null
            var Ri: Integer = null

            var pucchReportType: String = bean45389.getPUCCHReportType()
            var pucchReportMode: String = bean45389.getPUCCHReportingMode()

            if (pucchReportType.contains("Type 1") || pucchReportType.contains("Type 2")) {
              CQICW0Show = true

              if (pucchReportMode.contains("1_1") || pucchReportMode.contains("2_1")) {
                CQICW1Show = true
              }
            }

            if (pucchReportType.contains("Type 3")) {
              rankIndexShow = true
              RankIndex = bean45389.RankIndex
              if(RankIndex != null && RankIndex.contains("Rank ")){
                Ri = RankIndex.split("Rank ")(1).toInt
              }
            }

            bean45389.CarrierIndex match {

              case "PCC" =>
                if (CQICW0Show) {
                  pccCQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  pccCQICW1 = bean45389.CQICW1.toInt
                }
                pccRi = Ri
              case "SCC1" =>
                if (CQICW0Show) {
                  scc1CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc1CQICW1 = bean45389.CQICW1.toInt
                }
                scc1Ri = Ri
              case "SCC2" =>
                if (CQICW0Show) {
                  scc2CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc2CQICW1 = bean45389.CQICW1.toInt
                }
                scc2Ri = Ri
              case "SCC3" =>
                if (CQICW0Show) {
                  scc3CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc3CQICW1 = bean45389.CQICW1.toInt
                }
                scc3Ri = Ri
              case "SCC4" =>
                if (CQICW0Show) {
                  scc4CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc4CQICW1 = bean45389.CQICW1.toInt
                }
                scc4Ri = Ri
              case "SCC5" =>
                if (CQICW0Show) {
                  scc5CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc5CQICW1 = bean45389.CQICW1.toInt
                }
                scc5Ri = Ri
              case _ =>

            }
            logRecordQCommObj = logRecordQCommObj.copy(
              carrierIndex0xB14D = bean45389.CarrierIndex,
              cQICW0 = pccCQICW0,
              cQICW1 = pccCQICW1,
              cSFTxMode = bean45389.CSFTxMode,
              pUCCHReportType = bean45389.PUCCHReportType,
              rankIndexStr = RankIndex,
              startSystemFrameNumber = bean45389.StartSystemFrameNumber.toInt,
              startSystemSubframeNumber = bean45389.StartSystemSubframeNumber.toInt,
              scc1CQICW0 = scc1CQICW0,
              scc2CQICW0 = scc2CQICW0,
              scc3CQICW0 = scc3CQICW0,
              scc4CQICW0 = scc4CQICW0,
              scc5CQICW0 = scc5CQICW0,
              scc1CQICW1 = scc1CQICW1,
              scc2CQICW1 = scc2CQICW1,
              scc3CQICW1 = scc3CQICW1,
              scc4CQICW1 = scc4CQICW1,
              scc5CQICW1 = scc5CQICW1,
              pccRankIndex = pccRi,
              scc1RankIndex = scc1Ri,
              scc2RankIndex = scc2Ri,
              scc3RankIndex = scc3Ri,
              scc4RankIndex = scc4Ri,
              scc5RankIndex = scc5Ri)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45389",
              hexLogCode = "0xB14D", logRecordQComm = logRecordQCommObj)
          case 102 => val bean45389 = process45389_102(x, exceptionUseArray)

            var pccCQICW0: Integer = null
            var scc1CQICW0: Integer = null
            var scc2CQICW0: Integer = null
            var scc3CQICW0: Integer = null
            var scc4CQICW0: Integer = null
            var scc5CQICW0: Integer = null
            var pccCQICW1: Integer = null
            var scc1CQICW1: Integer = null
            var scc2CQICW1: Integer = null
            var scc3CQICW1: Integer = null
            var scc4CQICW1: Integer = null
            var scc5CQICW1: Integer = null

            var CQICW0Show: Boolean = false
            var CQICW1Show: Boolean = false
            var rankIndexShow: Boolean = false
            var RankIndex: String = null

            var pccRi: Integer = null
            var scc1Ri: Integer = null
            var scc2Ri: Integer = null
            var scc3Ri: Integer = null
            var scc4Ri: Integer = null
            var scc5Ri: Integer = null
            var Ri: Integer = null

            var pucchReportType: String = bean45389.getPUCCHReportType()
            var pucchReportMode: String = bean45389.getPUCCHReportingMode()

            if (pucchReportType.contains("Type 1") || pucchReportType.contains("Type 2")) {
              CQICW0Show = true

              if (pucchReportMode.contains("1_1") || pucchReportMode.contains("2_1")) {
                CQICW1Show = true
              }
            }

            if (pucchReportType.contains("Type 3")) {
              rankIndexShow = true
              RankIndex = bean45389.RankIndex
              if(RankIndex != null && RankIndex.contains("Rank ")){
                Ri = RankIndex.split("Rank ")(1).toInt
              }
            }

            bean45389.CarrierIndex match {

              case "PCC" =>
                if (CQICW0Show) {
                  pccCQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  pccCQICW1 = bean45389.CQICW1.toInt
                }
                pccRi = Ri
              case "SCC1" =>
                if (CQICW0Show) {
                  scc1CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc1CQICW1 = bean45389.CQICW1.toInt
                }
                scc1Ri = Ri
              case "SCC2" =>
                if (CQICW0Show) {
                  scc2CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc2CQICW1 = bean45389.CQICW1.toInt
                }
                scc2Ri = Ri
              case "SCC3" =>
                if (CQICW0Show) {
                  scc3CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc3CQICW1 = bean45389.CQICW1.toInt
                }
                scc3Ri = Ri
              case "SCC4" =>
                if (CQICW0Show) {
                  scc4CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc4CQICW1 = bean45389.CQICW1.toInt
                }
                scc4Ri = Ri
              case "SCC5" =>
                if (CQICW0Show) {
                  scc5CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc5CQICW1 = bean45389.CQICW1.toInt
                }
                scc5Ri = Ri
              case _ =>

            }
            logRecordQCommObj = logRecordQCommObj.copy(
              carrierIndex0xB14D = bean45389.CarrierIndex,
              cQICW0 = pccCQICW0,
              cQICW1 = pccCQICW1,
              cSFTxMode = bean45389.CSFTxMode,
              pUCCHReportType = bean45389.PUCCHReportType,
              rankIndexStr = RankIndex,
              startSystemFrameNumber = bean45389.StartSystemFrameNumber.toInt,
              startSystemSubframeNumber = bean45389.StartSystemSubframeNumber.toInt,
              scc1CQICW0 = scc1CQICW0,
              scc2CQICW0 = scc2CQICW0,
              scc3CQICW0 = scc3CQICW0,
              scc4CQICW0 = scc4CQICW0,
              scc5CQICW0 = scc5CQICW0,
              scc1CQICW1 = scc1CQICW1,
              scc2CQICW1 = scc2CQICW1,
              scc3CQICW1 = scc3CQICW1,
              scc4CQICW1 = scc4CQICW1,
              scc5CQICW1 = scc5CQICW1,
              pccRankIndex = pccRi,
              scc1RankIndex = scc1Ri,
              scc2RankIndex = scc2Ri,
              scc3RankIndex = scc3Ri,
              scc4RankIndex = scc4Ri,
              scc5RankIndex = scc5Ri)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45389",
              hexLogCode = "0xB14D", logRecordQComm = logRecordQCommObj)
          case 142 =>
            val bean45389 = process45389_142(x, exceptionUseArray)

            var pccCQICW0: Integer = null
            var scc1CQICW0: Integer = null
            var scc2CQICW0: Integer = null
            var scc3CQICW0: Integer = null
            var scc4CQICW0: Integer = null
            var scc5CQICW0: Integer = null
            var pccCQICW1: Integer = null
            var scc1CQICW1: Integer = null
            var scc2CQICW1: Integer = null
            var scc3CQICW1: Integer = null
            var scc4CQICW1: Integer = null
            var scc5CQICW1: Integer = null

            var CQICW0Show: Boolean = false
            var CQICW1Show: Boolean = false
            var rankIndexShow: Boolean = false
            var RankIndex: String = null

            var pccRi: Integer = null
            var scc1Ri: Integer = null
            var scc2Ri: Integer = null
            var scc3Ri: Integer = null
            var scc4Ri: Integer = null
            var scc5Ri: Integer = null
            var Ri: Integer = null

            var pucchReportType: String = bean45389.getPUCCHReportType()
            var pucchReportMode: String = bean45389.getPUCCHReportingMode()

            if (pucchReportType.contains("Type 1") || pucchReportType.contains("Type 2")) {
              CQICW0Show = true

              if (pucchReportMode.contains("1_1") || pucchReportMode.contains("2_1")) {
                CQICW1Show = true
              }
            }

            if (pucchReportType.contains("Type 3")) {
              rankIndexShow = true
              RankIndex = bean45389.RankIndex

              if(RankIndex != null && RankIndex.contains("Rank ")){
                Ri = RankIndex.split("Rank ")(1).toInt
              }
            }

            bean45389.CarrierIndex match {

              case "PCC" =>
                if (CQICW0Show) {
                  pccCQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  pccCQICW1 = bean45389.CQICW1.toInt
                }
                pccRi = Ri
              case "SCC1" =>
                if (CQICW0Show) {
                  scc1CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc1CQICW1 = bean45389.CQICW1.toInt
                }
                scc1Ri = Ri
              case "SCC2" =>
                if (CQICW0Show) {
                  scc2CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc2CQICW1 = bean45389.CQICW1.toInt
                }
                scc2Ri = Ri
              case "SCC3" =>
                if (CQICW0Show) {
                  scc3CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc3CQICW1 = bean45389.CQICW1.toInt
                }
                scc3Ri = Ri
              case "SCC4" =>
                if (CQICW0Show) {
                  scc4CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc4CQICW1 = bean45389.CQICW1.toInt
                }
                scc4Ri = Ri
              case "SCC5" =>
                if (CQICW0Show) {
                  scc5CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc5CQICW1 = bean45389.CQICW1.toInt
                }
                scc5Ri = Ri
              case _ =>

            }
            logRecordQCommObj = logRecordQCommObj.copy(
              carrierIndex0xB14D = bean45389.CarrierIndex,
              cQICW0 = pccCQICW0,
              cQICW1 = pccCQICW1,
              cSFTxMode = bean45389.CSFTxMode,
              pUCCHReportType = bean45389.PUCCHReportType,
              rankIndexStr = RankIndex,
              startSystemFrameNumber = bean45389.StartSystemFrameNumber.toInt,
              startSystemSubframeNumber = bean45389.StartSystemSubframeNumber.toInt,
              scc1CQICW0 = scc1CQICW0,
              scc2CQICW0 = scc2CQICW0,
              scc3CQICW0 = scc3CQICW0,
              scc4CQICW0 = scc4CQICW0,
              scc5CQICW0 = scc5CQICW0,
              scc1CQICW1 = scc1CQICW1,
              scc2CQICW1 = scc2CQICW1,
              scc3CQICW1 = scc3CQICW1,
              scc4CQICW1 = scc4CQICW1,
              scc5CQICW1 = scc5CQICW1,
              pccRankIndex = pccRi,
              scc1RankIndex = scc1Ri,
              scc2RankIndex = scc2Ri,
              scc3RankIndex = scc3Ri,
              scc4RankIndex = scc4Ri,
              scc5RankIndex = scc5Ri)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45389",
              hexLogCode = "0xB14D", logRecordQComm = logRecordQCommObj)

          case 162 =>
            val bean45389 = process45389_162(x, exceptionUseArray)

            var pccCQICW0: Integer = null
            var scc1CQICW0: Integer = null
            var scc2CQICW0: Integer = null
            var scc3CQICW0: Integer = null
            var scc4CQICW0: Integer = null
            var scc5CQICW0: Integer = null
            var pccCQICW1: Integer = null
            var scc1CQICW1: Integer = null
            var scc2CQICW1: Integer = null
            var scc3CQICW1: Integer = null
            var scc4CQICW1: Integer = null
            var scc5CQICW1: Integer = null

            var CQICW0Show: Boolean = false
            var CQICW1Show: Boolean = false
            var rankIndexShow: Boolean = false
            var RankIndex: String = null

            var pccRi: Integer = null
            var scc1Ri: Integer = null
            var scc2Ri: Integer = null
            var scc3Ri: Integer = null
            var scc4Ri: Integer = null
            var scc5Ri: Integer = null
            var Ri: Integer = null

            var pucchReportType: String = bean45389.getPUCCHReportType()
            var pucchReportMode: String = bean45389.getPUCCHReportingMode()

            if (pucchReportType.contains("Type 1") || pucchReportType.contains("Type 2")) {
              CQICW0Show = true

              if (pucchReportMode.contains("1_1") || pucchReportMode.contains("2_1")) {
                CQICW1Show = true
              }
            }

            if (pucchReportType.contains("Type 3")) {
              rankIndexShow = true
              RankIndex = bean45389.RankIndex

              if(RankIndex != null && RankIndex.contains("Rank ")){
                Ri = RankIndex.split("Rank ")(1).toInt
              }
            }

            bean45389.CarrierIndex match {

              case "PCC" =>
                if (CQICW0Show) {
                  pccCQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  pccCQICW1 = bean45389.CQICW1.toInt
                }
                pccRi = Ri
              case "SCC1" =>
                if (CQICW0Show) {
                  scc1CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc1CQICW1 = bean45389.CQICW1.toInt
                }
                scc1Ri = Ri
              case "SCC2" =>
                if (CQICW0Show) {
                  scc2CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc2CQICW1 = bean45389.CQICW1.toInt
                }
                scc2Ri = Ri
              case "SCC3" =>
                if (CQICW0Show) {
                  scc3CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc3CQICW1 = bean45389.CQICW1.toInt
                }
                scc3Ri = Ri
              case "SCC4" =>
                if (CQICW0Show) {
                  scc4CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc4CQICW1 = bean45389.CQICW1.toInt
                }
                scc4Ri = Ri
              case "SCC5" =>
                if (CQICW0Show) {
                  scc5CQICW0 = bean45389.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc5CQICW1 = bean45389.CQICW1.toInt
                }
                scc5Ri = Ri
              case _ =>

            }
            logRecordQCommObj = logRecordQCommObj.copy(
              carrierIndex0xB14D = bean45389.CarrierIndex,
              cQICW0 = pccCQICW0,
              cQICW1 = pccCQICW1,
              cSFTxMode = bean45389.CSFTxMode,
              pUCCHReportType = bean45389.PUCCHReportType,
              rankIndexStr = RankIndex,
              startSystemFrameNumber = bean45389.StartSystemFrameNumber.toInt,
              startSystemSubframeNumber = bean45389.StartSystemSubframeNumber.toInt,
              scc1CQICW0 = scc1CQICW0,
              scc2CQICW0 = scc2CQICW0,
              scc3CQICW0 = scc3CQICW0,
              scc4CQICW0 = scc4CQICW0,
              scc5CQICW0 = scc5CQICW0,
              scc1CQICW1 = scc1CQICW1,
              scc2CQICW1 = scc2CQICW1,
              scc3CQICW1 = scc3CQICW1,
              scc4CQICW1 = scc4CQICW1,
              scc5CQICW1 = scc5CQICW1,
              pccRankIndex = pccRi,
              scc1RankIndex = scc1Ri,
              scc2RankIndex = scc2Ri,
              scc3RankIndex = scc3Ri,
              scc4RankIndex = scc4Ri,
              scc5RankIndex = scc5Ri)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45389",
              hexLogCode = "0xB14D", logRecordQComm = logRecordQCommObj)
          case 163 =>
            val bean45389_163 = process45389_163(x, exceptionUseArray)

            var pccCQICW0: Integer = null
            var scc1CQICW0: Integer = null
            var scc2CQICW0: Integer = null
            var scc3CQICW0: Integer = null
            var scc4CQICW0: Integer = null
            var scc5CQICW0: Integer = null
            var pccCQICW1: Integer = null
            var scc1CQICW1: Integer = null
            var scc2CQICW1: Integer = null
            var scc3CQICW1: Integer = null
            var scc4CQICW1: Integer = null
            var scc5CQICW1: Integer = null

            var CQICW0Show: Boolean = false
            var CQICW1Show: Boolean = false
            var rankIndexShow: Boolean = false
            var RankIndex: String = null

            var pccRi: Integer = null
            var scc1Ri: Integer = null
            var scc2Ri: Integer = null
            var scc3Ri: Integer = null
            var scc4Ri: Integer = null
            var scc5Ri: Integer = null
            var Ri: Integer = null

            var pucchReportType: String = bean45389_163.getPUCCHReportType()
            var pucchReportMode: String = bean45389_163.getPUCCHReportingMode()

            if (pucchReportType.contains("Type 1") || pucchReportType.contains("Type 2")) {
              CQICW0Show = true

              if (pucchReportMode.contains("1_1") || pucchReportMode.contains("2_1")) {
                CQICW1Show = true
              }
            }

            if (pucchReportType.contains("Type 3")) {
              rankIndexShow = true
              RankIndex = bean45389_163.RankIndex

              if (RankIndex != null && RankIndex.contains("Rank ")) {
                Ri = RankIndex.split("Rank ")(1).toInt
              }
            }

            bean45389_163.CarrierIndex match {
              case "PCC" =>
                if (CQICW0Show) {
                  pccCQICW0 = bean45389_163.CQICW0.toInt
                }
                if (CQICW1Show) {
                  pccCQICW1 = bean45389_163.CQICW1.toInt
                }
                pccRi = Ri
              case "SCC1" =>
                if (CQICW0Show) {
                  scc1CQICW0 = bean45389_163.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc1CQICW1 = bean45389_163.CQICW1.toInt
                }
                scc1Ri = Ri
              case "SCC2" =>
                if (CQICW0Show) {
                  scc2CQICW0 = bean45389_163.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc2CQICW1 = bean45389_163.CQICW1.toInt
                }
                scc2Ri = Ri
              case "SCC3" =>
                if (CQICW0Show) {
                  scc3CQICW0 = bean45389_163.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc3CQICW1 = bean45389_163.CQICW1.toInt
                }
                scc3Ri = Ri
              case "SCC4" =>
                if (CQICW0Show) {
                  scc4CQICW0 = bean45389_163.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc4CQICW1 = bean45389_163.CQICW1.toInt
                }
                scc4Ri = Ri
              case "SCC5" =>
                if (CQICW0Show) {
                  scc5CQICW0 = bean45389_163.CQICW0.toInt
                }
                if (CQICW1Show) {
                  scc5CQICW1 = bean45389_163.CQICW1.toInt
                }
                scc5Ri = Ri
              case _ =>
            }

            logRecordQCommObj = logRecordQCommObj.copy(
              carrierIndex0xB14D = bean45389_163.CarrierIndex,
              cQICW0 = pccCQICW0,
              cQICW1 = pccCQICW1,
              cSFTxMode = bean45389_163.CSFTxMode,
              pUCCHReportType = bean45389_163.PUCCHReportType,
              rankIndexStr = RankIndex,
              startSystemFrameNumber = bean45389_163.StartSystemFrameNumber.toInt,
              startSystemSubframeNumber = bean45389_163.StartSystemSubframeNumber.toInt,
              scc1CQICW0 = scc1CQICW0,
              scc2CQICW0 = scc2CQICW0,
              scc3CQICW0 = scc3CQICW0,
              scc4CQICW0 = scc4CQICW0,
              scc5CQICW0 = scc5CQICW0,
              scc1CQICW1 = scc1CQICW1,
              scc2CQICW1 = scc2CQICW1,
              scc3CQICW1 = scc3CQICW1,
              scc4CQICW1 = scc4CQICW1,
              scc5CQICW1 = scc5CQICW1,
              pccRankIndex = pccRi,
              scc1RankIndex = scc1Ri,
              scc2RankIndex = scc2Ri,
              scc3RankIndex = scc3Ri,
              scc4RankIndex = scc4Ri,
              scc5RankIndex = scc5Ri)
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45389",
              hexLogCode = "0xB14D", logRecordQComm = logRecordQCommObj)

          case _ =>
            logRecord = logRecord.copy(logRecordName = "QCOMM", logCode = "45389", hexLogCode = "0xB14D", missingVersion = logVersion)
        }

    }
    logRecord
  }

  def getFilteredData(input:List[String], dataType:String)={
    var output: List[String] = input
    output = output.map(
      out => if(Constants.isValidNumeric(out, dataType)) out.toString
    ).collect {case n:String => n}
    output
  }

  def getKpisFrom61668(adbmsgjsonstr:String,deviceStartTime: ZonedDateTime):LogRecordQComm ={
    var logRecordQCommObj:LogRecordQComm = LogRecordQComm()
    var testCycleId = getJsonValueByKey(adbmsgjsonstr,"test_cycle_id")
    var deviceId = getJsonValueByKey(adbmsgjsonstr,"device_id")
    var rmTestName =  getJsonValueByKey(adbmsgjsonstr,"test_name")
    var rmTestState = getJsonValueByKey(adbmsgjsonstr,"test_state")
    var deviceTimeString =  getJsonValueByKey(adbmsgjsonstr,"timestamp")
    var M2MReceiveStart: String = ""
    var M2MReceiveEnd: String = ""
    var M2MCallStart: String = ""
    var M2MCallEnd: String = ""
    var TRANSMITStart: String = ""
    var TRANSMITEnd: String = ""
    var UDPECHOStart: String = ""
    var UDPECHOEnd: String = ""
    var SMSStart: String = ""
    var SMSEnd: String = ""
    var IdleStart: String = ""
    var IdleEnd: String = ""
    var DNSStart: String = ""
    var DNSEnd: String = ""
    var LDRStart: String = ""
    var LDREnd: String = ""
    var LDRSStart: String = ""
    var LDRSEnd: String = ""
    var UPLOADStart: String = ""
    var UPLOADEnd: String = ""
    var DOWNLOADStart: String = ""
    var DOWNLOADEnd: String = ""

    // Convert device timestamp to ZoneDateTime
    deviceTimeString = deviceTimeString.replace(" ", "T")
    val deviceTime = ZonedDateTime.parse(deviceTimeString + 'Z')

    // Assign KPI values according to the record's test name and status
    if(rmTestName.nonEmpty) {

      rmTestName match {
        case "M2MReceive" if rmTestState == "start" => M2MReceiveStart = "M2M Receive Start" + "_" + deviceTimeString
        case "M2MReceive" if rmTestState == "end" =>
          M2MReceiveEnd = "M2M Receive End" + "_" + deviceTimeString
        case "M2MCall" if rmTestState == "start" => M2MCallStart = "M2M Call Start" + "_" + deviceTimeString
        case "M2MCall" if rmTestState == "end" =>
          M2MCallEnd = "M2M Call End" + "_" + deviceTimeString
        case "TRANSMIT" if rmTestState == "start" => DOWNLOADStart = "DOWNLOAD Start" + "_" + deviceTimeString
        case "TRANSMIT" if rmTestState == "end" =>
          DOWNLOADEnd = "DOWNLOAD End" + "_" + deviceTimeString
        case "UDPECHO" if rmTestState == "start" => UDPECHOStart = "UPDECHO Start" + "_" + deviceTimeString
        case "UDPECHO" if rmTestState == "end" =>
          UDPECHOEnd = "UDPECHO End" + "_" + deviceTimeString
        case "SMS" if rmTestState == "start" => SMSStart = "SMS Start" + "_" + deviceTimeString
        case "SMS" if rmTestState == "end" =>
          SMSEnd = "SMS End" + "_" + deviceTimeString
        case "Idle" if rmTestState == "start" => IdleStart = "Idle Start" + "_" + deviceTimeString
        case "Idle" if rmTestState == "end" =>
          IdleEnd = "Idle End" + "_" + deviceTimeString
        case "DNS" if rmTestState == "start" => DNSStart = "DNS Start" + "_" + deviceTimeString
        case "DNS" if rmTestState == "end" =>
          DNSEnd = "DNS End" + "_" + deviceTimeString
        case "LDR" if rmTestState == "start" => LDRStart = "LDR Start" + "_" + deviceTimeString
        case "LDR" if rmTestState == "end" =>
          LDREnd = "LDR End" + "_" + deviceTimeString
        case "LDRS" if rmTestState == "start" => LDRSStart = "LDRS Start" + "_" + deviceTimeString
        case "LDRS" if rmTestState == "end" =>
          LDRSEnd = "LDRS End" + "_" + deviceTimeString
        case "UPLOAD" if rmTestState == "start" => UPLOADStart = "UPLOAD Start" + "_" + deviceTimeString
        case "UPLOAD" if rmTestState == "end" =>
          UPLOADEnd = "UPLOAD End" + "_" + deviceTimeString
        case _ =>
          logger.info("Entered into DEFAULT CASE for Root Metrics test name, 0xF0E4 with : " + rmTestName)
      }
      if(rmTestName=="TRANSMIT") rmTestName = "DOWNLOAD"
    }


    logRecordQCommObj = logRecordQCommObj.copy(
      TestCycleId = if(testCycleId.nonEmpty) testCycleId else null, DeviceId = if(deviceId.nonEmpty) deviceId else null,
      RMTestName = if(rmTestName.nonEmpty) rmTestName else null,RMTestState = if(rmTestState.nonEmpty) rmTestState else null,
      DeviceTime = if(deviceTimeString.nonEmpty) deviceTimeString else null,
      M2MReceiveStart = if(M2MReceiveStart.nonEmpty) M2MReceiveStart else null,
      M2MReceiveEnd = if(M2MReceiveEnd.nonEmpty) M2MReceiveEnd else null,
      M2MCallStart = if(M2MCallStart.nonEmpty) M2MCallStart else null,
      M2MCallEnd = if(M2MCallEnd.nonEmpty) M2MCallEnd else null,
      DOWNLOADStart = if(DOWNLOADStart.nonEmpty) DOWNLOADStart else null,
      DOWNLOADEnd = if(DOWNLOADEnd.nonEmpty) DOWNLOADEnd else null,
      UDPECHOStart = if(UDPECHOStart.nonEmpty) UDPECHOStart else null,
      UDPECHOEnd = if(UDPECHOEnd.nonEmpty) UDPECHOEnd else null,
      SMSStart = if(SMSStart.nonEmpty) SMSStart else null,
      SMSEnd = if(SMSEnd.nonEmpty) SMSEnd else null,
      IdleStart = if(IdleStart.nonEmpty) IdleStart else null,
      IdleEnd = if(IdleEnd.nonEmpty) IdleEnd else null,
      DNSStart = if(DNSStart.nonEmpty) DNSStart else null,
      DNSEnd = if(DNSEnd.nonEmpty) DNSEnd else null,
      LDRStart = if(LDRStart.nonEmpty) LDRStart else null,
      LDREnd = if(LDREnd.nonEmpty) LDREnd else null,
      LDRSStart = if(LDRSStart.nonEmpty) LDRSStart else null,
      LDRSEnd = if(LDRSEnd.nonEmpty) LDRSEnd else null,
      UPLOADStart = if(UPLOADStart.nonEmpty) UPLOADStart else null,
      UPLOADEnd = if(UPLOADEnd.nonEmpty) UPLOADEnd else null
    )

    logRecordQCommObj
  }



}
