package com.verizon.oneparser.parselogs

import java.sql.Timestamp

import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.ProcessLogCodes
import com.verizon.oneparser.common.{Constants, OPutil}
import com.verizon.oneparser.schema._
import org.apache.spark.sql.Row

import scala.collection.immutable.List

object LogRecordParserSIG extends ProcessLogCodes with LazyLogging {

  def parseLogRecordSIGSet1(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    var logRecord: LogRecord = parentlogRecord
    var logRecordQCommObj:LogRecordQComm = LogRecordQComm()
    var logRecordSigObj:LogRecordSIG = LogRecordSIG()
    import collection.JavaConverters._
    import collection.JavaConverters._
    import javax.xml.bind.DatatypeConverter
    //logger.info("Raw Payload before modifying for Timestamp : "+logRecord.dmTimeStamp+" ........"+OPutil.printHexBinary(x,true))
    logCode match {
      case 8449 =>
        val sigSeqRecord = x//OPutil.modifyLogRecordByteArrayToDropHeaderForHBFlex(x)
        val bean8449_1 = process8449_1(sigSeqRecord, exceptionUseArray)
        logRecordQCommObj = logRecordQCommObj.copy(Latitude = bean8449_1.Latitude, Longitude = bean8449_1.Longitude, hasGps = 1)
        logRecord = logRecord.copy(logRecordName = "SIG", version = logVersion, logCode = "8449", hexLogCode = "0x2101",  logRecordQComm = logRecordQCommObj )
      case 9227 =>
        val sigSeqRecord = x//OPutil.modifyLogRecordByteArrayToDropHeaderForHBFlex(x)
        val bean9227_1 = process9227_1(sigSeqRecord, exceptionUseArray)
        /* logger.info("9227 Raw Payload before modifying for Timestamp : "+logRecord.dmTimeStamp+" ........"+OPutil.printHexBinary(x,true)
           + ">>>>>>>>>>>>>" + "9227 Raw Payload After modifying........"+OPutil.printHexBinary(sigSeqRecord,true) +
           "JSON for File:"+logRecord.fileName + " at Index : "+logRecord.index+"==> "+ Constants.getJsonFromBean(bean9227_1))*/
        val cellId_SIGVal = bean9227_1.sr_101057_inst_list.asScala.toList.map(_.cellID)
        val RefPower_SIGVal = bean9227_1.sr_101057_inst_list.asScala.toList.map(_.RefPower)
        val RefQuality_SIGVal = bean9227_1.sr_101057_inst_list.asScala.toList.map(_.RefQuality)
        val CINR_SIGVal = bean9227_1.sr_101057_inst_list.asScala.toList.map(_.CINR)
        val channelNumber_SIGVal = bean9227_1.ChannelNumber
        val bandwidth_SIGVal = bean9227_1.Bandwidth

        var frequency240b:java.lang.Integer = if(bean9227_1.Frequency.toInt >0) bean9227_1.Frequency.toInt else null
        var cellId240b:List[String] = if(cellId_SIGVal.nonEmpty) cellId_SIGVal else null
        var RefPower240b:List[String] = if(RefPower_SIGVal.nonEmpty) RefPower_SIGVal else null
        var RefQuality240b:List[String] = if(RefQuality_SIGVal.nonEmpty)  RefQuality_SIGVal else null
        var CINR240b:List[String] = if(CINR_SIGVal.nonEmpty)  CINR_SIGVal else null
        var channelNumber240b:java.lang.Integer = if(bean9227_1.ChannelNumber.toInt >0) bean9227_1.ChannelNumber.toInt else null
        var freqkpi240bList:List[FreqKpi0x240bList] = null

        if(frequency240b != null) {
          freqkpi240bList = bean9227_1.Frequency.toList.map(x=>FreqKpi0x240bList(frequency240b, channelNumber240b, bandwidth_SIGVal, CINR240b,cellId240b,RefPower240b,RefQuality240b))
        }
        logRecordSigObj = logRecordSigObj.copy(frequency_SIG = bean9227_1.Frequency, freqKpiList0x240b = if (freqkpi240bList != null) freqkpi240bList.take(1) else freqkpi240bList, channelNumber_SIG = bean9227_1.ChannelNumber, bandwidth_SIG = bean9227_1.Bandwidth, numSignals_SIG = bean9227_1.numSignals,
          cellId_SIG = if (cellId_SIGVal.nonEmpty) cellId_SIGVal else null, RefPower_SIG = if (RefPower_SIGVal.nonEmpty) RefPower_SIGVal else null, RefQuality_SIG = if (RefQuality_SIGVal.nonEmpty) RefQuality_SIGVal else null ,CINR_SIG = if (CINR_SIGVal.nonEmpty) CINR_SIGVal else null)
        logRecord = logRecord.copy(logRecordName = "SIG", version = logVersion, logCode = "9227", hexLogCode = "0x240B",  logRecordSIG = logRecordSigObj)
      case 9222 =>
        val sigSeqRecord = x//OPutil.modifyLogRecordByteArrayToDropHeaderForHBFlex(x)
        // println("byte array >>>>>>>>>>>>>>>>>>>> "+ javax.xml.bind.DatatypeConverter.printHexBinary(sigSeqRecord))
        val bean9222_1 = process9222_1(sigSeqRecord, exceptionUseArray)
        // println("9222 JSON........."+Constants.getJsonFromBean(bean9222_1))
        val cellId_SIGVal = bean9222_1.sr_100907_inst_list.asScala.toList.map(_.CellID)
        val CINR_SIGVal = bean9222_1.sr_100907_inst_list.asScala.toList.map(_.CINR)
        val SecondarysyncQuality_SIGVal = bean9222_1.sr_100907_inst_list.asScala.toList.map(_.SecondarySyncQuality)
        val SecondarysyncPower_SIGVal = bean9222_1.sr_100907_inst_list.asScala.toList.map(_.SecondarySyncPower)
        val PrimarySyncQuality_SIGVal = bean9222_1.sr_100907_inst_list.asScala.toList.map(_.PrimarySyncQuality)
        val PrimarySyncPower_SIGVal = bean9222_1.sr_100907_inst_list.asScala.toList.map(_.PrimarySyncPower)
        var cpType = bean9222_1.sr_100907_inst_list.asScala.toList.map(_.cyclicPrefix)


        val frequency2406:java.lang.Integer = if(bean9222_1.frequencyMHz.toInt >0) bean9222_1.frequencyMHz.toInt else null
        val channelNumber2406:String = if(!bean9222_1.ChannelNum.isEmpty) bean9222_1.ChannelNum else null
        val cinr2406:List[String] = if (CINR_SIGVal.nonEmpty) CINR_SIGVal else null
        val cellId2406:List[String] = if (cellId_SIGVal.nonEmpty) cellId_SIGVal else null
        val PrimarySyncPower2406:List[String] = if (PrimarySyncPower_SIGVal.nonEmpty) PrimarySyncPower_SIGVal else null
        val primaryQuality2406: List[String] = if (PrimarySyncQuality_SIGVal.nonEmpty) PrimarySyncQuality_SIGVal else null
        val SecondarysyncPower2406: List[String] = if (SecondarysyncPower_SIGVal.nonEmpty) SecondarysyncPower_SIGVal else null
        val SecondarysyncQuality2406:List[String] = if (SecondarysyncQuality_SIGVal.nonEmpty) SecondarysyncQuality_SIGVal else null
        var cpType2406 = if(cpType.nonEmpty) cpType else null
        var freqKpiList: List[FreqKpi0x2406List] = null


        if(frequency2406 != null) {
          freqKpiList = bean9222_1.frequencyMHz.toList.map(x=>FreqKpi0x2406List(frequency2406, channelNumber2406,cinr2406,cellId2406,PrimarySyncPower2406,primaryQuality2406,SecondarysyncPower2406,SecondarysyncQuality2406,cpType2406))
        }

        logRecordSigObj = logRecordSigObj.copy(freqKpiList0x2406 = if (freqKpiList != null) freqKpiList.take(1) else freqKpiList, cpType0x2406_SIG = cpType2406,frequency0x2406_SIG = if(bean9222_1.frequencyMHz.toInt >0) bean9222_1.frequencyMHz else null,channelNumber0x2406_SIG = bean9222_1.ChannelNum, power0x2406_SIG = bean9222_1.Power, numSignals0x2406_SIG = bean9222_1.numSignals,
          cellId0x2406_SIG = if (cellId_SIGVal.nonEmpty) cellId_SIGVal else null,
          SecondarySyncQuality0x2406_SIG = if (SecondarysyncQuality_SIGVal.nonEmpty) SecondarysyncQuality_SIGVal else null,
          SecondarySyncPower0x2406_SIG = if (SecondarysyncPower_SIGVal.nonEmpty) SecondarysyncPower_SIGVal else null ,CINR0x2406_SIG = if (CINR_SIGVal.nonEmpty) CINR_SIGVal else null,
          PrimarySyncQuality0x2406_SIG = if (PrimarySyncQuality_SIGVal.nonEmpty) PrimarySyncQuality_SIGVal else null, PrimarySyncPower0x2406_SIG = if (PrimarySyncPower_SIGVal.nonEmpty) PrimarySyncPower_SIGVal else null)
        logRecord = logRecord.copy(logRecordName = "SIG", version = logVersion, logCode = "9222", hexLogCode = "0x2406",  logRecordSIG = logRecordSigObj)

      case 9224 =>
        /* val PhychanBitMaskVal = bean47236_65539.sr_12478_inst_list.asScala.toList.map(_.sr_12479_inst_list.asScala.toList.map(_.Phychan_Bit_Mask)).flatten*/
        val sigSeqRecord = x//OPutil.modifyLogRecordByteArrayToDropHeaderForHBFlex(x)
        // println("byte array >>>>>>>>>>>>>>>>>>>> "+ javax.xml.bind.DatatypeConverter.printHexBinary(sigSeqRecord))
        val bean9224_1 = process9224_1(sigSeqRecord, exceptionUseArray)
        // println("9222 JSON........."+Constants.getJsonFromBean(bean9222_1))
        val cellId_SIGVal = bean9224_1.sr_101018_inst_list.asScala.toList.map(_.PCI)
        val CINR_SIGVal = bean9224_1.sr_101018_inst_list.asScala.toList.map(_.CINR)
        val totalRsPower_SIGVal = bean9224_1.sr_101018_inst_list.asScala.toList.map(_.TotalRsPower)
        val rsrp_SIGVal = bean9224_1.sr_101018_inst_list.asScala.toList.map(_.RSRP)
        val rsrq_SIGVal = bean9224_1.sr_101018_inst_list.asScala.toList.map(_.RSRQ)
        val cpType_SIG = bean9224_1.sr_101018_inst_list.asScala.toList.map(_.cyclicPrefix)

        val totalPower_SIGVal = bean9224_1.sr_101018_inst_list.asScala.toList.map(_.sr_101032_inst_list.asScala.toList.map(_.TotalRsPowerAnt)).flatten
        val rsrpAnt_SIGVal = bean9224_1.sr_101018_inst_list.asScala.toList.map(_.sr_101032_inst_list.asScala.toList.map(_.RSRPAnt)).flatten
        val rsrqAnt_SIGVal = bean9224_1.sr_101018_inst_list.asScala.toList.map(_.sr_101032_inst_list.asScala.toList.map(_.RSRQAnt)).flatten
        val numberOfTxAntennaPorts0x2408_SIGVal = bean9224_1.sr_101018_inst_list.asScala.toList.map(_.numAntenna)
        val carrierRSSI0x2408_SIGVal = bean9224_1.ChannelPower

        logRecordSigObj = logRecordSigObj.copy(cpType0x2408_SIG=cpType_SIG,overallPower0x2408_SIG= rsrp_SIGVal, overallQuality0x2408_SIG=rsrq_SIGVal,
          antPower0x2408_SIG = rsrpAnt_SIGVal, antQuality0x2408_SIG = rsrqAnt_SIGVal, totalRsPower0x2408_SIG = if(totalRsPower_SIGVal.nonEmpty) totalRsPower_SIGVal else null,
          frequency0x2408_SIG = if(bean9224_1.Frequency.toInt>0) bean9224_1.Frequency else null, bandwidth0x2408_SIG = bean9224_1.bandwidth, numSignals0x2408_SIG = bean9224_1.numSignals,
          cellId0x2408_SIG = if (cellId_SIGVal.nonEmpty) cellId_SIGVal else null ,CINR0x2408_SIG = if (CINR_SIGVal.nonEmpty) CINR_SIGVal else null, numberOfTxAntennaPorts0x2408_SIG = numberOfTxAntennaPorts0x2408_SIGVal, carrierRSSI0x2408_SIG = carrierRSSI0x2408_SIGVal)
        logRecord = logRecord.copy(logRecordName = "SIG", version = logVersion, logCode = "9224", hexLogCode = "0x2408",  logRecordSIG = logRecordSigObj)

      case 9229 =>
        val sigSeqRecord = x//OPutil.modifyLogRecordByteArrayToDropHeaderForHBFlex(x)
        // println("byte array >>>>>>>>>>>>>>>>>>>> "+ javax.xml.bind.DatatypeConverter.printHexBinary(sigSeqRecord))
        val bean9229_2 = process9229_2(sigSeqRecord, exceptionUseArray)
        //println("bean9229_2 JSON........."+Constants.getJsonFromBean(bean9229_2))
        /* logger.info("9229(240D) Raw Payload before modifying for Timestamp : "+logRecord.dmTimeStamp+" ........"+OPutil.printHexBinary(x,true)
           + ">>>>>>>>>>>>>" + "9229(240D) Raw Payload After modifying........"+OPutil.printHexBinary(sigSeqRecord,true) +
           "JSON for File:"+logRecord.fileName + " at Index : "+logRecord.index+"==> "+ Constants.getJsonFromBean(bean9229_2))*/
        val frequency0x240D_SIGVal = bean9229_2.Frequency
        val channelNumber0x240D_SIGVal = bean9229_2.ChannelNumber
        val bandwidth0x240D_SIGVal = bean9229_2.Bandwidth
        val numSignals0x240D_SIGVal = bean9229_2.NumSignals
        val carrierRSSI0x240D_SIGVal = bean9229_2.carrierRSSI
        val PCI0x240D_SIGVal = bean9229_2.sr_101612_inst_list.asScala.toList.map(_.PCI)
        val RSRP0x240D_SIGVal = bean9229_2.sr_101612_inst_list.asScala.toList.map(_.RSRP)
        val RSRQ0x240D_SIGVal = bean9229_2.sr_101612_inst_list.asScala.toList.map(_.RSRQ)
        val CINR0x240D_SIGVal = bean9229_2.sr_101612_inst_list.asScala.toList.map(_.CINR)
        val numberOfTxAntennaPorts0x240D_SIGVal = bean9229_2.sr_101612_inst_list.asScala.toList.map(_.numberOfTxAntennaPorts)

        logRecordSigObj = logRecordSigObj.copy(frequency0x240D_SIG = frequency0x240D_SIGVal, channelNumber0x240D_SIG = channelNumber0x240D_SIGVal, bandwidth0x240D_SIG = bandwidth0x240D_SIGVal,
          numSignals0x240D_SIG = numSignals0x240D_SIGVal, carrierRSSI0x240D_SIG = carrierRSSI0x240D_SIGVal, PCI0x240D_SIG = PCI0x240D_SIGVal, RSRP0x240D_SIG = RSRP0x240D_SIGVal,
          RSRQ0x240D_SIG = RSRQ0x240D_SIGVal,CINR0x240D_SIG = CINR0x240D_SIGVal, numberOfTxAntennaPorts0x240D_SIG = numberOfTxAntennaPorts0x240D_SIGVal)
        logRecord = logRecord.copy(logRecordName = "SIG", version = logVersion, logCode = "9229", hexLogCode = "0x240D",  logRecordSIG = logRecordSigObj )

      case 9473 =>
        var logRecordQComm5GObj:LogRecordQComm5G = LogRecordQComm5G();
        logRecordQComm5GObj = logRecordQComm5GObj.copy(is5GTechnology = true)
        val sigSeqRecord = x//OPutil.modifyLogRecordByteArrayToDropHeaderForHBFlex(x)
        val bean9473 = process9473_1(sigSeqRecord, exceptionUseArray)
        /*  logger.info("9473(2501) Raw Payload before modifying for Timestamp : "+logRecord.dmTimeStamp+" ........"+OPutil.printHexBinary(x,true)
             + ">>>>>>>>>>>>>" + "9473(2501) Raw Payload After modifying........"+OPutil.printHexBinary(sigSeqRecord,true) +
             "JSON for File:"+logRecord.fileName + " at Index : "+logRecord.index+"==> "+ Constants.getJsonFromBean(bean9473))*/
        val channelNumber0x2501_SIGVal = bean9473.ChannelNumber
        val operatingBand0x2501_SIGVal = bean9473.OperatingBand
        val ssbRSSI0x2501_SIGVal = bean9473.SSBRSSI
        val numSSBurstDataBlocks0x2501_SIGVal = bean9473.NumberofSSBurstDataBlocks
        val PCI0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.CellID)
        val frequency0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.ChanFrequency)
        val subCarrierSpacing0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.SubCarrierSpacing)
        val numBeams0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.NumberofBeamDataBlocks)
        val beamIndex0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.sr_101583_inst_list.asScala.toList.map(_.BeamIndex)).flatten
        val pssBRSRP0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.sr_101583_inst_list.asScala.toList.map(_.PSSRPvalue)).flatten
        val pssBRSRQ0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.sr_101583_inst_list.asScala.toList.map(_.PSSRQvalue)).flatten
        val pssCINR0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.sr_101583_inst_list.asScala.toList.map(_.PSSCINRvalue)).flatten
        val sssBRSRP0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.sr_101583_inst_list.asScala.toList.map(_.SSSRPvalue)).flatten
        val sssBRSRQ0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.sr_101583_inst_list.asScala.toList.map(_.SSSRQvalue)).flatten
        val sssCINR0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.sr_101583_inst_list.asScala.toList.map(_.SSSCINRvalue)).flatten
        val sssDelay0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.sr_101583_inst_list.asScala.toList.map(_.SSSDelaySpread)).flatten
        val ssbBRSRP0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.sr_101583_inst_list.asScala.toList.map(_.SSBRPvalue)).flatten
        val ssbBRSRQ0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.sr_101583_inst_list.asScala.toList.map(_.SSBRQvalue)).flatten
        val ssbCINR0x2501_SIGVal = bean9473.sr_101573_inst_list.asScala.toList.map(_.sr_101583_inst_list.asScala.toList.map(_.SSCINRvalue)).flatten

        logRecordSigObj = logRecordSigObj.copy(channelNumber0x2501_SIG = channelNumber0x2501_SIGVal, operatingBand0x2501_SIG = operatingBand0x2501_SIGVal, ssbRSSI0x2501_SIG = ssbRSSI0x2501_SIGVal,
          numSSBurstDataBlocks0x2501_SIG = numSSBurstDataBlocks0x2501_SIGVal,PCI0x2501_SIG = PCI0x2501_SIGVal, frequency0x2501_SIG = frequency0x2501_SIGVal, subCarrierSpacing0x2501_SIG = subCarrierSpacing0x2501_SIGVal,
          numBeams0x2501_SIG = numBeams0x2501_SIGVal, beamIndex0x2501_SIG = beamIndex0x2501_SIGVal, pssBRSRP0x2501_SIG = pssBRSRP0x2501_SIGVal, pssBRSRQ0x2501_SIG = pssBRSRP0x2501_SIGVal, pssCINR0x2501_SIG = pssCINR0x2501_SIGVal,
          sssBRSRP0x2501_SIG = sssBRSRP0x2501_SIGVal, sssBRSRQ0x2501_SIG = ssbBRSRQ0x2501_SIGVal, sssCINR0x2501_SIG = sssCINR0x2501_SIGVal, sssDelay0x2501_SIG = sssDelay0x2501_SIGVal,ssbBRSRP0x2501_SIG = ssbBRSRP0x2501_SIGVal,
          ssbBRSRQ0x2501_SIG = ssbBRSRQ0x2501_SIGVal,ssbCINR0x2501_SIG = ssbCINR0x2501_SIGVal)
        logRecord = logRecord.copy(logRecordName = "SIG", version = logVersion, logCode = "9473", hexLogCode = "0x2501",  logRecordSIG = logRecordSigObj, logRecordQComm5G = logRecordQComm5GObj )
      /*case 9473 =>
        val sigSeqRecord = x//OPutil.modifyLogRecordByteArrayToDropHeaderForHBFlex(x)
        val bean9473_1 = process9473_1(sigSeqRecord, exceptionUseArray)
        println("Get 9473 logcode KPIs")
        println("9473 JSON........."+Constants.getJsonFromBean(bean9473_1))*/
      case 8535 =>
        def topN(n: Int, list: List[FreqRsrp]): List[FreqRsrp] = {
          def update(l: List[FreqRsrp], e: FreqRsrp): List[FreqRsrp] = {
            if (e.rsrp > l.head.rsrp) (e :: l.tail).sortWith(_.rsrp < _.rsrp) else l
          }
          list.drop(n).foldLeft( list.take(n).sortWith(_.rsrp < _.rsrp) )( update )
        }
        val sigSeqRecord = x//OPutil.modifyLogRecordByteArrayToDropHeaderForHBFlex(x)
        val bean8535 = process8535_1(sigSeqRecord, exceptionUseArray)
        import collection.mutable._
        val freqRsrpList = bean8535.sr_103903_inst_list.asScala.toList.map(x=>FreqRsrp(x.Frequency.toInt,x.Power.toDouble))
        val freqRsrp = topN(20,freqRsrpList)
        logRecordSigObj = logRecordSigObj.copy(frequencyRSRP0x2157_SIG=freqRsrp)
        logRecord = logRecord.copy(logRecordName = "SIG", version = logVersion, logCode = "8535", hexLogCode = "0x2157",  logRecordSIG = logRecordSigObj)

      case 9228 =>
        val sigSeqRecord = x//OPutil.modifyLogRecordByteArrayToDropHeaderForHBFlex(x)
        // println("byte array >>>>>>>>>>>>>>>>>>>> "+ javax.xml.bind.DatatypeConverter.printHexBinary(sigSeqRecord))
        val bean9228_2 = process9228_1(sigSeqRecord, exceptionUseArray)
        //         logger.info("9228(240C) Raw Payload before modifying for Timestamp : "+logRecord.dmTimeStamp+" ........"+OPutil.printHexBinary(x,true)
        //           + ">>>>>>>>>>>>>" + "9228(240C) Raw Payload After modifying........"+OPutil.printHexBinary(sigSeqRecord,true) +
        //           "JSON for File:"+logRecord.fileName + " at Index : "+logRecord.index+"==> "+ Constants.getJsonFromBean(bean9228_2))
        val centerFreq0x240C: java.lang.Integer = if(Constants.isValidNumeric(bean9228_2.CenterFrequency, "float")) bean9228_2.CenterFrequency.toFloat.toInt else null
        val numSignals0x240C:java.lang.Integer = if(bean9228_2.NumSignals.toInt >0) bean9228_2.NumSignals.toInt else null
        val pci0x240C_SIGVal:List[String] = bean9228_2.sr_46158_inst_list.asScala.toList.map(_.PhysicalCellId)
        val rsrp0x240C_SIGVal = bean9228_2.sr_46158_inst_list.asScala.toList.map(_.RSRP)
        val rsrqx240C_SIGVal = bean9228_2.sr_46158_inst_list.asScala.toList.map(_.RSRQ)
        val cinr0x240C_SIGVal = bean9228_2.sr_46158_inst_list.asScala.toList.map(_.rsCINR)
        val numberOfAntennas_SIGVal = bean9228_2.sr_46158_inst_list.asScala.toList.map(_.NumberOfTxAntennas)

        val antRsrp0x240C_SIGVal = bean9228_2.sr_46158_inst_list.asScala.toList.map(_.sr_46173_inst_list.asScala.toList.map(_.RSRPAnt)).flatten
        val antRsrqx240C_SIGVal = bean9228_2.sr_46158_inst_list.asScala.toList.map(_.sr_46173_inst_list.asScala.toList.map(_.RSRQAnt)).flatten
        val antCinr0x240C_SIGVal = bean9228_2.sr_46158_inst_list.asScala.toList.map(_.sr_46173_inst_list.asScala.toList.map(_.CINRAnt)).flatten

        var pciKpiList: List[PciKpi0x240CList] = null

        if(centerFreq0x240C !=null) {
          pciKpiList = bean9228_2.CenterFrequency.toList.map(x=>PciKpi0x240CList(centerFreq0x240C, numSignals0x240C,pci0x240C_SIGVal,rsrp0x240C_SIGVal,
            rsrqx240C_SIGVal,cinr0x240C_SIGVal,numberOfAntennas_SIGVal,antRsrp0x240C_SIGVal,antRsrqx240C_SIGVal,antCinr0x240C_SIGVal))
        }
        logRecordSigObj = logRecordSigObj.copy(pciKpi0x240C_SIG = if (pciKpiList != null) pciKpiList.take(1) else pciKpiList)
        logRecord = logRecord.copy(logRecordName = "SIG", version = logVersion, logCode = "9228", hexLogCode = "0x240C",logRecordSIG = logRecordSigObj)

    }
    logRecord
  }

}
