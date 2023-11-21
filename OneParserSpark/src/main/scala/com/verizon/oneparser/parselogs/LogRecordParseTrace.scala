package com.verizon.oneparser.parselogs

import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.ProcessLogCodes
import com.verizon.oneparser.parselogs.LogRecordParser5G.{parseDouble, parseFloat, parseInteger}
import com.verizon.oneparser.schema.{LogRecord, LogRecordTrace, PctelLTEBlindScanList, PctelLTETopNsignalList, PctelNRBlindScanList, PctelNRTopNSignalKpis}
import net.liftweb.json.JsonAST._
import net.liftweb.json.{JsonAST, parse}

import scala.collection.immutable.List


object LogRecordParseTrace extends ProcessLogCodes with LazyLogging {

  def parseLogRecordTraceSet(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    var logRecord: LogRecord = parentlogRecord
    var logRecordTraceObj: LogRecordTrace = LogRecordTrace()
    logCode match {
      case 65524 =>
        val bean65524 = process65524(x, exceptionUseArray).toString
        //        logger.info("bean65524 -> Bean as JSON>>>>>>>>" + Constants.getJsonFromBean(bean65524))
        val EtopNSignal = "etopNSignal".toUpperCase
        val BlindScan = "blindScan".toUpperCase
        val NrTopNSignal = "nrTopNSignal".toUpperCase
        var scanMessage: String = ""

        try {
          scanMessage = getJsonValueByKey(bean65524, "Scan Message Name")
        } catch {
          case ge: Exception =>
            logger.error("0xFFF4(65524) : Error while parsing Trace Payload for timestamp : " + logRecord.dmTimeStamp)
          case _: Throwable =>
            logger.error("0xFFF4(65524) : Error while parsing Trace Payload for timestamp : " + logRecord.dmTimeStamp);
        }

        scanMessage.toUpperCase match {
          case EtopNSignal =>
            val refDataJson: String = getSubJsonValueByKey(bean65524, "refDataBlock")
            var frequency: String = getJsonValueByKey(bean65524, "channelFrequency")
            var band: String = getSubJsonValueByKey(bean65524, "Band")
            var channelNumber: String = getSubJsonValueByKey(bean65524, "Channel Number")
            var lteTopNSignalList: List[PctelLTETopNsignalList] = List.empty

            if (refDataJson.nonEmpty) {
              var subjsonStr = parse(getSubJsonValueByKey(refDataJson, "dataBlocks"))
              if (subjsonStr != null) {
                var cellrank = for {JArray(objList) <- subjsonStr; JObject(obj) <- objList; JField("cellRank", JString(cellrank)) <- obj} yield (cellrank)
                var cellid = for {JArray(objList) <- subjsonStr; JObject(obj) <- objList; JField("cellId", JInt(cellid)) <- obj} yield (cellid.toString)
                var rsrp = for {JArray(objList) <- subjsonStr; JObject(obj) <- objList; JField("rs-rp", JString(rsrp)) <- obj} yield (rsrp)
                var rsrq = for {JArray(objList) <- subjsonStr; JObject(obj) <- objList; JField("rs-rq", JString(rsrq)) <- obj} yield (rsrq)
                var rscinr = for {JArray(objList) <- subjsonStr; JObject(obj) <- objList; JField("rs-cinr", JString(rscinr)) <- obj} yield (rscinr)

                if (frequency != null) {
                  lteTopNSignalList = PctelLTETopNsignalList(parseDouble(frequency).toInt, channelNumber.toInt, band, rscinr, cellid, rsrp, rsrq) :: lteTopNSignalList
                }
              }
            }

            logRecordTraceObj = logRecordTraceObj.copy(lteTopNSignalkpiList = lteTopNSignalList)
            logRecord = logRecord.copy(logRecordName = "SIG", version = logVersion, logCode = "65524", hexLogCode = "0xFFF4", logRecordTrace = logRecordTraceObj)

          case BlindScan =>
            val band: String = getSubJsonValueByKey(bean65524, "Band")
            val protocol: String = getSubJsonValueByKey(bean65524, "protocol").toUpperCase
            var lteBlindScanList: List[PctelLTEBlindScanList] = List.empty
            var nrBlindScanList: List[PctelNRBlindScanList] = List.empty
            val blindScanChannelList = parse(getSubJsonValueByKey(bean65524, "blindScanChannelList"))
            //            logger.info(s"protocol=$protocol blindScanChannelList=$blindScanChannelList ")

            if (protocol.contains("LTE") && blindScanChannelList != null) {
              val objList = for {JArray(objList) <- blindScanChannelList} yield objList

              for (JObject(obj) <- objList.head) {
                val channelNumber = for {JField("Channel Number", JInt(channelNumber)) <- obj} yield channelNumber
                val frequency = for {JField("channelFrequency", JString(frequency)) <- obj} yield frequency
                val numCells = for {JField("numCells", JInt(numCells)) <- obj} yield numCells
                val cellRank = for {JField("ids", JArray(ids)) <- obj; JObject(obj1) <- ids; JField("cellRank", JInt(cellRank)) <- obj1} yield cellRank
                val cellId = for {JField("ids", JArray(ids)) <- obj; JObject(obj1) <- ids; JField("id", JInt(cellId)) <- obj1} yield cellId.toString
                val rsrq = for {JField("ids", JArray(ids)) <- obj; JObject(obj1) <- ids; JField("lte", JObject(lte)) <- obj1; JField("rs-rq", JDouble(rsrq)) <- lte} yield rsrq.toString
                val rsrp = for {JField("ids", JArray(ids)) <- obj; JObject(obj1) <- ids; JField("lte", JObject(lte)) <- obj1; JField("rs-rp", JDouble(rsrp)) <- lte} yield rsrp.toString
                val cinr = for {JField("ids", JArray(ids)) <- obj; JObject(obj1) <- ids; JField("lte", JObject(lte)) <- obj1; JField("rs-cinr", JDouble(cinr)) <- lte} yield cinr.toString
                //                 logger.info(s"protocol=$protocol channelNumber=$channelNumber frequency=$frequency cellId=$cellId rsrq=$rsrq rsrp=$rsrp cinr=$cinr")

                lteBlindScanList = PctelLTEBlindScanList(parseDouble(frequency.head).toInt, channelNumber.head.toInt, band, cinr, cellId, rsrp, rsrq) :: lteBlindScanList
                //                 logger.info(s"lteBlindScanList2=$lteBlindScanList")
              }
            }

            if (protocol.contains("NR") && blindScanChannelList != null) {
              val objList = for {JArray(objList) <- blindScanChannelList} yield objList

              for (JObject(obj) <- objList.head) {
                val channelNumber = for {JField("Channel Number", JInt(channelNumber)) <- obj} yield channelNumber
                val frequency = for {JField("channelFrequency", JString(frequency)) <- obj} yield frequency
                val numCells = for {JField("numCells", JInt(numCells)) <- obj} yield numCells
                val cellRank = for {JField("ids", JArray(ids)) <- obj; JObject(obj1) <- ids; JField("cellRank", JInt(cellRank)) <- obj1} yield cellRank
                val cellId = for {JField("ids", JArray(ids)) <- obj; JObject(obj1) <- ids; JField("id", JInt(cellId)) <- obj1} yield cellId.toString
                val rsrq = for {JField("ids", JArray(ids)) <- obj; JObject(obj1) <- ids; JField("nr", JObject(nr)) <- obj1; JField("beamDataBlocks", JArray(bdb)) <- nr; JObject(obj2) <- bdb; JField("pss-rq", JString(rsrq)) <- obj2} yield rsrq
                val rsrp = for {JField("ids", JArray(ids)) <- obj; JObject(obj1) <- ids; JField("nr", JObject(nr)) <- obj1; JField("beamDataBlocks", JArray(bdb)) <- nr; JObject(obj2) <- bdb; JField("pss-rp", JString(rsrp)) <- obj2} yield rsrp
                val cinr = for {JField("ids", JArray(ids)) <- obj; JObject(obj1) <- ids; JField("nr", JObject(nr)) <- obj1; JField("beamDataBlocks", JArray(bdb)) <- nr; JObject(obj2) <- bdb; JField("pss-cinr", JString(cinr)) <- obj2} yield cinr

                nrBlindScanList = PctelNRBlindScanList(parseDouble(frequency.head).toInt, channelNumber.head.toInt, band, cinr, cellId, rsrp, rsrq) :: nrBlindScanList
              }
            }

            logRecordTraceObj = logRecordTraceObj.copy(lteBlindScanKpiList = lteBlindScanList, nrBlindScanKpiList = nrBlindScanList)
            logRecord = logRecord.copy(logRecordName = "SIG", version = logVersion, logCode = "65524", hexLogCode = "0xFFF4", logRecordTrace = logRecordTraceObj)

          case NrTopNSignal =>
            val band = getJsonValueByKey(bean65524, "Band")
            val channelNumber = getJsonValueByKey(bean65524, "Channel Number")
            val frequency = getJsonValueByKey(bean65524, "channelFrequency")
            val numCells = getJsonValueByKey(bean65524, "numCells")
            val ssbRssi = getJsonValueByKey(bean65524, "ssbRssi")
            var pctelNRTopNSignal0: PctelNRTopNSignalKpis = null
            var pctelNRTopNSignal1: PctelNRTopNSignalKpis = null

            val content = parse(getSubJsonValueByKey(bean65524, "Content: "))
            //            logger.info(s"content=$content ")
            if (content != null) {
              val signalList = for {JObject(ol) <- content; JField("ssBurstDataBlocks", JArray(ssbdb)) <- ol} yield ssbdb
              if (signalList.head.nonEmpty) {
                pctelNRTopNSignal0 = parseSignal(signalList.head(0), 0)
              }
              if (signalList.head.size > 1) {
                pctelNRTopNSignal1 = parseSignal(signalList.head(1), 1)
              }

              def parseSignal(obj: JsonAST.JValue, number: Integer) = {
                val subCarrierSpacing = for {JObject(obj1) <- obj; JField("subcarrierSpacing", JInt(subCarrierSpacing)) <- obj1} yield subCarrierSpacing
                val pssRsrq = for {JObject(obj1) <- obj; JField("beamDataBlocks", JArray(bdb)) <- obj1; JObject(obj2) <- bdb; JField("pss-rq", JString(pssRsrq)) <- obj2} yield pssRsrq
                val pssRsrp = for {JObject(obj1) <- obj; JField("beamDataBlocks", JArray(bdb)) <- obj1; JObject(obj2) <- bdb; JField("pss-rp", JString(pssRsrp)) <- obj2} yield pssRsrp
                val pssCinr = for {JObject(obj1) <- obj; JField("beamDataBlocks", JArray(bdb)) <- obj1; JObject(obj2) <- bdb; JField("pss-cinr", JString(pssCinr)) <- obj2} yield pssCinr
                val sssRsrq = for {JObject(obj1) <- obj; JField("beamDataBlocks", JArray(bdb)) <- obj1; JObject(obj2) <- bdb; JField("sss-rq", JString(sssRsrq)) <- obj2} yield sssRsrq
                val sssRsrp = for {JObject(obj1) <- obj; JField("beamDataBlocks", JArray(bdb)) <- obj1; JObject(obj2) <- bdb; JField("sss-rp", JString(sssRsrp)) <- obj2} yield sssRsrp
                val sssCinr = for {JObject(obj1) <- obj; JField("beamDataBlocks", JArray(bdb)) <- obj1; JObject(obj2) <- bdb; JField("sss-cinr", JString(sssCinr)) <- obj2} yield sssCinr
                val ssbRsrq = for {JObject(obj1) <- obj; JField("beamDataBlocks", JArray(bdb)) <- obj1; JObject(obj2) <- bdb; JField("ssb-rq", JString(ssbRsrq)) <- obj2} yield ssbRsrq
                val ssbRsrp = for {JObject(obj1) <- obj; JField("beamDataBlocks", JArray(bdb)) <- obj1; JObject(obj2) <- bdb; JField("ssb-rp", JString(ssbRsrp)) <- obj2} yield ssbRsrp
                val ssbCinr = for {JObject(obj1) <- obj; JField("beamDataBlocks", JArray(bdb)) <- obj1; JObject(obj2) <- bdb; JField("ssb-cinr", JString(ssbCinr)) <- obj2} yield ssbCinr
                val cellId = for {JObject(obj1) <- obj; JField("cellId", JInt(cellId)) <- obj1} yield cellId.toString()
                val beamIndex = for {JObject(obj1) <- obj; JField("beamDataBlocks", JArray(bdb)) <- obj1; JObject(obj2) <- bdb; JField("beamIndex", JInt(pssRsrq)) <- obj2} yield pssRsrq.toString()

                PctelNRTopNSignalKpis(signalNumber = number, freq = parseDouble(frequency).toInt,
                  channelNumber = parseInteger(channelNumber), numCells = parseInteger(numCells), ssbRssi = parseDouble(ssbRssi),
                  band = band, subCarrierSpacing = subCarrierSpacing.head.toInt,
                  pssCinr = pssCinr.take(3), pssRsrp = pssRsrp.take(3), pssRsrq = pssRsrq.take(3),
                  sssCinr = sssCinr.take(3), sssRsrp = sssRsrp.take(3), sssRsrq = sssRsrq.take(3),
                  ssbCinr = ssbCinr.take(3), ssbRsrp = ssbRsrp.take(3), ssbRsrq = ssbRsrq.take(3),
                  cellId = cellId.take(2), beamIndex = beamIndex.take(3))
              }
            }

            var nrTopNSignalKpiList: List[PctelNRTopNSignalKpis] = List()
            nrTopNSignalKpiList = if (pctelNRTopNSignal0 != null) nrTopNSignalKpiList :+ pctelNRTopNSignal0 else nrTopNSignalKpiList
            nrTopNSignalKpiList = if (pctelNRTopNSignal1 != null) nrTopNSignalKpiList :+ pctelNRTopNSignal1 else nrTopNSignalKpiList
            logRecordTraceObj = logRecordTraceObj.copy(nrTopNSignalKpiList = nrTopNSignalKpiList)
//            logger.info(s"pctelNRTopNSignal0=$pctelNRTopNSignal0  pctelNRTopNSignal1=$pctelNRTopNSignal1")
            logRecord = logRecord.copy(logRecordName = "SIG", version = logVersion, logCode = "65524", hexLogCode = "0xFFF4", logRecordTrace = logRecordTraceObj)

          case _ =>
            // logger.info("0xFFF4 65524 JSON>>>>>>>>>>>>>>>>>>"+bean65524)
            logRecord = logRecord.copy(logRecordName = "SIG", logCode = "65524", hexLogCode = " 0xFFF4", missingScanMessage = scanMessage)
        }
    }

    logRecord
  }
}
