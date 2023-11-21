package com.verizon.oneparser.parselogs

import com.verizon.oneparser.ProcessLogCodes
import com.verizon.oneparser.schema._
import org.apache.spark.sql.Row

import scala.collection.immutable.List

object LogRecordParserNAS extends ProcessLogCodes {

  def parseLogRecordNASSet1(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {

    var logRecord: LogRecord = parentlogRecord
    var logRecordNASObj: LogRecordNAS = LogRecordNAS()
    logCode match{
      case 45282 =>
        val bean45282 = process45282(x, exceptionUseArray)
        //logger.info("NAS JSON 0xB0E2>>>>>>>>>"+bean45282.toString)
        val msgType: String = getJsonValueByKey(bean45282.toString, "NAS Msg Type Str")
        //logger.info("NASMSG 0xB0E2:: " + msgType.toUpperCase)

        msgType.toUpperCase match {
          case "ACTIVATE DEFAULT EPS BEARER CONTEXT REQUEST" =>
            val contextTypeVal = "Default"
            val lteActDefBerContRequest = 1
            //logger.info("0xB0E2 Qci: " + getJsonValueByKey(bean45282.toString, "qci"))
            val qciExtracted = getJsonValueByKey(bean45282.toString, "qci")
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, epsMobileIdType = getJsonValueByKey(bean45282.toString, "EPS Bearer Id"),
              pdnApnName = getJsonValueByKey(bean45282.toString, "num_acc_pt_val"), contextType = contextTypeVal, qci = if(qciExtracted.isEmpty) null else qciExtracted.toString,
              linkedEpsBearerId = getJsonValueByKey(bean45282.toString, "Linked EPS bearer identity"), lteActDefBerContReq = lteActDefBerContRequest)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45282",
              hexLogCode = "0xB0E2", logRecordNAS = logRecordNASObj)
          case "ACTIVATE DEDICATED EPS BEARER CONTEXT REQUEST" =>
            val contextTypeVal = "Dedicated"
            val lteActDedBerContRequest = 1
            //logger.info("0xB0E2 Qci: " + getJsonValueByKey(bean45282.toString, "qci"))
            val qciExtracted = getJsonValueByKey(bean45282.toString, "qci")
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, epsMobileIdType = getJsonValueByKey(bean45282.toString, "EPS Bearer Id"),
              pdnApnName = getJsonValueByKey(bean45282.toString, "num_acc_pt_val"), contextType = contextTypeVal, qci = if(qciExtracted.isEmpty) null else qciExtracted.toString,
              linkedEpsBearerId = getJsonValueByKey(bean45282.toString, "Linked EPS bearer identity"), lteActDedBerContReq = lteActDedBerContRequest)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45282",
              hexLogCode = "0xB0E2", logRecordNAS = logRecordNASObj)
          case "PDN CONNECTIVITY REJECT" =>
            val esmCauseVal: String = getJsonValueByKey(bean45282.toString, "ESM cause")
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, esmCause = esmCauseVal, pdnReqType = getJsonValueByKey(bean45282.toString, "Request type"),
              pdnType = getJsonValueByKey(bean45282.toString, "PDN type"), pdnApnName = getJsonValueByKey(bean45282.toString, "num_acc_pt_val"),ltePdnRej =1)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45282",
              hexLogCode = "0xB0E2", logRecordNAS = logRecordNASObj)
          case "DEACTIVATE EPS BEARER CONTEXT REQUEST" =>
            val lteDeactBerContRequest = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, epsMobileIdType = getJsonValueByKey(bean45282.toString, "EPS Bearer Id"),
              lteDeactBerContReq = lteDeactBerContRequest)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45282",
              hexLogCode = "0xB0E2", logRecordNAS = logRecordNASObj)
          case "ESM INFORMATION REQUEST" =>
            val lteEsmInfoRequest = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase,
              lteEsmInfoReq = lteEsmInfoRequest)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45282",
              hexLogCode = "0xB0E2", logRecordNAS = logRecordNASObj)
          case "MODIFY EPS BEARER CONTEXT REQUEST" =>
            val lteModifyEpsRequest = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase,
              lteEsmModifyEpsReq = lteModifyEpsRequest)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45282",
              hexLogCode = "0xB0E2", logRecordNAS = logRecordNASObj)
          case _ =>
            logger.info("Entered into DEFAULT CASE for NAS, 0xB0E2 with : " + msgType.toUpperCase)
        }
      //logger.info("NAS JSON output>>>>>>>>>>>>>>>>>>>>>>"+bean45282.toString)
      case 45283 =>
        val bean45283 = process45283(x, exceptionUseArray)
        //logger.info("NAS JSON 0xB0E3>>>>>>>>>"+bean45283.toString)
        val msgType: String = getJsonValueByKey(bean45283.toString, "NAS Msg Type Str")
        //logger.info("NASMSG 0xB0E3:: " + msgType.toUpperCase)
        msgType.toUpperCase match {
          case "PDN CONNECTIVITY REQUEST" =>
            val ltePdnConntRequest = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, pdnReqType = getJsonValueByKey(bean45283.toString, "Request type"),
              pdnType = getJsonValueByKey(bean45283.toString, "PDN type"), pdnApnName = getJsonValueByKey(bean45283.toString, "num_acc_pt_val"),
              ltePdnConntReq = ltePdnConntRequest)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45283",
              hexLogCode = "0xB0E3", logRecordNAS = logRecordNASObj)
          case "ESM INFORMATION RESPONSE" =>
            val lteEsmInfoResuest = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, pdnReqType = getJsonValueByKey(bean45283.toString, "Request type"),
              pdnType = getJsonValueByKey(bean45283.toString, "PDN type"), pdnApnName = getJsonValueByKey(bean45283.toString, "num_acc_pt_val"),
              lteEsmInfoRes = lteEsmInfoResuest)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45283",
              hexLogCode = "0xB0E3", logRecordNAS = logRecordNASObj)
          case "DEACTIVATE EPS BEARER CONTEXT ACCEPT" =>
            val lteDeactContAccept = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, pdnReqType = getJsonValueByKey(bean45283.toString, "Request type"),
              pdnType = getJsonValueByKey(bean45283.toString, "PDN type"), epsMobileIdType = getJsonValueByKey(bean45283.toString, "EPS Bearer Id"),
              lteDeactContAcpt = lteDeactContAccept)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45283",
              hexLogCode = "0xB0E3", logRecordNAS = logRecordNASObj)
          case "ACTIVATE DEFAULT EPS BEARER CONTEXT ACCEPT" =>
            val lteActDefContAccept = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase,
              lteActDefContAcpt = lteActDefContAccept)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45283",
              hexLogCode = "0xB0E3", logRecordNAS = logRecordNASObj)
          case "ACTIVATE DEDICATED EPS BEARER CONTEXT ACCEPT" =>
            val lteActDedContAccept = 1
            logRecordNASObj = logRecordNASObj.copy( NAS_MsgType = msgType.toUpperCase,
              lteActDedContAcpt = lteActDedContAccept)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45283",
              hexLogCode = "0xB0E3", logRecordNAS = logRecordNASObj)
          case "PDN DISCONNECT REQUEST" =>
            val ltePdnDisconRequest = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase,
              ltePdnDisconReq = ltePdnDisconRequest)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45283",
              hexLogCode = "0xB0E3", logRecordNAS = logRecordNASObj)
          case "MODIFY EPS BEARER CONTEXT ACCEPT" =>
            val lteEsmModifyEpsAccptMsg = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase,
              lteEsmModifyEpsAccpt = lteEsmModifyEpsAccptMsg)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45283",
              hexLogCode = "0xB0E3", logRecordNAS = logRecordNASObj)
          case "MODIFY EPS BEARER CONTEXT REJECT" =>
            val lteEsmModifyEpsRejMsg = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase,
              lteEsmModifyEpsRej = lteEsmModifyEpsRejMsg)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45283",
              hexLogCode = "0xB0E3", logRecordNAS = logRecordNASObj)
          case "ACTIVATE DEDICATED EPS BEARER CONTEXT REJECT" =>
            val lteActDedContRejMsg = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase,
              lteActDedContRej = lteActDedContRejMsg)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45283",
              hexLogCode = "0xB0E3", logRecordNAS = logRecordNASObj)
          case "ACTIVATE DEFAULT EPS BEARER CONTEXT REJECT" =>
            val lteActDefContRejMsg = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase,
              lteActDefContRej = lteActDefContRejMsg)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45283",
              hexLogCode = "0xB0E3", logRecordNAS = logRecordNASObj)
          case _ =>
            logger.info("Entered into DEFAULT CASE for NAS, 0xB0E3 with : " + msgType.toUpperCase)
        }
      case 45292 =>
        val bean45292 = process45292(x, exceptionUseArray)
        //logger.info("NAS JSON 0xB0EC>>>>>>>>>"+bean45292.toString)
        val msgType: String = getJsonValueByKey(bean45292.toString, "NAS Msg Type Str")
        //logger.info("NASMSG 0xB0EC:: " + msgType.toUpperCase)
        msgType.toUpperCase match {
          case "ATTACH ACCEPT" =>
            val lteAttAccept = 1
            var vopsVal = "Not Supported"
            if (getJsonValueByKey(bean45292.toString, "IMS VoPS") == "1")
              vopsVal = "Supported"
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, voPSStatus = vopsVal, lteAttAcpt = lteAttAccept)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45292",
              hexLogCode = "0xB0EC", logRecordNAS = logRecordNASObj)
          case "TRACKING AREA UPDATE ACCEPT" =>
            val lteTaAccept = 1
            var vopsVal = "Not Supported"
            if (getJsonValueByKey(bean45292.toString, "IMS VoPS") == "1")
              vopsVal = "Supported"
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, voPSStatus = vopsVal, lteTaAcpt = lteTaAccept)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45292",
              hexLogCode = "0xB0EC", logRecordNAS = logRecordNASObj)
          case "SERVICE REJECT" =>
            val emmCause: String = getJsonValueByKey(bean45292.toString, "EMM cause")
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, servRejCause = emmCause, lteServiceRej=1)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45292",
              hexLogCode = "0xB0EC", logRecordNAS = logRecordNASObj)
          case "TRACKING AREA REJECT" =>
            val emmCause: String = getJsonValueByKey(bean45292.toString, "EMM cause")
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, emmTaRejCause = emmCause, lteTaRej=1)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45292",
              hexLogCode = "0xB0EC", logRecordNAS = logRecordNASObj)
          case "ATTACH REJECT" =>
            val emmCause: String = getJsonValueByKey(bean45292.toString, "EMM cause")
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, attRejCause = emmCause, lteAttRej = 1)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45292",
              hexLogCode = "0xB0EC", logRecordNAS = logRecordNASObj)
          case "AUTHENTICATION REJECT" =>
            val emmCause: String = getJsonValueByKey(bean45292.toString, "EMM cause")
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, attRejCause = emmCause, lteAuthRej = 1)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45292",
              hexLogCode = "0xB0EC", logRecordNAS = logRecordNASObj)
          case "EMM INFORMATION" =>
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, lteEmmInfo = 1)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45292",
              hexLogCode = "0xB0EC", logRecordNAS = logRecordNASObj)
          case _ =>
            logger.info("Entered into DEFAULT CASE for NAS, 0xB0EC with : " + msgType.toUpperCase)
        }
      case 45293 =>
        val bean45293 = process45293(x, exceptionUseArray)
        //logger.info("NAS JSON 0xB0ED>>>>>>>>>"+bean45293.toString)
        val msgType: String = getJsonValueByKey(bean45293.toString, "NAS Msg Type Str")
        //logger.info("NASMSG 0xB0ED:: " + msgType.toUpperCase)
        msgType.toUpperCase match {
          case "ATTACH REQUEST" =>
            val lteAttRequest = 1
            //                  logger.info("NAS JSON 0xB0ED = "+bean45293.toString)
            //                  logger.info("NAS DCNR ="+getJsonValueByKey(bean45293.toString, "DCNR"))
            //                  logger.info("NAS N1mode ="+getJsonValueByKey(bean45293.toString, "N1mode"))

            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, attachType = getJsonValueByKey(bean45293.toString, "EPS ATTACH TYPE"),
              taType = getJsonValueByKey(bean45293.toString, "EPS UPDATE TYPE VALUE"), epsMobileIdType = getJsonValueByKey(bean45293.toString, "TYPE OF IDENTITY"),
              imsi_NAS = getJsonValueByKey(bean45293.toString, "ident"), pdnType = getJsonValueByKey(bean45293.toString, "PDN type"),
              pdnReqType = getJsonValueByKey(bean45293.toString, "Request type"), lteAttReq = lteAttRequest, dcnr = getJsonValueByKey(bean45293.toString, "DCNR"),
              n1Mode = getJsonValueByKey(bean45293.toString, "N1mode"))
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45293",
              hexLogCode = "0xB0ED", logRecordNAS = logRecordNASObj)
          case "DETACH ACCEPT" =>
            val lteDetachAccept = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, lteDetachAcpt = lteDetachAccept)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45293",
              hexLogCode = "0xB0ED", logRecordNAS = logRecordNASObj)
          case "DETACH REQUEST" =>
            val lteDetachRequest = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, lteDetachReq = lteDetachRequest)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45293",
              hexLogCode = "0xB0ED", logRecordNAS = logRecordNASObj)
          case "TRACKING AREA UPDATE REQUEST" =>
            val lteTaRequest = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase, taUpdateType = getJsonValueByKey(bean45293.toString, "EPS UPDATE TYPE VALUE"),
              lteTaReq = lteTaRequest)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45293",
              hexLogCode = "0xB0ED", logRecordNAS = logRecordNASObj)
          case "ATTACH COMPLETE" =>
            val lteAttComplete = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase,
              lteAttCmp = lteAttComplete)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45293",
              hexLogCode = "0xB0ED", logRecordNAS = logRecordNASObj)
          case "SERVICE REQUEST" =>
            val lteSrvRequest = 1
            logRecordNASObj = logRecordNASObj.copy(NAS_MsgType = msgType.toUpperCase,
              lteSrvReq = lteSrvRequest)
            logRecord = logRecord.copy(logRecordName = "NAS", logCode = "45293",
              hexLogCode = "0xB0ED", logRecordNAS = logRecordNASObj)
          case _ =>
            logger.info("Entered into DEFAULT CASE for NAS, 0xB0ED with : " + msgType.toUpperCase)
        }
      case _ =>
      //logger.info("Enetered into DEFAULT CASE for NAS Log Codes...")
      //DON'T DO ANYTHING
    }
    logRecord
  }

}
