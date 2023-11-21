package com.verizon.oneparser.parselogs

import com.parser.commonFunction.CommonFunction
import com.verizon.oneparser.ProcessLogCodes
import com.verizon.oneparser.common.{Constants, OPutil}
import com.verizon.oneparser.common.Constants.isValidNumeric
import com.verizon.oneparser.schema._
import com.vzwdt.parseutils.ParseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.immutable.List

object LogRecordParserIP extends ProcessLogCodes {

  def parseLogRecordIPSet1(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String],isIPParserJar:Boolean=false) = {
    var logRecord: LogRecord = parentlogRecord
    var logRecordIPObj: LogRecordIP = LogRecordIP()

    logCode match {
      case 4587 =>
        val subArrayBytes_SR = java.util.Arrays.copyOfRange(x, 14, x.length)
        logRecordIPObj = getDataProtocolLogRecord(logRecordIPObj,subArrayBytes_SR)
        if(logRecordIPObj.ipSubtitle!=null && logRecordIPObj.ipSubtitle.nonEmpty) {
          logRecord = logRecord.copy(logRecordName = "IP", logCode = "4587",
            hexLogCode = "0x11EB", logRecordIP = logRecordIPObj)
        }
      case 65520 =>

        val pkt_13: Int = ParseUtils.intFromByte(x(13))
        //val iFaceId:Int = ParseUtils.intFromByte(x(14))
        val subArrayBytes_SR = java.util.Arrays.copyOfRange(x, 14, x.length)

        if(isIPParserJar){
          //New IPParser Integration
          val currentPayload = OPutil.printHexBinary(subArrayBytes_SR)
          var msgType:String=""
          var ipParserJson:String=""
          try{
            ipParserJson = CommonFunction.getJSONString(currentPayload)
            msgType = getJsonValueByKey(ipParserJson,"MessageType")
            //logger.info("MsgType : "+logRecord.dmTimeStamp+">>>>>>>>> "+msgType)
          }
          catch{
            case ge:Exception =>
              logger.error("0xFFF0(65520) : Error while parsing IP Payload for timestamp : "+logRecord.dmTimeStamp)
            case _ : Throwable =>
              logger.error("0xFFF0(65520) : Error while parsing IP Payload for timestamp : "+logRecord.dmTimeStamp);
          }


          msgType.toUpperCase match {
            case "RTP" =>
              //logger.info("RTP Raw Payload for Timestamp : "+logRecord.dmTimeStamp+">>>>>>>>> "+currentPayload+ ">>>>> Parsed as JSON : "+ipParserJson)
              val rtpDirectionVal: String = OPutil.getIpDirection(pkt_13)
              val ssrcIdJson:String = getSubJsonValueByKey(ipParserJson,"syncronizationSourceIdentifier")
              val SSRCIDHexVal: String = getJsonValueByKey(ssrcIdJson,"hexadecimal") + " ("+getJsonValueByKey(ssrcIdJson,"decimal")+")"
              logRecordIPObj = logRecordIPObj.copy(rtpDirection = rtpDirectionVal,SSRCIDHex = SSRCIDHexVal,ipType = msgType)
              if(!rtpDirectionVal.equalsIgnoreCase("UNKNOWN")){
                val seqNumJson:String = getSubJsonValueByKey(ipParserJson,"sequenceNumber")
                val timestampJson:String = getSubJsonValueByKey(ipParserJson,"timestamp")

                val rtpDownLinkSn: java.lang.Long = getJsonValueByKey(seqNumJson, "decimal").toLong
                val rxTimeStamp: String = getJsonValueByKey(timestampJson, "decimal")
                logRecordIPObj = logRecordIPObj.copy(rtpDlSn = rtpDownLinkSn, statusRtpDlSn=true, rtpRxTimeStamp = rxTimeStamp)
              }
              logRecord = logRecord.copy(logRecordName = "IP", logCode = "65520",
                hexLogCode = "0xFFF0",logRecordIP = logRecordIPObj)

            case "SIP" =>
              //logger.info("SIP Raw Payload for Timestamp : "+logRecord.dmTimeStamp+">>>>>>>>> "+currentPayload+ ">>>>> Parsed as JSON : "+ipParserJson)
              val sipMessage = getJsonValueByKey(ipParserJson, "MSG")
              val cseqMsg = getJsonValueByKey(ipParserJson, "CSeq")
              val subtitleVal = getJsonValueByKey(ipParserJson, "Subtitle")
              val callIdVal = getJsonValueByKey(ipParserJson, "Call-ID")
              var sipSplitArray: Array[String] = null
              var cSeqSplitArray: Array[String] = null
              if (sipMessage != null && sipMessage.nonEmpty) {
                try {
                  sipSplitArray = sipMessage.split(" ").map(_.trim)
                  cSeqSplitArray = cseqMsg.split(" ").map(_.trim)
                } catch {
                  case _: Exception =>
                    logger.error("Cannot Split sipMessage:" + sipMessage + "; File Name" + logRecord.fileName)
                }
              }

              Constants.getDlfFromSeq(logRecord.fileName)
              if (sipSplitArray != null) {
                logRecordIPObj = logRecordIPObj.copy(
                  sipMsg = sipMessage,
                  sipCSeq = cseqMsg,
                  ipType = msgType,
                  sipDirection = OPutil.getIpDirection(pkt_13),ipSubtitle = subtitleVal, ipCallId = callIdVal)
                if(logRecord.fileName.contains(Constants.CARRIER_VERIZON) && logRecord.fileName.contains("_dml_dlf")) {
                  if(subtitleVal.trim.equalsIgnoreCase("invite") && OPutil.getIpDirection(pkt_13).equalsIgnoreCase("outgoing")){
                    logRecordIPObj = logRecordIPObj.copy(rmMDN = getSipMdnFrom(ipParserJson))
                  }
                  if(subtitleVal.trim.equalsIgnoreCase("cancel")){
                    logRecordIPObj = logRecordIPObj.copy(cancelFromMDN = getSipMdnFrom(ipParserJson))
                  }
                }
                sipSplitArray.head match {
                  case "INVITE" =>
                  //logger.info("SIP sipMessage: INVITE!")
                  case "SIP/2.0" =>
                    //logger.info("SIP sipMessage: sipSplitArray: " + sipSplitArray)

                    if (sipSplitArray.size >= 2 && isValidNumeric(sipSplitArray(1), Constants.CONST_NUMBER)) {
                      //                        logger.info("SIP sipMessage: sipSplitArray(1).toInt: " + sipSplitArray(1).toInt)
                      sipSplitArray(1).toInt match {
                        case 200 =>
                          //                            logger.info("SIP sipMessage: cseqMsg size: " + cseqMsg.size)
                          //                            logger.info("SIP sipMessage: cseqMsg(0): " + cseqMsg(0))
                          //                            logger.info("SIP sipMessage: cseqMsg(1): " + cseqMsg(1))
                          if (cSeqSplitArray.size >= 2 && cSeqSplitArray(1).equalsIgnoreCase("INVITE")) {
                            logRecordIPObj = logRecordIPObj.copy(voLTETriggerEvt = 1,statusVoLTETriggerEvt = false)

                            //                              logger.info("SIP sipMessage: voLTETriggerEvt= 1")
                          }
                        case x if 300 until 699 contains x =>
                          if (cSeqSplitArray.size >= 2 && cSeqSplitArray(1).equalsIgnoreCase("INVITE")) {
                            logRecordIPObj = logRecordIPObj.copy(voLTEAbortEvt = 1, voLTECallEndEvt = 1, statusVoLTECallEndEvt = true)
                          }
                        case _ =>
                        //                            logger.info("SIP sipMessage: ignored(sipSplitArray(1)): " + sipSplitArray(1))
                      }
                    }
                  case "BYE" =>
                    var sipReason: String = null
                    val reasonStr = getJsonValueByKey(ipParserJson, "Reason")
                    //                      logger.info("SIP sipMessage: reasonStr: " + reasonStr)

                    try {
                      //val splitArray: String = sipMessage.split(" ").apply(0)
                      var subStr: Array[String] = reasonStr.split(":").map(_.trim)
                      sipReason = subStr.head

                      if(sipReason != null && sipReason.trim().isEmpty){
                        sipReason = null
                      }
                      //logger.info("SIP sipMessage: BYE sipReason: " + sipReason)
                    } catch {
                      case _: Exception =>
                        logger.error("Cannot Split sipMessage:" + sipMessage + "; File Name" + logRecord.fileName)
                    }

                    if(sipReason != null &&
                      !sipReason.contains("New Dialog Established")
                      && !sipReason.contains("Far Device Joined N-Way Call")) {

                      if (sipReason.contains("User Triggered") || sipReason.contains("Q.850;cause=16")) {

                        logRecordIPObj = logRecordIPObj.copy(sipByeReason = sipReason, voLTECallEndEvt = 1, statusVoLTECallEndEvt = true,  voLTECallNormalRelEvt = 1)
                        //                          logger.info("SIP sipMessage: voLTECallEndEvt= 1; sipReason = " + sipReason)

                      } else {
                        logRecordIPObj = logRecordIPObj.copy(sipByeReason = sipReason, voLTEDropEvt = 1,  voLTECallEndEvt = 1, statusVoLTECallEndEvt = true)
                        //logger.info("SIP sipMessage: voLTEDropEvt= 1; voLTECallEndEvt = 1; sipReason = " + sipReason)
                      }
                    }
                  case _ =>
                  //                      logger.info("SIP sipMessage: ignored(sipSplitArray.head): " + sipSplitArray.head)
                }
                logRecord = logRecord.copy(logRecordName = "IP", logCode = "65520",
                  hexLogCode = "0xFFF0",logRecordIP = logRecordIPObj)
              }
            case _ =>
            // UN-HANDLED MSG Type CASE
            //logger.info("Need Support for New IP Packets : "+ipParserJson)
          }

        }
        else{
          ParseUtils.intFromByte(x(12)) match {
            case 0 =>
              val sipJsonBean: String = getSipBeanAsJSON(subArrayBytes_SR, pkt_13)
              //logger.info("SIP JSON Bean : " + logRecord.fileName + ">>>>>>>>>>>>>>>>>>>>" + sipJsonBean)
              //logger.info("SIP Raw Payload for Timestamp : "+logRecord.dmTimeStamp+">>>>>>> Modified Payload>>>>> "+OPutil.printHexBinary(subArrayBytes_SR)+ ">>>>> Parsed as JSON : "+sipJsonBean)

              val sipMessage = getJsonValueByKey(sipJsonBean, "MSG")
              val cseqMsg = getJsonValueByKey(sipJsonBean, "CSeq")
              val subtitleVal = getJsonValueByKey(sipJsonBean, "Subtitle")
              val callIdVal = getJsonValueByKey(sipJsonBean, "Call-ID")
              var sipSplitArray: Array[String] = null
              var cSeqSplitArray: Array[String] = null

              //                logger.info("SIP sipMessage: " + sipMessage)
              //                logger.info("SIP sipMessage: cseqMsg= " + cseqMsg)

              if (sipMessage != null && sipMessage.nonEmpty) {
                try {
                  //val splitArray: String = sipMessage.split(" ").apply(0)
                  sipSplitArray = sipMessage.split(" ").map(_.trim)
                  cSeqSplitArray = cseqMsg.split(" ").map(_.trim)
                } catch {
                  case _: Exception =>
                    logger.error("Cannot Split sipMessage:" + sipMessage + "; File Name" + logRecord.fileName)
                }
              }
              if (sipSplitArray != null) {
                var sipDirectionVal = getJsonValueByKey(sipJsonBean, "Direction ")
                if(sipDirectionVal.isEmpty){
                  sipDirectionVal = getJsonValueByKey(sipJsonBean, "Direction")
                }
                if(logRecord.fileName.contains(Constants.CARRIER_VERIZON) && logRecord.fileName.contains("_dml_dlf")) {
                  if(subtitleVal.trim.equalsIgnoreCase("invite") && sipDirectionVal.equalsIgnoreCase("outgoing")){
                    logRecordIPObj = logRecordIPObj.copy(rmMDN = getSipMdnFrom(sipJsonBean))
                  }
                  if(subtitleVal.trim.equalsIgnoreCase("cancel")){
                    logRecordIPObj = logRecordIPObj.copy(cancelFromMDN = getSipMdnFrom(sipJsonBean))
                  }
                }
                logRecordIPObj = logRecordIPObj.copy(
                  sipMsg = sipMessage,
                  sipCSeq = cseqMsg,
                  ipType = "SIP",
                  sipDirection = sipDirectionVal,ipSubtitle = subtitleVal, ipCallId = callIdVal)

                sipSplitArray.head match {
                  case "INVITE" =>
                  //logger.info("SIP sipMessage: INVITE!")
                  case "SIP/2.0" =>
                    //logger.info("SIP sipMessage: sipSplitArray: " + sipSplitArray)

                    if (sipSplitArray.size >= 2 && isValidNumeric(sipSplitArray(1), Constants.CONST_NUMBER)) {
                      //                        logger.info("SIP sipMessage: sipSplitArray(1).toInt: " + sipSplitArray(1).toInt)
                      sipSplitArray(1).toInt match {
                        case 200 =>
                          //                            logger.info("SIP sipMessage: cseqMsg size: " + cseqMsg.size)
                          //                            logger.info("SIP sipMessage: cseqMsg(0): " + cseqMsg(0))
                          //                            logger.info("SIP sipMessage: cseqMsg(1): " + cseqMsg(1))
                          if (cSeqSplitArray.size >= 2 && cSeqSplitArray(1).equalsIgnoreCase("INVITE")) {
                            logRecordIPObj = logRecordIPObj.copy(voLTETriggerEvt = 1,statusVoLTETriggerEvt = false)

                            //                              logger.info("SIP sipMessage: voLTETriggerEvt= 1")
                          }
                        case x if 300 until 699 contains x =>
                          if (cSeqSplitArray.size >= 2 && cSeqSplitArray(1).equalsIgnoreCase("INVITE")) {
                            logRecordIPObj = logRecordIPObj.copy(voLTEAbortEvt = 1, voLTECallEndEvt = 1, statusVoLTECallEndEvt = true)
                          }
                        case _ =>
                        //                            logger.info("SIP sipMessage: ignored(sipSplitArray(1)): " + sipSplitArray(1))
                      }
                    }
                  case "BYE" =>
                    var sipReason: String = null
                    var sipCause: String = null
                    val reasonStr = getJsonValueByKey(sipJsonBean, "Reason")
                    //                      logger.info("SIP sipMessage: reasonStr: " + reasonStr)

                    try {
                      //val splitArray: String = sipMessage.split(" ").apply(0)
                      var subStr: Array[String] = reasonStr.split(";").map(_.trim)
                      sipReason = subStr.head
                      sipCause = subStr.last


                      if(sipReason != null && sipReason.trim().isEmpty) {
                        var subStr: Array[String] = reasonStr.split(":").map(_.trim)
                        sipReason = subStr.head
                      }

                      if(sipReason != null && sipReason.trim().isEmpty){
                        sipReason = null
                      }
                      //  logger.info("SIP sipMessage: BYE sipReason: " + sipReason)
                      //                      if(sipCause !=null) {
                      //                        logger.info("SIP sipMessage: BYE sipCause: " + sipCause)
                      //                      }
                    } catch {
                      case _: Exception =>
                        logger.error("Cannot Split sipMessage:" + sipMessage + "; File Name" + logRecord.fileName)
                    }

                    if(sipReason != null &&
                      !sipReason.contains("New Dialog Established")
                      && !sipReason.contains("Far Device Joined N-Way Call")) {

                      if (sipCause.contains("User Triggered") || sipCause.contains("Q.850;cause=16")) {

                        logRecordIPObj = logRecordIPObj.copy(sipByeReason = sipReason, voLTECallEndEvt = 1, statusVoLTECallEndEvt = true,  voLTECallNormalRelEvt = 1)
                        //                          logger.info("SIP sipMessage: voLTECallEndEvt= 1; sipReason = " + sipReason)

                      } else if((sipCause.contains("RTP-RTCP Timeout"))
                        || (sipCause.contains("cause=503") && (sipCause.contains("Session released - service-based local policy function aborted session") || sipCause.contains("Session released – IP CAN Session not available") || sipCause.contains("Session released – IP CAN Session not available")))) {

                        logRecordIPObj = logRecordIPObj.copy(sipByeReason = sipReason, voLTEDropEvt = 1,  voLTECallEndEvt = 1, statusVoLTECallEndEvt = true)
                        //logger.info("SIP sipMessage: voLTEDropEvt= 1; voLTECallEndEvt = 1; sipReason = " + sipReason)
                      }
                    }
                  case _ =>
                  //                      logger.info("SIP sipMessage: ignored(sipSplitArray.head): " + sipSplitArray.head)
                }
                logRecord = logRecord.copy(logRecordName = "IP", logCode = "65520",
                  hexLogCode = "0xFFF0",logRecordIP = logRecordIPObj)
              }
            case 1 =>
              //RTP Packet Parsing
              val rtpBean: String = getRtpBeanAsJSON(subArrayBytes_SR, pkt_13)
              //logger.info("RTP Raw Payload for Timestamp : "+logRecord.dmTimeStamp+">>>>>>> Modified Payload>>>>> "+OPutil.printHexBinary(subArrayBytes_SR)+ ">>>>> Parsed as JSON : "+rtpBean)
              //logger.info("RTP JSON Bean : " + logRecord.fileName + ">>>>>>>>>>>>>>>>>>>>" + rtpBean)
              //                logger.info("RTP JSON Bean (1): " + logRecord_IP.dmTimeStamp + ":: " + rtpBean)
              //                logger.info("RTP JSON Bean (2): " + logRecord_IP.dmTimeStamp + ":: " + convertBytesToHex(subArrayBytes_SR))

              def convertBytesToHex(bytes: Seq[Byte]): String = {
                val sb = new StringBuilder
                for (b <- bytes) {
                  sb.append(String.format("%02x", Byte.box(b)))
                  //sb.append(Byte.box(b).formatted("%02x"))
                }
                sb.toString
              }

              var rtpDirectionVal: String = null
              var SSRCIDHexVal: String = null
              if (getJsonValueByKey(rtpBean, "Description").contentEquals("RTP")) {
                rtpDirectionVal = getJsonValueByKey(rtpBean, "Direction")
                SSRCIDHexVal = getJsonValueByKey(rtpBean, "SSRCID(Hex)")
                logRecordIPObj = logRecordIPObj.copy(rtpDirection = rtpDirectionVal,SSRCIDHex = SSRCIDHexVal,ipType = "RTP")
                if (rtpDirectionVal.contentEquals("INCOMING") || rtpDirectionVal.contentEquals("OUTGOING")) {
                  val rtpDownLinkSn: java.lang.Long = getJsonValueByKey(rtpBean, "SequenceNumber").toLong
                  val rxTimeStamp: String = getJsonValueByKey(rtpBean, "Timestamp")

                  logRecordIPObj = logRecordIPObj.copy(rtpDlSn = rtpDownLinkSn, statusRtpDlSn=true, rtpRxTimeStamp = rxTimeStamp)
                }
              }
              logRecord = logRecord.copy(logRecordName = "IP", logCode = "65520",
                hexLogCode = "0xFFF0",logRecordIP = logRecordIPObj)
            case _ =>
            //Un-known IP Packet

          }
        }
    }
    logRecord
  }

  def getSipMdnFrom(ipParserJson:String): String ={
    val fromVal = getJsonValueByKey(ipParserJson,"From")
    var output = ""
    if(!fromVal.isEmpty){
      val fromSpltArr:Array[String] = fromVal.split("@").map(_.trim)
      if(fromSpltArr.size > 0){
        output = fromSpltArr(0).replace("<sip:+1","")
      }
    }
    output
  }
  def getDataProtocolLogRecord(lr:LogRecordIP,subArrayBytes_SR:Array[Byte]):LogRecordIP={
    var logRecordIPObj = lr
    try{
      val currentPayload = OPutil.printHexBinary(subArrayBytes_SR)
      val dpJson:String = CommonFunction.getJSONString(currentPayload)
      val msgType:String = getJsonValueByKey(dpJson,"MessageType")
      msgType.toUpperCase match {
        case "HTTP" =>
          /*if(msgType.equalsIgnoreCase("IPV6")){
            val tlp = getJsonValueByKey(dpJson,"TransportLayerProtocol")
            if(tlp.nonEmpty){
              logRecord = logRecord.copy(subtitle = tlp.split("v").apply(0).toUpperCase)
            }
          }*/
          if(msgType.equalsIgnoreCase("HTTP")) {
            logRecordIPObj = logRecordIPObj.copy(ipSubtitle = msgType.toUpperCase)
            val methodVal = getJsonValueByKey(dpJson, "Method")
            val hostVal = getJsonValueByKey(dpJson, "Host").trim
            if (methodVal.nonEmpty) {
              logRecordIPObj = logRecordIPObj.copy(httpMethod = methodVal)
              if(methodVal.toLowerCase.contains("200 ok")){
                val contentLengthStr = getJsonValueByKey(dpJson, "Content-Length")
                if(contentLengthStr.nonEmpty){
                  //val contentLenFlt = (contentLengthStr.toFloat)/(1024*1024)
                  val contentLenFlt = (contentLengthStr.toFloat)/(1024)
                  import scala.math.BigDecimal
                  logRecordIPObj = logRecordIPObj.copy(httpContentLength = BigDecimal.valueOf(contentLenFlt).setScale(2, BigDecimal.RoundingMode.HALF_UP).floatValue)
                }
              }
            }
            if (hostVal.nonEmpty) {
              logRecordIPObj = logRecordIPObj.copy(httpHost = hostVal)
            }
          }
        case _ =>
      }
    }catch{
      case e:Exception =>
        logger.error("[getDataProtocolLogRecord] ERROR Case -> IP Parser Issue " + "--> Msg : "+e.getMessage)
    }
    logRecordIPObj
  }
}
