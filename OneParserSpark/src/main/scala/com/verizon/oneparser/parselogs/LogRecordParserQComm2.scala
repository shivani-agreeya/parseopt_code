package com.verizon.oneparser.parselogs

import com.dmat.qc.parser.ParseUtils
import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.ProcessLogCodes
import com.verizon.oneparser.common.{CommonDaoImpl, Constants}
import com.verizon.oneparser.config.CommonConfigParameters
import com.verizon.oneparser.parselogs.LogRecordParser5G.{parseFloat, parseInteger, parseLong}
import com.verizon.oneparser.schema._
import com.vzwdt.qc_custom.CallManagerServingSystem.STACK_INFO_DTO

import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer
import scala.util.Try

object LogRecordParserQComm2 extends ProcessLogCodes with LazyLogging {

  def parseLogRecordQCOMM2_Set1(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String],commonConfigParams:CommonConfigParameters) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQComm2Obj:LogRecordQComm2 = LogRecordQComm2()
    logCode match {
      case 65529 => val bean65529 = process65529(x, exceptionUseArray).toString
        //logger.info("0xFFF9 65529 JSON>>>>>>>>>>>>>>>>>>"+bean65529)
        //logger.info("0xFFF9 65529 Raw Payload for Timestamp : "+logRecord.dmTimeStamp+">>>>>>>>> "+OPutil.printHexBinary(x)+ ">>>>> Parsed as JSON : "+bean65529)
        logRecordQComm2Obj = logRecordQComm2Obj.copy( deviceBuildVersion = getJsonValueByKey(bean65529, "Build_Version"),
          deviceModel = getJsonValueByKey(bean65529, "phoneModel"))
        logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "65529", hexLogCode = "0xFFF9",logRecordQComm2 = logRecordQComm2Obj)

      case 65530 => val bean65530 = process65530(x, exceptionUseArray).toString
        //logger.info("WiFi Json: " + bean65530)
        logRecordQComm2Obj = logRecordQComm2Obj.copy(wifiSSID = getJsonValueByKey(bean65530, "SSID"), wifiBSSID = getJsonValueByKey(bean65530, "BSSID"), wifiDhcpServer = getJsonValueByKey(bean65530, "serverAddress"),
          wifiGateway = getJsonValueByKey(bean65530, "gateway"), wifiHiddenSSID = getJsonValueByKey(bean65530, "hiddenSSID"), wifiIPAddr = getJsonValueByKey(bean65530, "ip"), wifiLinkSpeed = getJsonValueByKey(bean65530, "linkSpeed"),
          wifiMacAddr = getJsonValueByKey(bean65530, "macAddress"), wifiNetMask = getJsonValueByKey(bean65530, "netmask"), wifiNetworkId = getJsonValueByKey(bean65530, "networkId"), wifiPrimaryDNS = getJsonValueByKey(bean65530, "dns1"),
          wifiRSSI = getJsonValueByKey(bean65530, "rssi"), wifiSecDNS = getJsonValueByKey(bean65530, "dns2"),
          wifiDetailState = getJsonValueByKey(bean65530, "detailState"), wifiChannel = getJsonValueByKey(bean65530, "channel"), wifiFrequency = getJsonValueByKey(bean65530, "frequency"),
          wifiCallState = getJsonValueByKey(bean65530, "wifiCallState"), wifiISP = getJsonValueByKey(bean65530, "wifiISP"), wifiModel = getJsonValueByKey(bean65530, "wifiModel"),
          wifiSupplicantState = getJsonValueByKey(bean65530, "supplicantState"))
        logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "65530", hexLogCode = "0xFFFA", logRecordQComm2 = logRecordQComm2Obj)

      case 65531 =>
        //logger.info("0xFFFB 65531 Raw Payload for Timestamp Before Parsing : "+logRecord.dmTimeStamp+">>>>>>>>> "+OPutil.printHexBinary(x))
        val bean65531 = process65531(x, exceptionUseArray).toString
        //logger.info("0xFFFB 65531 JSON>>>>>>>>>>>>>>>>>>"+bean65531)
        //logger.info("0xFFFB 65531 Raw Payload for Timestamp : "+logRecord.dmTimeStamp+">>>>>>>>> "+OPutil.printHexBinary(x)+ ">>>>> Parsed as JSON : "+bean65531)
        val emailId = getJsonValueByKey(bean65531, "email")
        val dvcInfoVer = getJsonValueByKey(bean65531, "deviceInfoVersion")
        var modelNameFromJson = getJsonValueByKey(bean65531, "modelName")
        if(dvcInfoVer.equals("3")){
          val scannerJson = getSubJsonValueByKey(bean65531,"scanner")
          val ue1Json = getSubJsonValueByKey(bean65531,"ue1")
          modelNameFromJson = getJsonValueByKey(ue1Json,"modelName") + " " + getJsonValueByKey(scannerJson,"modelName")
        }
        val userInfo = CommonDaoImpl.getUserInfo(if (emailId != null) emailId.toLowerCase else "",commonConfigParams)
        logRecordQComm2Obj = logRecordQComm2Obj.copy(logRecordDescription = getJsonValueByKey(bean65531, "logRecordDescription"),
          appVersionCode = getJsonValueByKey(bean65531, "appVersionCode"), appVersionName = getJsonValueByKey(bean65531, "appVersionName"),
          baseBandVersion = getJsonValueByKey(bean65531, "baseBandVersion"), deviceInfoVersion = dvcInfoVer,
          email = getJsonValueByKey(bean65531, "email").toLowerCase,//OPutil.emailFormatter(getJsonValueByKey(bean65531, "email")),
          iccid = getJsonValueByKey(bean65531, "iccid"), imei_QComm2 = getJsonValueByKey(bean65531, "imei"),
          imsFeatureTag = getJsonValueByKey(bean65531, "imsFeatureTag"), imsStatus = getJsonValueByKey(bean65531, "imsStatus"),
          imsi_QComm2 = getJsonValueByKey(bean65531, "imsi"), isInBuilding = getJsonValueByKey(bean65531, "isInBuilding"),
          isLastChunk = getJsonValueByKey(bean65531, "isLastChunk"), mdn = getJsonValueByKey(bean65531, "mdn").replace("+1",""),
          modelName = modelNameFromJson, oemName = getJsonValueByKey(bean65531, "oemName"),
          osBuildId = getJsonValueByKey(bean65531, "osBuildId"), osSdkLevel = getJsonValueByKey(bean65531, "osSdkLevel"),
          osVersion = getJsonValueByKey(bean65531, "osVersion"), firstName = userInfo.firstName, lastName = userInfo.lastName, dmUser = userInfo.user_id,
          udid=getJsonValueByKey(bean65531, "udid"))
        logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "65531", hexLogCode = "0xFFFB", logRecordQComm2 = logRecordQComm2Obj)

      case 65523 =>
        val bean65523 = process65523(x, exceptionUseArray).toString
        logRecordQComm2Obj = logRecordQComm2Obj.copy(nr_rsrq = parseFloat(getJsonValueByKey(bean65523, "5g_rsrq")),
          nr_rsrp = parseFloat(getJsonValueByKey(bean65523, "5g_rsrp")), nr_rssnr = parseFloat(getJsonValueByKey(bean65523, "5g_rssnr")),
          lte_rsrq = parseFloat(getJsonValueByKey(bean65523, "lte_rsrq")), lte_rsrp = parseFloat(getJsonValueByKey(bean65523, "lte_rsrp")),
          lte_rssnr = parseFloat(getJsonValueByKey(bean65523, "lte_rssnr")), lte_enbId = parseInteger(getJsonValueByKey(bean65523, "lte_enb_id")),
          cellId = parseInteger(getJsonValueByKey(bean65523, "cell_id")), startTime = getJsonValueByKey(bean65523, "start_time"),
          testName = getJsonValueByKey(bean65523, "testname"), resultType = getJsonValueByKey(bean65523, "resultType"),
          avg_nr_rsrq = parseFloat(getJsonValueByKey(bean65523, "avg_5g_rsrq")), avg_nr_rsrp = parseFloat(getJsonValueByKey(bean65523, "avg_5g_rsrp")),
          avg_nr_rssnr = parseFloat(getJsonValueByKey(bean65523, "avg_5g_rssnr")), avg_lte_rsrq = parseFloat(getJsonValueByKey(bean65523, "avg_lte_rsrq")),
          avg_lte_rsrp = parseFloat(getJsonValueByKey(bean65523, "avg_lte_rsrp")), avg_lte_rssnr = parseFloat(getJsonValueByKey(bean65523, "avg_lte_rssnr")),
          task_time_median = parseFloat(getJsonValueByKey(bean65523, "task_time_median")),task_time_05p = parseFloat(getJsonValueByKey(bean65523, "task_time_05p")),
          task_time_95p = parseFloat(getJsonValueByKey(bean65523, "task_time_95p")),access_success = parseFloat(getJsonValueByKey(bean65523, "access_success")),
          task_success = parseFloat(getJsonValueByKey(bean65523, "task_success")),ttc_median = parseFloat(getJsonValueByKey(bean65523, "ttc_median")),
          ttc_05p = parseFloat(getJsonValueByKey(bean65523, "ttc_05p")),ttc_95p = parseFloat(getJsonValueByKey(bean65523, "ttc_95p")),
          iteration_id = parseInteger(getJsonValueByKey(bean65523, "iteration_id")),seq_id = parseInteger(getJsonValueByKey(bean65523, "seq_id")),
          pingRtt = parseInteger(getJsonValueByKey(bean65523, "pingRTT")),downloadTput = parseFloat(getJsonValueByKey(bean65523, "downloadTput")),uploadTput = parseFloat(getJsonValueByKey(bean65523, "uploadTput"))
        )


        logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "65523", hexLogCode = "0xFFF3", logRecordQComm2 = logRecordQComm2Obj)

      case 65525 =>
        val bean65525 = process65525(x, exceptionUseArray).toString
        logRecordQComm2Obj = logRecordQComm2Obj.copy(inbldImgId = getJsonValueByKey(bean65525, "imageId"),
          inbldAccuracy = getJsonValueByKey(bean65525, "accuracy"), inbldAltitude = getJsonValueByKey(bean65525, "altitude"),
          inbldUserMark = getJsonValueByKey(bean65525, "userMarked"), inbldX = getJsonValueByKey(bean65525, "x"), inbldY = getJsonValueByKey(bean65525, "y"))
        logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "65525", hexLogCode = "0xFFF5", logRecordQComm2 = logRecordQComm2Obj)

      case 6222 =>
        val bean6222 = process6222(x, exceptionUseArray)
        //logger.info("0x184E json:" + Constants.getJsonFromBean(bean6222))

        var stk_SYS_MODE0_0x184E: java.lang.Integer = null
        var stk_SYS_MODE1_0x184E: java.lang.Integer = null
        var CdmaTypeSid: java.lang.Integer = null
        var CdmaTypeNid: java.lang.Integer = null
        var CdmaEcIo: java.lang.Float = null
        var BandClass: String = null
        var EvDoECIO: java.lang.Float = null
        var EvDoSINR: java.lang.Integer = null
        var WcdmaRssi: java.lang.Float = null
        var WcdmaRscp: java.lang.Integer = null
        var WcdmaEcio: java.lang.Float = null

        val mSTACK_INFO_0: STACK_INFO_DTO = bean6222.getSTACK_INFO_0()
        val mSTACK_INFO_1: STACK_INFO_DTO = bean6222.getSTACK_INFO_1()

        if(mSTACK_INFO_0!=null){
          stk_SYS_MODE0_0x184E = mSTACK_INFO_0.getSYS_MODE()
          if (mSTACK_INFO_0.getSYS_ID().getID_TYPE() == 2) {
            CdmaTypeSid = mSTACK_INFO_0.getSYS_ID().id.getIS95().getSID() //CDMA SID
            CdmaTypeNid = mSTACK_INFO_0.getSYS_ID().id.getIS95().getNID //CDMA NID
          }
          if (mSTACK_INFO_0.getSYS_MODE() == 2) {
            CdmaEcIo = mSTACK_INFO_0.getECIO() //mECIO
            BandClass = mSTACK_INFO_0.getMODE_INFO().getCDMA_INFO().getBandClass()
          } else if (mSTACK_INFO_0.getSYS_MODE() == 4) {
            EvDoECIO = mSTACK_INFO_0.getECIO() //mECIO
            EvDoSINR = mSTACK_INFO_0.getSINR() // EvDo SINR

          } else if (mSTACK_INFO_0.getSYS_MODE() == 5) {
            WcdmaRssi = mSTACK_INFO_0.getRSSI() //RSSI for WCDMA
            WcdmaRscp = mSTACK_INFO_0.getRSCP() // WCDMA RSCP
            WcdmaEcio = mSTACK_INFO_0.getECIO() // WCDMA ECIO

          } else {
            //Other RAT
          }
        }
        if(mSTACK_INFO_1!=null){
          stk_SYS_MODE1_0x184E = mSTACK_INFO_1.getSYS_MODE()
          if (mSTACK_INFO_1.getSYS_ID().getID_TYPE() == 2) {
            CdmaTypeSid = mSTACK_INFO_1.getSYS_ID().id.getIS95().getSID() //CDMA SID
            CdmaTypeNid = mSTACK_INFO_1.getSYS_ID().id.getIS95().getNID //CDMA NID
          }
          if (mSTACK_INFO_1.getSYS_MODE() == 2) {
            CdmaEcIo = mSTACK_INFO_1.getECIO() //mECIO
            BandClass = mSTACK_INFO_1.getMODE_INFO().getCDMA_INFO().getBandClass()
          } else if (mSTACK_INFO_1.getSYS_MODE() == 4) {
            EvDoECIO = mSTACK_INFO_1.getECIO() //mECIO
            EvDoSINR = mSTACK_INFO_1.getSINR() // EvDo SINR

          } else if (mSTACK_INFO_1.getSYS_MODE() == 5) {
            WcdmaRssi = mSTACK_INFO_1.getRSSI() //RSSI for WCDMA
            WcdmaRscp = mSTACK_INFO_1.getRSCP() // WCDMA RSCP
            WcdmaEcio = mSTACK_INFO_1.getECIO() // WCDMA ECIO
          } else {
            //Other RAT
          }
        }

        /*stk_SYS_MODE0_0x184E = mSTACK_INFO_0.getSYS_MODE()
        stk_SYS_MODE1_0x184E = mSTACK_INFO_1.getSYS_MODE()

        if (mSTACK_INFO_0.getSYS_ID().getID_TYPE() == 2) {
          CdmaTypeSid = mSTACK_INFO_0.getSYS_ID().id.getIS95().getSID() //CDMA SID
          CdmaTypeNid = mSTACK_INFO_0.getSYS_ID().id.getIS95().getNID //CDMA NID
          if (mSTACK_INFO_1.getSYS_ID().getID_TYPE() == 2) {
            CdmaTypeSid = mSTACK_INFO_1.getSYS_ID().id.getIS95().getSID() //CDMA SID
            CdmaTypeNid = mSTACK_INFO_1.getSYS_ID().id.getIS95().getNID //CDMA NID
          }
        }

        if (mSTACK_INFO_0.getSYS_MODE() == 2) {
          CdmaEcIo = mSTACK_INFO_0.getECIO() //mECIO
          BandClass = mSTACK_INFO_0.getMODE_INFO().getCDMA_INFO().getBandClass()
        } else if (mSTACK_INFO_0.getSYS_MODE() == 4) {
          EvDoECIO = mSTACK_INFO_0.getECIO() //mECIO
          EvDoSINR = mSTACK_INFO_0.getSINR() // EvDo SINR

        } else if (mSTACK_INFO_0.getSYS_MODE() == 5) {
          WcdmaRssi = mSTACK_INFO_0.getRSSI() //RSSI for WCDMA
          WcdmaRscp = mSTACK_INFO_0.getRSCP() // WCDMA RSCP
          WcdmaEcio = mSTACK_INFO_0.getECIO() // WCDMA ECIO

        } else {
          //Other RAT
        }

        if (mSTACK_INFO_1.getSYS_MODE() == 2) {
          CdmaEcIo = mSTACK_INFO_1.getECIO() //mECIO
          BandClass = mSTACK_INFO_1.getMODE_INFO().getCDMA_INFO().getBandClass()
        } else if (mSTACK_INFO_1.getSYS_MODE() == 4) {
          EvDoECIO = mSTACK_INFO_1.getECIO() //mECIO
          EvDoSINR = mSTACK_INFO_1.getSINR() // EvDo SINR

        } else if (mSTACK_INFO_1.getSYS_MODE() == 5) {
          WcdmaRssi = mSTACK_INFO_1.getRSSI() //RSSI for WCDMA
          WcdmaRscp = mSTACK_INFO_1.getRSCP() // WCDMA RSCP
          WcdmaEcio = mSTACK_INFO_1.getECIO() // WCDMA ECIO
        } else {
          //Other RAT
        }*/

        //logger.info("0x184E WcdmaRssi:" + WcdmaRssi)

        logRecordQComm2Obj = logRecordQComm2Obj.copy(
          stkInfo_ECIO = Try(bean6222.STACK_INFO_0.ECIO).getOrElse(null),//Try(bean6222.STACK_INFO_0.ECIO.asInstanceOf[Float]).getOrElse(null),
          stk1Info_ECIO = Try(bean6222.STACK_INFO_1.ECIO).getOrElse(null),
          stk_ROAM_STATUS = Try(bean6222.STACK_INFO_0.ROAM_STATUS).getOrElse(null),
          stk1_ROAM_STATUS = Try(bean6222.STACK_INFO_1.ROAM_STATUS).getOrElse(null),
          stk_SYS_MODE0_0x184E = stk_SYS_MODE0_0x184E,
          stk_SYS_MODE1_0x184E = stk_SYS_MODE1_0x184E,
          stk1_SYS_MODE = Try(bean6222.STACK_INFO_1.SYS_MODE).getOrElse(null),
          stk_SRV_STATUS0x184E = Try(bean6222.STACK_INFO_0.SRV_STATUS).getOrElse(null),
          stk1_SRV_STATUS = Try(bean6222.STACK_INFO_1.SRV_STATUS).getOrElse(null),
          stk_SID0x184E = CdmaTypeSid,
          stk_NID0x184E = CdmaTypeNid,
          stk_SINR_EvDo0x184E = Try(bean6222.STACK_INFO_0.SINR).getOrElse(null),
          stk1_SINR = Try(bean6222.STACK_INFO_1.SINR).getOrElse(null),
          stk_RSSI_wcdma0x184E = Try(bean6222.STACK_INFO_0.RSSI).getOrElse(null),
          stk1_RSSI = Try(bean6222.STACK_INFO_1.RSSI).getOrElse(null),
          stk_GW_RSCP0x184E = Try(bean6222.STACK_INFO_0.RSCP).getOrElse(null),
          stk1_GW_RSCP = Try(bean6222.STACK_INFO_1.RSCP).getOrElse(null), bandClass_QComm2 = bean6222.bandClass,
          stk_ECIO0x184E = Try(bean6222.STACK_INFO_0.ECIO).getOrElse(null),
          sys_ID0x184E = Try(bean6222.STACK_INFO_0.SYS_ID.ID_TYPE).getOrElse(null),
          stk_ECIO_EvDo0x184E = Try(bean6222.STACK_INFO_0.ECIO).getOrElse(null),
          sys_Mode_Operational_0x184E = Try(bean6222.STACK_INFO_1.IS_OPERATIONAL).getOrElse(null),
          CdmaEcIo_0x184E = CdmaEcIo,
          BandClass_0x184E = BandClass,
          EvDoECIO_0x184E = EvDoECIO,
          EvDoSINR_0x184E = EvDoSINR,
          WcdmaRssi_0x184E = WcdmaRssi,
          WcdmaRscp_0x184E = WcdmaRscp,
          WcdmaEcio_0x184E = WcdmaEcio,
          status0x184E = true)
        logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "6222",
          hexLogCode = "0x184E",  logRecordQComm2 = logRecordQComm2Obj)
      case 4943 =>
        val bean4943 = process4943(x, exceptionUseArray)
        //logger.info("0x134F json:"+Constants.getJsonFromBean(bean4943))

        logRecordQComm2Obj = logRecordQComm2Obj.copy( stk_SID = bean4943.SID, stk_NID = bean4943.NID, stk_SYS_MODE = bean4943.SYS_MODE,
          stk_ECIO = bean4943.ECIO, stk_ECIO_EvDo = bean4943.ECIO_EvDo, stk_SRV_STATUS = bean4943.SRV_STATUS,
          stk_GW_RSCP = bean4943.GW_RSCP, stk_RSSI_wcdma = bean4943.RSSI_wcdma, stk_SINR_EvDo = bean4943.SINR_EvDo, bandClass_QComm2 = bean4943.MODE_INFO.CDMA_INFO.bandClass, HdrHybrid = bean4943.HDR_HYBRID
        )
        logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "4943",
          hexLogCode = "0x134F",  logRecordQComm2 = logRecordQComm2Obj)
      case 45207 =>
        logVersion match {
          /*case 1 =>
            try{
              val bean45207 = process45207_1(x, exceptionUseArray)
              //logger.info("0xB097 json 45207 json>>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45207));
              var ctrlBytesVal: List[Long] = null
              var dataBytesVal: List[Long] = null
              var rlcUlTotalBytesVal: Long = 0
              dataBytesVal = bean45207.sr_22184_inst_list.asScala.toList.map(_.sr_22194_inst_list.asScala.toList.map(_.sr_22281_inst_list.asScala.toList.map(_.sr_22484_inst_list.asScala.toList.map(_.NumNewDataPDUBytes.asInstanceOf[Long])))).flatten.flatten.flatten
              ctrlBytesVal = bean45207.sr_22184_inst_list.asScala.toList.map(_.sr_22194_inst_list.asScala.toList.map(_.sr_22281_inst_list.asScala.toList.map(_.sr_22484_inst_list.asScala.toList.map(_.NumC.asInstanceOf[Long])))).flatten.flatten.flatten

              for (i <- 0 to bean45207.numRb - 1) {
                rlcUlTotalBytesVal += dataBytesVal(i) + ctrlBytesVal(i)
              }
              logRecordQComm2Obj = logRecordQComm2Obj.copy( rlcUlDataPDUByte = bean45207.rbs.asScala.toList.map(_.dataPDUByte.asInstanceOf[Long]), rlcUlNumRBs = bean45207.numRb,
                rlcUlCtrlPDUBytes = bean45207.rbs.asScala.toList.map(_.ctrlPDUBytes.asInstanceOf[Long]), rlcUlTotalBytes = rlcUlTotalBytesVal, rlcUlTimeStamp = logRecord.dmTimeStamp.getTime)
            }
            catch{
              case _ : Throwable =>
                logger.error("0xB097("+logVersion+") : Error while parsing : ");
            }*/
          case 56 =>
            val bean45207_56 = process45207_56(x, exceptionUseArray)
            var ctrlBytesVal: List[Long] = null
            var dataBytesVal: List[Long] = null
            var rlcUlTotalBytesVal: Long = 0

            dataBytesVal = bean45207_56.sr_125796_inst_list.asScala.toList.map(e => parseLong(e.NumNewDataPDUBytes))
            ctrlBytesVal = bean45207_56.sr_125796_inst_list.asScala.toList.map(e => parseLong(e.NumCtrlPDUBytesTx))
            val numRb = bean45207_56.sr_125796_inst_list.size()

            for (i <- 0 to numRb - 1) {
              rlcUlTotalBytesVal += dataBytesVal(i) + ctrlBytesVal(i)
            }
            logRecordQComm2Obj = logRecordQComm2Obj.copy(rlcUlDataPDUByte = dataBytesVal, rlcUlNumRBs = numRb,
              rlcUlCtrlPDUBytes = ctrlBytesVal, rlcUlTotalBytes = rlcUlTotalBytesVal, rlcUlTimeStamp = logRecord.dmTimeStamp.getTime)
          case _ =>
            val bean45207 = process45207(x, exceptionUseArray)
            //logger.info("0xB097 json 45207 json>>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45207));
            var ctrlBytesVal: List[Long] = null
            var dataBytesVal: List[Long] = null
            var rlcUlTotalBytesVal: Long = 0

            dataBytesVal = bean45207.rbs.asScala.toList.map(_.dataPDUByte.asInstanceOf[Long])
            ctrlBytesVal = bean45207.rbs.asScala.toList.map(_.ctrlPDUBytes.asInstanceOf[Long])

            for (i <- 0 to bean45207.numRb - 1) {
              rlcUlTotalBytesVal += dataBytesVal(i) + ctrlBytesVal(i)
            }
            logRecordQComm2Obj = logRecordQComm2Obj.copy( rlcUlDataPDUByte = bean45207.rbs.asScala.toList.map(_.dataPDUByte.asInstanceOf[Long]), rlcUlNumRBs = bean45207.numRb,
              rlcUlCtrlPDUBytes = bean45207.rbs.asScala.toList.map(_.ctrlPDUBytes.asInstanceOf[Long]), rlcUlTotalBytes = rlcUlTotalBytesVal, rlcUlTimeStamp = logRecord.dmTimeStamp.getTime)
        }

        logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45207", hexLogCode = "0xB097", logRecordQComm2 = logRecordQComm2Obj)

      case 45155 =>
        //logger.info("0xB063(45155) Raw Payload for Timestamp : "+logRecord.dmTimeStamp+" ........"+OPutil.printHexBinary(x,true))
        logVersion match {
          case 49 =>
            try{
              val bean45155 = process45155_49(x, exceptionUseArray)
              //logger.info("0xB063(45155) JSON for Timestamp : "+logRecord.dmTimeStamp+" ........"+Constants.getJsonFromBean(bean45155))
              val macDlfsn = bean45155.sr_101812_inst_list.asScala.toList.map(_.sr_101814_inst_list.asScala.toList.map(_.sr_101835_inst_list.asScala.toList.map(_.Frame))).flatten.flatten
              val macDlSubSfn = bean45155.sr_101812_inst_list.asScala.toList.map(_.sr_101814_inst_list.asScala.toList.map(_.sr_101835_inst_list.asScala.toList.map(_.SubFrame))).flatten.flatten
              val macDlTbs = bean45155.sr_101812_inst_list.asScala.toList.map(_.sr_101814_inst_list.asScala.toList.map(_.sr_101835_inst_list.asScala.toList.map(_.TBSize))).flatten.flatten

              logRecordQComm2Obj = logRecordQComm2Obj.copy(MacDlSfn = Try(macDlfsn.map(_.toInt)).getOrElse(null),
                MacDlNumSamples =Try(macDlTbs.size.asInstanceOf[Integer]).getOrElse(null), MacDlSubSfn = Try(macDlSubSfn.map(_.toInt)).getOrElse(null), MacDlTbs = Try(macDlTbs.map(_.toInt)).getOrElse(null))
              logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45155",
                hexLogCode = "0xB063",  logRecordQComm2 = logRecordQComm2Obj)
            }
            catch{
              case _ : Throwable =>
                logger.error("0xB063("+logVersion+") : Error while parsing : ");
            }

          case _ =>
            try{
              val bean45155 = process45155(x, exceptionUseArray)
              //logger.info("0xB063(45155) json  for Timestamp : "+logRecord.dmTimeStamp+"........."+Constants.getJsonFromBean(bean45155));
              logRecordQComm2Obj = logRecordQComm2Obj.copy( MacDlSfn = Try(bean45155.samples.asScala.toList.map(_.sfn.toInt)).getOrElse(null),
                MacDlSubSfn = Try(bean45155.samples.asScala.toList.map(_.subFn.toInt)).getOrElse(null), MacDlNumSamples = Try(bean45155.numSamples).getOrElse(null),
                MacDlTbs = Try(bean45155.samples.asScala.toList.map(_.dlTbs.toInt)).getOrElse(null))
              logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45155",
                hexLogCode = "0xB063",  logRecordQComm2 = logRecordQComm2Obj)
            }
            catch{
              case _ : Throwable =>
                logger.error("0xB063("+logVersion+") : Error while parsing : ");
            }

        }
        logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45155",
          hexLogCode = "0xB063",  logRecordQComm2 = logRecordQComm2Obj)

      case 45156 =>
        try {
          val bean45156 = process45156(x, exceptionUseArray)
          //logger.info("0xB064 json 45156 json>>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45156));
          logRecordQComm2Obj = logRecordQComm2Obj.copy( MacUlSfn = bean45156.samples.asScala.toList.map(_.sfn.asInstanceOf[Integer]), MacUlSubSfn = bean45156.samples.asScala.toList.map(_.subFn.asInstanceOf[Integer]),
            MacUlNumSamples = bean45156.numSamples, MacUlGrant = bean45156.samples.asScala.toList.map(_.ulGrant.asInstanceOf[Integer]))
          logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45156",
            hexLogCode = "0xB064",  logRecordQComm2 = logRecordQComm2Obj)
        }
        catch {
          case _ : Throwable =>
            logger.error("0xB064 : Error while parsing");
        }
    }
    logRecord
  }

  def parseLogRecordQCOMM2_Set2(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQComm2Obj:LogRecordQComm2 = LogRecordQComm2()
    var logRecordQComm5GObj: LogRecordQComm5G = LogRecordQComm5G()

    logCode match {
      case 45220 =>
        logVersion match {
          case 49 =>
            val bean45220 = process45220_49(x, exceptionUseArray)
            var pdcpDlCtrlBytesVal: List[Long] = null
            var pdcpDlDataBytesVal: List[Long] = null
            var pdcpDlInvalidBytesVal: List[Long] = null
            var pdcpDlTotalBytesVal: Long = 0
            val numRbVal = if(Constants.isValidNumeric(bean45220.NumRB,"NUMBER")) bean45220.NumRB.toInt else 0

            //logger.info("0xB0A4 json >>>>>>>>>>>>>>>" + Constants.getJsonFromBean(bean45220))

            if (bean45220.sr_103434_inst_list.size() > 0) {
              pdcpDlCtrlBytesVal = bean45220.sr_103434_inst_list.asScala.toList.map(_.NumControlPDUReceived.toLong.asInstanceOf[Long])
              pdcpDlDataBytesVal = bean45220.sr_103434_inst_list.asScala.toList.map(_.DataPDUBytesReceived.toLong.asInstanceOf[Long])
            }

            for (i <- 0 to bean45220.sr_103434_inst_list.size() - 1) {
              pdcpDlTotalBytesVal += pdcpDlCtrlBytesVal(i) + pdcpDlDataBytesVal(i)
            }

            logRecordQComm2Obj = logRecordQComm2Obj.copy(pdcpDlNumRBs = numRbVal, pdcpDlCtrlBytes = pdcpDlCtrlBytesVal, pdcpDlDataBytes = pdcpDlDataBytesVal, pdcpDlInvalidBytes = pdcpDlInvalidBytesVal,
              pdcpDlTotalBytes = pdcpDlTotalBytesVal, pdcpDlTimeStamp = logRecord.dmTimeStamp.getTime)

          case 50 =>
            val bean45220 = process45220_50(x, exceptionUseArray)
            var pdcpDlCtrlBytesVal: List[Long] = null
            var pdcpDlDataBytesVal: List[Long] = null
            var pdcpDlInvalidBytesVal: List[Long] = null
            var pdcpDlTotalBytesVal: Long = 0
            val numRbVal = if(Constants.isValidNumeric(bean45220.NumRB,"NUMBER")) bean45220.NumRB.toInt else 0

            if (bean45220.sr_104503_inst_list.size() > 0) {
              pdcpDlCtrlBytesVal = bean45220.sr_104503_inst_list.asScala.toList.map(_.NumControlPDUReceived.toLong.asInstanceOf[Long])
              pdcpDlDataBytesVal = bean45220.sr_104503_inst_list.asScala.toList.map(_.DataPDUBytesReceived.toLong.asInstanceOf[Long])
            }

            for (i <- 0 to bean45220.sr_104503_inst_list.size() - 1) {
              pdcpDlTotalBytesVal += pdcpDlCtrlBytesVal(i) + pdcpDlDataBytesVal(i)
            }

            logRecordQComm2Obj = logRecordQComm2Obj.copy(pdcpDlNumRBs = numRbVal, pdcpDlCtrlBytes = pdcpDlCtrlBytesVal, pdcpDlDataBytes = pdcpDlDataBytesVal, pdcpDlInvalidBytes = pdcpDlInvalidBytesVal,
              pdcpDlTotalBytes = pdcpDlTotalBytesVal, pdcpDlTimeStamp = logRecord.dmTimeStamp.getTime)

          case 51 =>
            val bean45220 = process45220_51(x, exceptionUseArray)
            var pdcpDlCtrlBytesVal: List[Long] = null
            var pdcpDlDataBytesVal: List[Long] = null
            var pdcpDlInvalidBytesVal: List[Long] = null
            var pdcpDlTotalBytesVal: Long = 0
            val numRbVal = if(Constants.isValidNumeric(bean45220.NumRB,"NUMBER")) bean45220.NumRB.toInt else 0

            if (bean45220.sr_104553_inst_list.size() > 0) {
              pdcpDlCtrlBytesVal = bean45220.sr_104553_inst_list.asScala.toList.map(_.NumControlPDUReceived.toLong.asInstanceOf[Long])
              pdcpDlDataBytesVal = bean45220.sr_104553_inst_list.asScala.toList.map(_.DataPDUBytesReceived.toLong.asInstanceOf[Long])
            }

            for (i <- 0 to bean45220.sr_104553_inst_list.size() - 1) {
              pdcpDlTotalBytesVal += pdcpDlCtrlBytesVal(i) + pdcpDlDataBytesVal(i)
            }

            logRecordQComm2Obj = logRecordQComm2Obj.copy(pdcpDlNumRBs = numRbVal, pdcpDlCtrlBytes = pdcpDlCtrlBytesVal, pdcpDlDataBytes = pdcpDlDataBytesVal, pdcpDlInvalidBytes = pdcpDlInvalidBytesVal,
              pdcpDlTotalBytes = pdcpDlTotalBytesVal, pdcpDlTimeStamp = logRecord.dmTimeStamp.getTime)
          case 52 =>
            val bean45220 = process45220_52(x, exceptionUseArray)
            var pdcpDlCtrlBytesVal: List[Long] = null
            var pdcpDlDataBytesVal: List[Long] = null
            var pdcpDlInvalidBytesVal: List[Long] = null
            var pdcpDlTotalBytesVal: Long = 0
            val numRbVal = if(Constants.isValidNumeric(bean45220.NumRB,"NUMBER")) bean45220.NumRB.toInt else 0

            if (bean45220.sr_119071_inst_list.size() > 0) {
              pdcpDlCtrlBytesVal = bean45220.sr_119071_inst_list.asScala.toList.map(_.NumControlPDUReceived.toLong.asInstanceOf[Long])
              pdcpDlDataBytesVal = bean45220.sr_119071_inst_list.asScala.toList.map(_.DataPDUBytesReceived.toLong.asInstanceOf[Long])
            }

            for (i <- 0 to bean45220.sr_119071_inst_list.size() - 1) {
              pdcpDlTotalBytesVal += pdcpDlCtrlBytesVal(i) + pdcpDlDataBytesVal(i)
            }

            logRecordQComm2Obj = logRecordQComm2Obj.copy(pdcpDlNumRBs = numRbVal, pdcpDlCtrlBytes = pdcpDlCtrlBytesVal, pdcpDlDataBytes = pdcpDlDataBytesVal, pdcpDlInvalidBytes = pdcpDlInvalidBytesVal,
              pdcpDlTotalBytes = pdcpDlTotalBytesVal, pdcpDlTimeStamp = logRecord.dmTimeStamp.getTime)

          case _ =>
            val bean45220 = process45220(x, exceptionUseArray)
            var pdcpDlCtrlBytesVal: List[Long] = null
            var pdcpDlDataBytesVal: List[Long] = null
            var pdcpDlInvalidBytesVal: List[Long] = null
            var pdcpDlTotalBytesVal: Long = 0

            //logger.info("0xB0A4 json >>>>>>>>>>>>>>>" + Constants.getJsonFromBean(bean45220))

            if (bean45220.rbs.size() > 0) {
              pdcpDlCtrlBytesVal = bean45220.rbs.asScala.toList.map(_.NumControlPDUBytes.toLong.asInstanceOf[Long])
              pdcpDlDataBytesVal = bean45220.rbs.asScala.toList.map(_.NumPDUBytes.toLong.asInstanceOf[Long])
              pdcpDlInvalidBytesVal = bean45220.rbs.asScala.toList.map(_.NumPDUInvalidBytes.toLong.asInstanceOf[Long])

            } else if (bean45220.rbsNew.size() > 0) {
              var connectionCount: List[Integer] = bean45220.rbsNew.asScala.toList.map(_.NumConn.asInstanceOf[Integer])
              var pdcpDlCtrlBytesValNew: List[List[Long]] = bean45220.rbsNew.asScala.toList.map(_.NumControlPDUBytes.asInstanceOf[List[Long]])
              var pdcpDlDataBytesValNew: List[List[Long]] = bean45220.rbsNew.asScala.toList.map(_.NumPDUBytes.asInstanceOf[List[Long]])
              var pdcpDlInvalidBytesValNew: List[List[Long]] = bean45220.rbsNew.asScala.toList.map(_.NumPDUInvalidBytes.asInstanceOf[List[Long]])

              var pdcpDlCtrlBytesValLb = new ListBuffer[Long]()
              var pdcpDlDataBytesValLb = new ListBuffer[Long]()
              var pdcpDlInvalidBytesValLb = new ListBuffer[Long]()

              //          for (size :Integer <- connectionCount) {
              //          }

              for (x: List[Long] <- pdcpDlCtrlBytesValNew) {
                var sum: Long = 0L
                x.foreach(sum += _)
                pdcpDlCtrlBytesValLb += sum
              }

              for (x: List[Long] <- pdcpDlDataBytesValNew) {
                var sum: Long = 0L
                x.foreach(sum += _)
                pdcpDlDataBytesValLb += sum

              }

              for (x: List[Long] <- pdcpDlInvalidBytesValNew) {
                var sum: Long = 0L
                x.foreach(sum += _)
                pdcpDlInvalidBytesValLb += sum
              }

              pdcpDlCtrlBytesVal = pdcpDlCtrlBytesValLb.toList
              pdcpDlDataBytesVal = pdcpDlDataBytesValLb.toList
              pdcpDlInvalidBytesVal = pdcpDlInvalidBytesValLb.toList

            } else if (bean45220.rbsV40.size() > 0) {
              pdcpDlCtrlBytesVal = bean45220.rbsV40.asScala.toList.map(_.NumControlPDUBytes.asInstanceOf[Long])
              pdcpDlDataBytesVal = bean45220.rbsV40.asScala.toList.map(_.NumPDUBytes.asInstanceOf[Long])
              pdcpDlInvalidBytesVal = bean45220.rbsV40.asScala.toList.map(_.NumPDUInvalidBytes.asInstanceOf[Long])

            } else if (bean45220.rbsV41.size() > 0) {
              pdcpDlCtrlBytesVal = bean45220.rbsV41.asScala.toList.map(_.NumControlPDUBytes.asInstanceOf[Long])
              pdcpDlDataBytesVal = bean45220.rbsV41.asScala.toList.map(_.NumPDUBytes.asInstanceOf[Long])
              pdcpDlInvalidBytesVal = bean45220.rbsV41.asScala.toList.map(_.NumPDUInvalidBytes.asInstanceOf[Long])

            }

            for (i <- 0 to bean45220.numRb - 1) {
              pdcpDlTotalBytesVal += pdcpDlCtrlBytesVal(i) + pdcpDlDataBytesVal(i) + pdcpDlInvalidBytesVal(i)
            }

            logRecordQComm2Obj = logRecordQComm2Obj.copy(pdcpDlNumRBs = bean45220.numRb, pdcpDlCtrlBytes = pdcpDlCtrlBytesVal, pdcpDlDataBytes = pdcpDlDataBytesVal, pdcpDlInvalidBytes = pdcpDlInvalidBytesVal,
              pdcpDlSubPacketVersion = bean45220.subPacketVersion, pdcpDlTotalBytes = pdcpDlTotalBytesVal, pdcpDlTimeStamp = logRecord.dmTimeStamp.getTime)
        }
        logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45220",
          hexLogCode = "0xB0A4", logRecordQComm2 = logRecordQComm2Obj)

      case 45236 =>
        logVersion match {
          case 27 =>
            var pdcpUlTotalBytesVal: Long = 0
            val bean45236 = process45236_27(x, exceptionUseArray)
            //logger.info("0xB0B4 JSON>>>>>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean45236))
            val pdcpUlCtrlBytesVal = bean45236.sr_116425_inst_list.asScala.toList.map(_.sr_116537_inst_list.asScala.toList.map(_.sr_116426_inst_list.asScala.toList.map(_.sr_116568_inst_list.asScala.toList.map(_.NumControlPDUTxBytes.asInstanceOf[Long])))).flatten.flatten.flatten
            val pdcpUlCDataBytesVal = bean45236.sr_116425_inst_list.asScala.toList.map(_.sr_116537_inst_list.asScala.toList.map(_.sr_116426_inst_list.asScala.toList.map(_.sr_116568_inst_list.asScala.toList.map(_.NumDataPDUTxBytes.asInstanceOf[Long])))).flatten.flatten.flatten
            val pdcpUlCInvalidBytesVal = bean45236.sr_116425_inst_list.asScala.toList.map(_.sr_116537_inst_list.asScala.toList.map(_.sr_116426_inst_list.asScala.toList.map(_.sr_116568_inst_list.asScala.toList.map(_.NumDiscardSDUBytes.asInstanceOf[Long])))).flatten.flatten.flatten
            val numRbsVal:Integer = Try(bean45236.sr_116425_inst_list.asScala.toList.map(_.sr_116537_inst_list.asScala.toList.map(_.sr_116426_inst_list.asScala.toList.map(_.NumRBs.asInstanceOf[Integer]))).flatten.flatten.head).getOrElse(0)
            val subPacketVersionVal:Integer = Try(bean45236.sr_116425_inst_list.asScala.toList.map(_.sr_116537_inst_list.asScala.toList.map(_.Version.asInstanceOf[Integer])).flatten.head).getOrElse(0)
            for (i <- 0 to numRbsVal - 1) {
              pdcpUlTotalBytesVal += pdcpUlCDataBytesVal(i) + pdcpUlCInvalidBytesVal(i)
            }
            logRecordQComm2Obj = logRecordQComm2Obj.copy(pdcpUlNumRBs = numRbsVal, pdcpUlCtrlBytes = pdcpUlCtrlBytesVal, pdcpUlDataBytes = pdcpUlCDataBytesVal, pdcpUlInvalidBytes = pdcpUlCInvalidBytesVal,
              pdcpUlSubPacketVersion = subPacketVersionVal, pdcpUlTotalBytes = pdcpUlTotalBytesVal, pdcpUlTimeStamp = logRecord.dmTimeStamp.getTime)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45236",
              hexLogCode = "0xB0B4", logRecordQComm2 = logRecordQComm2Obj)
          case 56 =>
            var pdcpUlTotalBytesVal: Long = 0
            val bean45236_56 = process45236_56(x, exceptionUseArray)
            val pdcpUlCtrlBytesVal = bean45236_56.sr_125640_inst_list.asScala.toList.flatMap(_.sr_125647_inst_list.asScala.toList.map(_.ControlPduBytes.asInstanceOf[Long]))
            val pdcpUlCDataBytesVal = bean45236_56.sr_125640_inst_list.asScala.toList.flatMap(_.sr_125647_inst_list.asScala.toList.map(_.DataPduBytes.asInstanceOf[Long]))
            val pdcpUlCInvalidBytesVal = bean45236_56.sr_125640_inst_list.asScala.toList.flatMap(_.sr_125647_inst_list.asScala.toList.map(_.DiscardSDUBytes.asInstanceOf[Long]))
            val numRbsVal:Integer = Try(bean45236_56.sr_125639_inst_list.asScala.toList.map(_.NumRbLogged.asInstanceOf[Integer]).head).getOrElse(0)
            val subPacketVersionVal:Integer = Try(bean45236_56.Version.asInstanceOf[Integer]).getOrElse(0)
            for (i <- 0 to numRbsVal - 1) {
              pdcpUlTotalBytesVal += pdcpUlCDataBytesVal(i) + pdcpUlCInvalidBytesVal(i)
            }
            logRecordQComm2Obj = logRecordQComm2Obj.copy(pdcpUlNumRBs = numRbsVal, pdcpUlCtrlBytes = pdcpUlCtrlBytesVal, pdcpUlDataBytes = pdcpUlCDataBytesVal, pdcpUlInvalidBytes = pdcpUlCInvalidBytesVal,
              pdcpUlSubPacketVersion = subPacketVersionVal, pdcpUlTotalBytes = pdcpUlTotalBytesVal, pdcpUlTimeStamp = logRecord.dmTimeStamp.getTime)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45236",
              hexLogCode = "0xB0B4", logRecordQComm2 = logRecordQComm2Obj)
          case _ =>
            var pdcpUlTotalBytesVal: Long = 0
            val bean45236 = process45236(x, exceptionUseArray)

            val pdcpUlCtrlBytesVal = bean45236.rbs.asScala.toList.map(_.numControlPDUTxBytes.asInstanceOf[Long])
            val pdcpUlCDataBytesVal = bean45236.rbs.asScala.toList.map(_.numDataPDUTxBytes.asInstanceOf[Long])
            val pdcpUlCInvalidBytesVal = bean45236.rbs.asScala.toList.map(_.numDiscardSDUBytes.asInstanceOf[Long])

            for (i <- 0 to bean45236.numRb - 1) {
              pdcpUlTotalBytesVal += pdcpUlCDataBytesVal(i) + pdcpUlCInvalidBytesVal(i)
            }

            logRecordQComm2Obj = logRecordQComm2Obj.copy(pdcpUlNumRBs = bean45236.numRb, pdcpUlCtrlBytes = pdcpUlCtrlBytesVal, pdcpUlDataBytes = pdcpUlCDataBytesVal, pdcpUlInvalidBytes = pdcpUlCInvalidBytesVal,
              pdcpUlSubPacketVersion = bean45236.subPacketVersion, pdcpUlTotalBytes = pdcpUlTotalBytesVal, pdcpUlTimeStamp = logRecord.dmTimeStamp.getTime)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45236",
              hexLogCode = "0xB0B4", logRecordQComm2 = logRecordQComm2Obj)
        }

      case 45411 =>
        val bean45411 = process45411(x, exceptionUseArray)
        logRecordQComm2Obj = logRecordQComm2Obj.copy(txModeUl = bean45411.txMode)
        logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45411",
          hexLogCode = "0xB163", logRecordQComm2 = logRecordQComm2Obj)
      case 45409 =>
        val bean45409 = process45409(x, exceptionUseArray)
        logRecordQComm2Obj = logRecordQComm2Obj.copy(txModeDl = bean45409.txMode)
        logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45409",
          hexLogCode = "0xB161", logRecordQComm2 = logRecordQComm2Obj)
      case 45191 =>
        //logger.info("0xB087 logVersion: " + logVersion)

        logVersion match {
          case 1 =>
            val bean45191 = process45191_1(x, exceptionUseArray)
            //logger.info("0xB087 json: " + Constants.getJsonFromBean(bean45191));

            val subpktVersion = bean45191.sr_22184_inst_list.asScala.toList.map(_.sr_22189_inst_list.asScala.toList.map(_.Version)).flatten.head
            val subpkId = bean45191.sr_22184_inst_list.asScala.toList.map(_.SubpacketID)

            var ctrlBytesVal: List[Integer] = null
            var dataBytesVal: List[Long] = null
            var invalidBytesVal: List[Long] = null
            var numRBsVal: Integer = null
            var rlcDlTotalBytesVal: Long = 0

            subpktVersion.intValue() match {
              case 1 =>
                ctrlBytesVal = bean45191.sr_22184_inst_list.asScala.toList.map(_.sr_22189_inst_list.asScala.toList.map(_.sr_22903_inst_list.asScala.toList.map(_.sr_23147_inst_list.asScala.toList.map(_.NumCtrlPDU)))).flatten.flatten.flatten
                val dataBytesValV1 = bean45191.sr_22184_inst_list.asScala.toList.map(_.sr_22189_inst_list.asScala.toList.map(_.sr_22903_inst_list.asScala.toList.map(_.sr_23147_inst_list.asScala.toList.map(_.DataPDUBytes)))).flatten.flatten.flatten
                var invalidBytesValV1 = bean45191.sr_22184_inst_list.asScala.toList.map(_.sr_22189_inst_list.asScala.toList.map(_.sr_22903_inst_list.asScala.toList.map(_.sr_23147_inst_list.asScala.toList.map(_.InvalidPDUBytes)))).flatten.flatten.flatten
                numRBsVal = bean45191.sr_22184_inst_list.asScala.toList.map(_.sr_22189_inst_list.asScala.toList.map(_.sr_22903_inst_list.asScala.toList.map(_.NumRBs))).flatten.flatten.head
                dataBytesVal = dataBytesValV1.map(_.toString.toLong)
                invalidBytesVal = invalidBytesValV1.map(_.toString.toLong)
              case 2 =>
                ctrlBytesVal = bean45191.sr_22184_inst_list.asScala.toList.map(_.sr_22189_inst_list.asScala.toList.map(_.sr_22904_inst_list.asScala.toList.map(_.sr_23038_inst_list.asScala.toList.map(_.NumCtrlPDU)))).flatten.flatten.flatten
                val dataBytesValV2 = bean45191.sr_22184_inst_list.asScala.toList.map(_.sr_22189_inst_list.asScala.toList.map(_.sr_22904_inst_list.asScala.toList.map(_.sr_23038_inst_list.asScala.toList.map(_.DataPDUBytes)))).flatten.flatten.flatten
                val invalidBytesValV2 = bean45191.sr_22184_inst_list.asScala.toList.map(_.sr_22189_inst_list.asScala.toList.map(_.sr_22904_inst_list.asScala.toList.map(_.sr_23038_inst_list.asScala.toList.map(_.InvalidPDUBytes)))).flatten.flatten.flatten
                numRBsVal = bean45191.sr_22184_inst_list.asScala.toList.map(_.sr_22189_inst_list.asScala.toList.map(_.sr_22904_inst_list.asScala.toList.map(_.NumRBs.asInstanceOf[Integer]))).flatten.flatten.head
                dataBytesVal = dataBytesValV2.map(_.toString.toLong)
                invalidBytesVal = invalidBytesValV2.map(_.toString.toLong)
              case 3 =>
                ctrlBytesVal = bean45191.sr_22184_inst_list.asScala.toList.map(_.sr_22189_inst_list.asScala.toList.map(_.sr_22905_inst_list.asScala.toList.map(_.sr_22909_inst_list.asScala.toList.map(_.NumCtrlPDU)))).flatten.flatten.flatten
                dataBytesVal = bean45191.sr_22184_inst_list.asScala.toList.map(_.sr_22189_inst_list.asScala.toList.map(_.sr_22905_inst_list.asScala.toList.map(_.sr_22909_inst_list.asScala.toList.map(_.DataPDUBytes.asInstanceOf[Long])))).flatten.flatten.flatten
                invalidBytesVal = bean45191.sr_22184_inst_list.asScala.toList.map(_.sr_22189_inst_list.asScala.toList.map(_.sr_22905_inst_list.asScala.toList.map(_.sr_22909_inst_list.asScala.toList.map(_.InvalidPDUBytes.asInstanceOf[Long])))).flatten.flatten.flatten
                numRBsVal = bean45191.sr_22184_inst_list.asScala.toList.map(_.sr_22189_inst_list.asScala.toList.map(_.sr_22905_inst_list.asScala.toList.map(_.NumRBs))).flatten.flatten.head
            }

            for (i <- 0 to numRBsVal - 1) {
              rlcDlTotalBytesVal += dataBytesVal(i) + ctrlBytesVal(i) + invalidBytesVal(i)
            }

            logRecordQComm2Obj = logRecordQComm2Obj.copy(rlcDlDataBytes = dataBytesVal, rlcDlCtrlBytes = ctrlBytesVal, rlcDlInvalidBytes = invalidBytesVal, rlcDlNumRBs = numRBsVal, rlcDlTotalBytes = rlcDlTotalBytesVal, rlcDlTimeStamp = logRecord.dmTimeStamp.getTime)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45191",
              hexLogCode = "0xB087", version = logVersion,  logRecordQComm2 = logRecordQComm2Obj)

          //Try(List(macDlfsn(0).toInt.asInstanceOf[Integer])).getOrElse(null),
          case 49 =>
            val bean45191 = process45191_49(x, exceptionUseArray)
            val subpktVersion = bean45191.Version
            val rlcDataBytesVal:List[String] = bean45191.sr_103494_inst_list.asScala.toList.map(_.RlcDataBytes)
            val rbCongifIndexVal:List[String] = bean45191.sr_103494_inst_list.asScala.toList.map(_.RBConfigIndex)

            val numRBsVal:Integer = if(Try(bean45191.NumRB.toInt).isSuccess) bean45191.NumRB.toInt.asInstanceOf[Integer] else null
            //var rlcDataBytesPerConfigVal = scala.collection.mutable.Map[Integer,java.lang.Long]()
            var rlcDataBytes = Try(rlcDataBytesVal.map(_.toLong)).getOrElse(null)
            var rbCongifIndex  = Try(rbCongifIndexVal.map(_.toInt)).getOrElse(null)

            logRecordQComm2Obj = logRecordQComm2Obj.copy(rlcDlNumRBs = numRBsVal,rlcDlDataBytes = rlcDataBytes, rlcDlTimeStamp = logRecord.dmTimeStamp.getTime,rlcRbConfigIndex = rbCongifIndex)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45191",
              hexLogCode = "0xB087", version = logVersion,  logRecordQComm2 = logRecordQComm2Obj)

          case _ =>
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45191", hexLogCode = "0xB087", missingVersion = logVersion)
        }
      case 45294 =>
        logVersion match {
          case 2 =>
            val bean45294 = process45294_2(x, exceptionUseArray)
            val emmStateVal = bean45294.EMMstate
            val emmSubStateVal = bean45294.EMMsubstate
            val mccVal = bean45294.sr_35426_inst_list.asScala.toList.map(_.mcc)
            val mncVal = bean45294.sr_35426_inst_list.asScala.toList.map(_.mnc)
            logRecordQComm2Obj = logRecordQComm2Obj.copy(emmState = emmStateVal, emmSubState = emmSubStateVal, mcc_QComm2 = if( mccVal.nonEmpty) mccVal(0) else null, mnc_QComm2 = if( mncVal.nonEmpty) mncVal(0) else null)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45294",
              hexLogCode = "0xB0EE", logRecordQComm2 = logRecordQComm2Obj)
          case _ =>
            val bean45294 = process45294(x, exceptionUseArray)
            val emmStateVal = bean45294.emmState
            val emmSubStateVal = bean45294.emmSubState
            val mccVal = bean45294.mcc
            val mncVal = bean45294.mnc
            logRecordQComm2Obj = logRecordQComm2Obj.copy(emmState = emmStateVal, emmSubState = emmSubStateVal, mcc_QComm2 = mccVal, mnc_QComm2 = mncVal)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "45294",
              hexLogCode = "0xB0EE", logRecordQComm2 = logRecordQComm2Obj)
        }

    }
    logRecord
  }

  def parseLogRecordQCOMM2_Set3(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    import collection.JavaConverters._
    var logRecord: LogRecord = parentlogRecord
    var logRecordQComm2Obj:LogRecordQComm2 = LogRecordQComm2()
    var logRecordQComm5GObj: LogRecordQComm5G = LogRecordQComm5G()

    logCode match {
      case 8187 =>
        val eventCode = (256 * ParseUtils.intFromLastHalfByte(x(15))) + ParseUtils.intFromByte(x(14))
        //logger.info("Event Code for 8187.........."+eventCode)
        eventCode match {
          case 1498 => val bean1498 = process1498_1(x, exceptionUseArray)
            logRecordQComm2Obj = logRecordQComm2Obj.copy(timingAdvanceEvt = bean1498.TimingAdvance)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "1498", hexLogCode = "1498", logRecordQComm2 = logRecordQComm2Obj)
          case 1606 =>
            //            TODO commenting causing error lines
            //            val bean1606 = process1606_1(x, exceptionUseArray)
            //logger.info("lteRrcState(1606): " + bean1606.RRCState)
            //            logRecordQComm2Obj = logRecordQComm2Obj.copy(lteRrcState = bean1606.RRCState)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "1606", hexLogCode = "1606", logRecordQComm2 = logRecordQComm2Obj)
          case 1607 =>
            logRecordQComm2Obj = logRecordQComm2Obj.copy(lteOos = 1)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "1607", hexLogCode = "1607",  logRecordQComm2 = logRecordQComm2Obj)
          case 1608 =>
            logRecordQComm2Obj = logRecordQComm2Obj.copy( lteRlf = 1)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "1608", hexLogCode = "1608", logRecordQComm2 = logRecordQComm2Obj)
          case 1611 => val bean5649 = process1611(x, exceptionUseArray)
            var handoverSuccessMsg: Integer = null
            var newCellCauseStr: String = null
            //logger.info("1611: Json: " + Constants.getJsonFromBean(bean5649))
            //            logger.info("1611 Cause: " + bean5649.Cause)

            newCellCauseStr = bean5649.Cause
            bean5649.Cause match{
              case "handover" =>
                handoverSuccessMsg = 1
              //logger.info("1611 handoverSuccessMsg: " + handoverSuccessMsg)
              case _=>
            }

            //Workaround for ticket ONP-159: Start. To be removed after the fix is available
            if(handoverSuccessMsg == null && ParseUtils.intFromByte(x(25)) == 2) {
              newCellCauseStr = "handover"
              handoverSuccessMsg = 1
            }
            //Workaround for ticket ONP-159: End. To be removed after the fix is available

            logRecordQComm2Obj = logRecordQComm2Obj.copy( newCellCause = newCellCauseStr, lteHandoverSuccess = handoverSuccessMsg)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "1611", hexLogCode = "1611",  logRecordQComm2 = logRecordQComm2Obj)
          case 1612 => val bean5650 = process1612(x, exceptionUseArray)
            logRecordQComm2Obj = logRecordQComm2Obj.copy( ReselectionFailureCause = bean5650.Cause)
            val failCause = logRecordQComm2Obj.ReselectionFailureCause
            if (failCause != null && !failCause.isEmpty && failCause.contains("IRAT Resel Failure")) {
              logRecordQComm2Obj = logRecordQComm2Obj.copy(lteReselFromGsmUmtsFail = 1)
            }
            else {
              logRecordQComm2Obj = logRecordQComm2Obj.copy(lteIntraReselFail = 1)
            }
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "1612", hexLogCode = "1612",  logRecordQComm2 = logRecordQComm2Obj)
          case 1613 => val bean5651 = process1613(x, exceptionUseArray)
            logRecordQComm2Obj = logRecordQComm2Obj.copy( HoFailureCause = bean5651.Cause, /*ntraLteHoTgtBand =1, IntraLteHoTgtFrq = 1
          IntraLteHoTgtPci =1,*/ HoFailure = 1, lteIntraHoFail = 1)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "1613", hexLogCode = "1613",  logRecordQComm2 = logRecordQComm2Obj)

          case 1616 => val bean5654 = process1616(x, exceptionUseArray)
            logRecordQComm2Obj = logRecordQComm2Obj.copy( lteMobilityFromEutraFail = 1,
              MobFromEutraFailureCause = bean5654.StatusCause)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "1616", hexLogCode = "1616",  logRecordQComm2 = logRecordQComm2Obj)

          case 1619 => val bean5657 = process1619(x, exceptionUseArray)
            //logger.info("Json for 1619>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean5657))
            logRecordQComm2Obj = logRecordQComm2Obj.copy( lteSibReadFailure = 1, ReceivedSibMask = if(Constants.isValidNumeric(bean5657.RxedSIBsMask,"NUMBER")) bean5657.RxedSIBsMask.toInt else null )
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "1619", hexLogCode = "1619",  logRecordQComm2 = logRecordQComm2Obj)

          case 1995 => val bean5657 = process1995(x, exceptionUseArray)
            logRecordQComm2Obj = logRecordQComm2Obj.copy( RadioLinkFailCause = bean5657.RLFCause)
            logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "1995", hexLogCode = "1995",  logRecordQComm2 = logRecordQComm2Obj)

          case 3184 => val bean3184 = process8187_3184(x,exceptionUseArray)
            //logger.info("Json for 3184>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean3184))
            val NRRrcNewCellIndEvtVal:Integer = if(!bean3184.EventID.isEmpty && bean3184.EventID.equalsIgnoreCase("3184")) 1 else null
            logRecordQComm5GObj = logRecordQComm5GObj.copy(NRRrcNewCellIndEvt = NRRrcNewCellIndEvtVal)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "3184", hexLogCode = "3184",  logRecordQComm5G = logRecordQComm5GObj)

          case 3189 => val bean3189 = process8187_3189(x,exceptionUseArray)
            //logger.info("Json for 3189>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean3189))
            val NRRrcHoFailEvtVal:Integer = if(!bean3189.EventID.isEmpty && bean3189.EventID.equalsIgnoreCase("3189")) 1 else null
            logRecordQComm5GObj = logRecordQComm5GObj.copy(NRRrcHoFailEvt = NRRrcHoFailEvtVal, NRRrcHoFailEvtCause = bean3189.FailureCase)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "3189", hexLogCode = "3189",  logRecordQComm5G = logRecordQComm5GObj)

          case 3192 => val bean3192 = process8187_3192(x,exceptionUseArray)
            //logger.info("Json for 3192>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean3192))
            val NRRlfEvtVal:Integer = if(!bean3192.EventID.isEmpty && bean3192.EventID.equalsIgnoreCase("3192")) 1 else null
            logRecordQComm5GObj = logRecordQComm5GObj.copy(NRRlfEvt = NRRlfEvtVal, NRRlfEvtCause = bean3192.Cause)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "3192", hexLogCode = "3192",  logRecordQComm5G = logRecordQComm5GObj)

          case 3243 => val bean3243 = process8187_3243(x,exceptionUseArray)
            //logger.info("Json for 3243>>>>>>>>>>>>>>"+Constants.getJsonFromBean(bean3243))
            val NRScgFailureEvtVal:Integer = if(!bean3243.EventID.isEmpty && bean3243.EventID.equalsIgnoreCase("3243")) 1 else null
            logRecordQComm5GObj = logRecordQComm5GObj.copy(NRScgFailureEvt = NRScgFailureEvtVal, NRScgFailureEvtCause = bean3243.FailureType)
            logRecord = logRecord.copy(logRecordName = "QCOMM5G", logCode = "3243", hexLogCode = "3243",  logRecordQComm5G = logRecordQComm5GObj)

          case _ =>
          //logger.info("Ignoring "+eventCode+" event..")
        }
    }
    logRecord
  }

  def getDrmDlfDeviceInfo(parentlogRecord: LogRecord, commonConfigParams:CommonConfigParameters) = {
    //val userInfo = CommonDaoImpl.getUserInfo(Constants.CONST_DML_EMAIL.toLowerCase, commonConfigParams)
    var dmUserVal = 0
    var firstNameVal = ""
    var lastNameVal = ""
    var emailVal = Constants.CONST_DRM_EMAIL.toLowerCase
    val userInfo = CommonDaoImpl.getUserInfoByLogUpload(parentlogRecord.fileName,parentlogRecord.testId, commonConfigParams)
    if(userInfo!=null){
      dmUserVal = userInfo.user_id
      firstNameVal = userInfo.firstName
      lastNameVal = userInfo.lastName
      emailVal = userInfo.email
    }
    var logRecord: LogRecord = parentlogRecord
    var logRecordQComm2Obj:LogRecordQComm2 = LogRecordQComm2()
    logRecordQComm2Obj = logRecordQComm2Obj.copy(appVersionCode = "-", appVersionName = "-", baseBandVersion = "-", deviceInfoVersion = "-", email = emailVal,
      iccid = "12121", imei_QComm2 = "123456789123456", imsFeatureTag = "-", imsStatus = "-", imsi_QComm2 = "12345", isInBuilding = "false", isLastChunk = "1",
      mdn = "1234567890", modelName = "UNKNOWN", oemName = "-", osBuildId = "-", osSdkLevel = "-", osVersion = "-", firstName = firstNameVal, lastName = lastNameVal, dmUser = dmUserVal)
    logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "65531",
      hexLogCode = "0xFFFB", logRecordQComm2 = logRecordQComm2Obj)
    logRecord
  }

  def getDmlDlfDeviceInfo(parentlogRecord: LogRecord, commonConfigParams:CommonConfigParameters) = {
    //val userInfo = CommonDaoImpl.getUserInfo(Constants.CONST_DML_EMAIL.toLowerCase, commonConfigParams)
    var dmUserVal = 0
    var firstNameVal = ""
    var lastNameVal = ""
    var emailVal = Constants.CONST_DML_EMAIL.toLowerCase
    val userInfo = CommonDaoImpl.getUserInfoByLogUpload(parentlogRecord.fileName,parentlogRecord.testId, commonConfigParams)
    if(userInfo!=null){
      dmUserVal = userInfo.user_id
      firstNameVal = userInfo.firstName
      lastNameVal = userInfo.lastName
      emailVal = userInfo.email
    }
    var logRecord: LogRecord = parentlogRecord
    var logRecordQComm2Obj:LogRecordQComm2 = LogRecordQComm2()
    logRecordQComm2Obj = logRecordQComm2Obj.copy(appVersionCode = "-", appVersionName = "-", baseBandVersion = "-", deviceInfoVersion = "-", email = emailVal,
      iccid = "21212", imei_QComm2 = "987654321987654", imsFeatureTag = "-", imsStatus = "-", imsi_QComm2 = "54321", isInBuilding = "false", isLastChunk = "1",
      mdn = "9876543210", modelName = "UNKNOWN", oemName = "-", osBuildId = "-", osSdkLevel = "-", osVersion = "-", firstName = firstNameVal, lastName = lastNameVal, dmUser = dmUserVal)
    logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "65531",
      hexLogCode = "0xFFFB", logRecordQComm2 = logRecordQComm2Obj)
    logRecord
  }

  def getSigDlfDeviceInfo(parentlogRecord: LogRecord, commonConfigParams:CommonConfigParameters) = {
    //val userInfo = CommonDaoImpl.getUserInfo(Constants.CONST_SIG_EMAIL.toLowerCase, commonConfigParams)
    var dmUserVal = 0
    var firstNameVal = ""
    var lastNameVal = ""
    var emailVal = Constants.CONST_SIG_EMAIL.toLowerCase
    val userInfo = CommonDaoImpl.getUserInfoByLogUpload(parentlogRecord.fileName,parentlogRecord.testId, commonConfigParams)
    if(userInfo!=null){
      dmUserVal = userInfo.user_id
      firstNameVal = userInfo.firstName
      lastNameVal = userInfo.lastName
      emailVal = userInfo.email
    }
    var logRecord: LogRecord = parentlogRecord
    var logRecordQComm2Obj:LogRecordQComm2 = LogRecordQComm2()
    logRecordQComm2Obj = logRecordQComm2Obj.copy( appVersionCode = "-", appVersionName = "-", baseBandVersion = "-", deviceInfoVersion = "-", email = emailVal,
      iccid = "31313", imei_QComm2 = "987654321912398", imsFeatureTag = "-", imsStatus = "-", imsi_QComm2 = "98765", isInBuilding = "false", isLastChunk = "1",
      mdn = "9876512345", modelName = "UNKNOWN", oemName = "-", osBuildId = "-", osSdkLevel = "-", osVersion = "-", firstName = firstNameVal, lastName = lastNameVal, dmUser = dmUserVal)
    logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "65531",
      hexLogCode = "0xFFFB", logRecordQComm2 = logRecordQComm2Obj)
    logRecord
  }

  def getQMDLDeviceInfo(parentlogRecord: LogRecord, commonConfigParams:CommonConfigParameters) = {
    var dmUserVal = 0
    var firstNameVal = ""
    var lastNameVal = ""
    var emailVal = Constants.CONST_SPI_DLF_EMAIL.toLowerCase
    val userInfo = CommonDaoImpl.getUserInfoByLogUpload(parentlogRecord.fileName,parentlogRecord.testId, commonConfigParams)
    if(userInfo!=null){
      dmUserVal = userInfo.user_id
      firstNameVal = userInfo.firstName
      lastNameVal = userInfo.lastName
      emailVal = userInfo.email
    }
    var logRecord: LogRecord = parentlogRecord
    var logRecordQComm2Obj:LogRecordQComm2 = LogRecordQComm2()
    logRecordQComm2Obj = logRecordQComm2Obj.copy( appVersionCode = "-", appVersionName = "-", baseBandVersion = "-", deviceInfoVersion = "-", email = emailVal,
      iccid = "41414", imei_QComm2 = "989876565432321", imsFeatureTag = "-", imsStatus = "-", imsi_QComm2 = "98987", isInBuilding = "false", isLastChunk = "1",
      mdn = "9898765654", modelName = "UNKNOWN", oemName = "-", osBuildId = "-", osSdkLevel = "-", osVersion = "-", firstName = firstNameVal, lastName = lastNameVal, dmUser = dmUserVal)
    logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "65531",
      hexLogCode = "0xFFFB", logRecordQComm2 = logRecordQComm2Obj)
    logRecord
  }

  def getSpiDlfDeviceInfo(parentlogRecord: LogRecord, commonConfigParams:CommonConfigParameters) = {
    //val userInfo = CommonDaoImpl.getUserInfo(Constants.CONST_SIG_EMAIL.toLowerCase, commonConfigParams)
    var dmUserVal = 0
    var firstNameVal = ""
    var lastNameVal = ""
    var emailVal = Constants.CONST_SPI_DLF.toLowerCase
    val userInfo = CommonDaoImpl.getUserInfoByLogUpload(parentlogRecord.fileName,parentlogRecord.testId, commonConfigParams)
    if(userInfo!=null){
      dmUserVal = userInfo.user_id
      firstNameVal = userInfo.firstName
      lastNameVal = userInfo.lastName
      emailVal = userInfo.email
    }
    var logRecord: LogRecord = parentlogRecord
    var logRecordQComm2Obj:LogRecordQComm2 = LogRecordQComm2()
    logRecordQComm2Obj = logRecordQComm2Obj.copy( appVersionCode = "-", appVersionName = "-", baseBandVersion = "-", deviceInfoVersion = "-", email = emailVal,
      iccid = "41414", imei_QComm2 = "989876565432321", imsFeatureTag = "-", imsStatus = "-", imsi_QComm2 = "98987", isInBuilding = "false", isLastChunk = "1",
      mdn = "9898765654", modelName = "UNKNOWN", oemName = "-", osBuildId = "-", osSdkLevel = "-", osVersion = "-", firstName = firstNameVal, lastName = lastNameVal, dmUser = dmUserVal)
    logRecord = logRecord.copy(logRecordName = "QCOMM2", logCode = "65531",
      hexLogCode = "0xFFFB", logRecordQComm2 = logRecordQComm2Obj)
    logRecord
  }
}
