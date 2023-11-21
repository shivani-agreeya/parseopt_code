package com.verizon.oneparser.parselogs

import com.verizon.oneparser.ProcessLogCodes
import com.verizon.oneparser.common.{Constants, OPutil}
import com.verizon.oneparser.parselogs.LogRecordParser5G.parseInteger
import com.verizon.oneparser.schema._
import com.vzwdt.parseutils.ParseUtils
import net.liftweb.json.JsonAST.{JArray, JField, JObject}
import net.liftweb.json.parse

import scala.collection.immutable.List
import scala.util.Try

object LogRecordParserASN extends ProcessLogCodes  {

  def parseLogRecordASNSet1(logCode: Int, parentlogRecord: LogRecord, logVersion: Int, x: Array[Byte], exceptionUseArray: List[String]) = {
    var logRecord: LogRecord = parentlogRecord
    logCode match {
      case 45248 =>
        var ssac_BarringForMMTEL_Val: Boolean = false
        var p0_NominalPUCCH_Val: String = ""
        var upperLayerIndication_r15_Val: String = null

        var rrcConnectionRelease: Integer = null
        var rrcConnectionRequest: Integer = null
        var rrcConnectionSetup: Integer = null
        var rrcConnectionSetupComplete: Integer = null
        var securityModeCommand: Integer = null
        var securityModeComplete: Integer = null
        var rrcConnectionReconfiguration: Integer = null
        var rrcConnectionReconfigurationComplete: Integer = null
        var ueCapabilityEnquiry: Integer = null
        var ueCapabilityInformation: Integer = null
        var rrcConnectionRestRejMsg: Integer = null
        var rrcConnectionRestReqMsg: Integer = null
        var rrcConnectionRestCmpMsg: Integer = null
        var rrcConnectionRestMsg: Integer = null
        var rrcConnectionReject: Integer = null
        var mobilityControlInfo: Integer = null
        var nrStateVal:String = ""
        var nrFailureTypeVal:String = ""
        var asnMissVer = -1
        var nrRlfVal: Integer = null

        val asnJsonStr: String = getAsnBeanAsJSON(x)
        //logger.info("0xB0C0 --> LTE ASN for Time Stamp : "+logRecord.dmTimeStamp + " & Json : " + asnJsonStr)

        val SubTitle: String = getJsonValueByKey(asnJsonStr, "Subtitle").trim
        var cellIdentityVal:String = null
        var cellIdentityHexDumpVal:String = null
        var secondaryCellGroupCfgVal:Integer = 0
        var nrRelease:Integer = null
        var measGapOffset: Integer = null
        var measGapRelease:Integer = null
        val phyCellIdVal = getJsonValueByKey(asnJsonStr, "Phy Cell Id")
        val asn4GPCIVal:Integer = if (Constants.isValidNumeric(phyCellIdVal,"NUMBER")) phyCellIdVal.toInt else null
        SubTitle match {
          case "systemInformationBlockType1" =>
            val cellIdRawVal:String = getJsonValueByKey(asnJsonStr, "cellIdentity")
            if(cellIdRawVal.nonEmpty){
              cellIdentityHexDumpVal = cellIdRawVal
              cellIdentityVal = OPutil.parseCellIdtoBinary(cellIdRawVal)
            }
          case "systemInformation" =>
            val sib2Json = getSubJsonValueByKey(asnJsonStr, "sib2")

            if (sib2Json.nonEmpty) {
              ssac_BarringForMMTEL_Val = if (getJsonValueByKey(sib2Json, "ssac_BarringForMMTEL").nonEmpty) true else false
              p0_NominalPUCCH_Val = getJsonValueByKey(sib2Json, "p0_NominalPUCCH")
              val upperLayerIndication_r15 = getJsonValueByKey(sib2Json, "upperLayerIndication_r15")
              upperLayerIndication_r15_Val = if (upperLayerIndication_r15.nonEmpty) upperLayerIndication_r15 else "Not Present"
              //                    logger.info("LTE RRC ssac_BarringForMMTEL_Val: " + ssac_BarringForMMTEL_Val)
              //                    logger.info("LTE RRC p0_NominalPUCCH_Val: " + p0_NominalPUCCH_Val)
            }
          case "paging" =>
          case "rrcConnectionRequest" =>
            rrcConnectionRequest = 1
          case "rrcConnectionSetup" =>
            rrcConnectionSetup = 1
          case "rrcConnectionSetupComplete" =>
            rrcConnectionSetupComplete = 1
          case "securityModeCommand" =>
            securityModeCommand = 1
          case "securityModeComplete" =>
            securityModeComplete = 1
          case "rrcConnectionReconfiguration" =>
            rrcConnectionReconfiguration = 1
            val mobilityControlInfoJson: String = getSubJsonValueByKey(asnJsonStr, "mobilityControlInfo")
            if(mobilityControlInfoJson.nonEmpty){
              mobilityControlInfo = 1
            }
            val secCellGrpCfgInfoJson = getSubJsonValueByKey(asnJsonStr,"nr_SecondaryCellGroupConfig_r15")
            if(secCellGrpCfgInfoJson.nonEmpty){
              secondaryCellGroupCfgVal = 1
            }
            val nrReleaseInfoJson:Boolean = checkNr_releaseExistsInPayload(asnJsonStr,"nr_release")
            if(nrReleaseInfoJson) {
              nrRelease =1
            }
            val GapOffset:String = getSubJsonValueByKey(asnJsonStr,"gp0")
            if(GapOffset.nonEmpty) {
              measGapOffset = 1
            }
            val measGapConfigStr:String = getSubJsonValueByKey(asnJsonStr,"measGapConfig")
            if(measGapConfigStr.nonEmpty) {
              val gapRelease:Boolean = checkNr_releaseExistsInPayload(measGapConfigStr,"release")
              if(gapRelease) {
                measGapRelease = 1
              }
            }
          case "rrcConnectionReconfigurationComplete" =>
            rrcConnectionReconfigurationComplete = 1
          case "measurementReport" =>
          case "ulInformationTransfer" =>
          case "dlInformationTransfer" =>
          case "ueCapabilityEnquiry" =>
            ueCapabilityEnquiry = 1
          case "ueCapabilityInformation" =>
            ueCapabilityInformation = 1
          case "rrcConnectionRelease" =>
            rrcConnectionRelease = 1
          case "rrcConnectionReestablishmentReject" =>
            rrcConnectionRestRejMsg = 1
          case "rrcConnectionReestablishment" =>
            rrcConnectionRestMsg = 1
          case "rrcConnectionReestablishmentRequest" =>
            rrcConnectionRestReqMsg = 1
          case "rrcConnectionReestablishmentComplete" =>
            rrcConnectionRestCmpMsg = 1
          case "rrcConnectionReject" =>
            rrcConnectionReject = 1
          case "BCCH_BCH_Message" =>
          case "c2" | "Extension_c2_scgFailureInformationNR_r15" =>
            val c2JsonStr:String = getSubJsonValueByKey(asnJsonStr, "c2")
            if(c2JsonStr.contains("scgFailureInformationNR_r15")){
              nrStateVal = "Disconnected/RLF"
              nrFailureTypeVal = getJsonValueByKey(c2JsonStr, "failureType_r15")
              nrRlfVal = 1
            }
          case "" =>
            asnMissVer = getJsonValueByKey(asnJsonStr,"Packet Ver. Num").toInt
          case _ =>
        }
        var logRecordASNObj:LogRecordASN = new LogRecordASN()
        logRecordASNObj = logRecordASNObj.copy(
          ssac_BarringForMMTEL = ssac_BarringForMMTEL_Val,
          p0_NominalPUCCH = p0_NominalPUCCH_Val,
          upperLayerIndication_r15 = upperLayerIndication_r15_Val,
          lteRrcConRestRej = rrcConnectionRestRejMsg,
          lteRrcConRej = rrcConnectionReject,
          lteRrcConRel = rrcConnectionRelease,
          lteRrcConReq = rrcConnectionRequest,
          lteRrcConSetup = rrcConnectionSetup,
          lteRrcConSetupCmp = rrcConnectionSetupComplete,
          lteSecModeCmd = securityModeCommand,
          lteSecModeCmp = securityModeComplete,
          lteRrcConReconfig = rrcConnectionReconfiguration,
          lteRrcConReconfCmp = rrcConnectionReconfigurationComplete,
          lteUeCapEnq = ueCapabilityEnquiry,
          lteUeCapInfo = ueCapabilityInformation,
          lteHoCmd = mobilityControlInfo,
          lteRrcConRestReq = rrcConnectionRestReqMsg,
          lteRrcConRest = rrcConnectionRestMsg,
          nrState = nrStateVal,
          //nrFailureType = nrFailureTypeVal,
          lteRrcConRestCmp = rrcConnectionRestCmpMsg,
          //nrRlf = nrRlfVal,
          subTitle = SubTitle,
          cellIdentity = cellIdentityVal,
          cellIdentityHexDump = cellIdentityHexDumpVal,
          secondaryCellGroupCfg = secondaryCellGroupCfgVal,
          asn4GPCI = asn4GPCIVal,
          NrRelease = nrRelease,
          MeasGapOffset = measGapOffset,
          MeasGapRelease = measGapRelease,
          status0xB0C0 = true
        )
        logRecord = logRecord.copy(logRecordName = "ASN", logCode = "45248",
          hexLogCode = "0xB0C0", missingVersion = asnMissVer, logRecordASN = logRecordASNObj)

      case 47137 =>
        var pci: Integer = null
        var dlFreq: Integer = null
        var ulFreq: Integer = null
        var dlBandWidth: java.lang.Double = null
        var ulBandWidth: java.lang.Double = null
        var dlBand: Integer = null
        var ulBand: Integer = null
        var duplexMode: String = null
        var nrRrcReconfig: Integer = null
        var nrScgCfgEvtVal: Integer = null
        var nrRbConfig: Integer = null
        var nrRrcReconfigComplete: Integer = null
        var nrStateVal:String=""
        var nrPrimaryPathVal: String = null
        var nrUlDataSplitThresholdVal: String = null
        var asnMissVer = -1
        //SSBRI (SS/PBCH Block Resoruce Indicator)
        val asnJsonStr: String = getNr5gAsnBeanAsJSON(x)

        //logger.info("0xB821 --> ASN for Time Stamp : "+logRecord.dmTimeStamp + " & Json : "  + asnJsonStr)

        val pduType: String = getJsonValueByKey(asnJsonStr, "PDU Type")
        val subTitleVal: String = getJsonValueByKey(asnJsonStr, "Subtitle").trim
        val phyCellIdVal = getJsonValueByKey(asnJsonStr, "Phy Cell Id")
        val asn5GPCIVal:Integer = if (Constants.isValidNumeric(phyCellIdVal,"NUMBER")) phyCellIdVal.toInt else null
        var spCellBandDLVal:Integer = null
        var spCellNRARFCNDLVal:Integer = null
        var spCellSubCarSpaceDLVal:String = null
        var spCellNumPRBDLVal:Integer = null
        var spCellBandwidthDLVal:String = null
        var spCellDuplexModeVal:String = null
        var spCellTypeDLVal:String = null
        var spCellBandULVal:Integer = null
        var spCellNRARFCNULVal:Integer = null
        var spCellSubCarSpaceULVal:String = null
        var spCellNumPRBULVal:Integer = null
        var spCellBandwidthULVal:String = null
        var spCellTypeULVal:String = null
        var cdrxOndurationTimerVal:String = null
        var cdrxInactivityTimerVal:String = null
        var cdrxLongCycleStartOffsetVal:String = null
        var cdrxShortCycleVal:String = null
        var cdrxShortCylcleTimerVal:String = null

        pduType match {
          case "" =>
            asnMissVer = getJsonValueByKey(asnJsonStr,"Packet Ver. Num").toInt
          case "RRC_RECONFIG" =>

            nrRrcReconfig = 1
            nrStateVal = "Conn Requested"
            //logger.info("NRState for RRC_RECONFIG: "+nrStateVal)
            val spCellConfigJson: String = getSubJsonValueByKey(asnJsonStr, "spCellConfig")

            if (spCellConfigJson.nonEmpty) {
              val reconfigWithSyncJson = getSubJsonValueByKey(spCellConfigJson, "reconfigurationWithSync")

              if (reconfigWithSyncJson.nonEmpty) {

                val spCellConfigCommonJson = getSubJsonValueByKey(reconfigWithSyncJson, "spCellConfigCommon")

                if (spCellConfigCommonJson.nonEmpty) {
                  val physCellIdJson = getSubJsonValueByKey(reconfigWithSyncJson, "physCellId")

                  if (physCellIdJson.nonEmpty) {
                    pci = Try(physCellIdJson.toInt.asInstanceOf[Integer]).getOrElse(null)
                    //logger.info("5G NR ASN DL physCellIdJson : " + physCellIdJson)
                  } else {
                    //logger.info("5G NR ASN DL physCellIdJson is Empty")
                  }
                  val dlConfigJson = getSubJsonValueByKey(reconfigWithSyncJson, "downlinkConfigCommon")
                  if (dlConfigJson.nonEmpty) {
                    val frequencyInfoDLJson = getSubJsonValueByKey(dlConfigJson, "frequencyInfoDL")

                    if (frequencyInfoDLJson.nonEmpty) {
                      val absoluteFrequencySSBJson = getSubJsonValueByKey(frequencyInfoDLJson, "absoluteFrequencySSB")
                      if (absoluteFrequencySSBJson.nonEmpty) {
                        dlFreq = Try(absoluteFrequencySSBJson.toInt.asInstanceOf[Integer]).getOrElse(null)
                        spCellNRARFCNDLVal = dlFreq
                        //logger.info("5G NR ASN DL absoluteFrequencySSB : " + absoluteFrequencySSBJson + ", " + dlFreq)
                      } else {
                        //logger.info("5G NR ASN DL absoluteFrequencySSB is Empty")
                      }

                      val frequencyBandListJson = getSubJsonValueByKey(frequencyInfoDLJson, "frequencyBandList")
                      if (frequencyBandListJson.nonEmpty) {
                        val elementsJson = getSubJsonValueByKey(frequencyBandListJson, "elements")

                        if (elementsJson.nonEmpty) {
                          dlBand = Try(elementsJson.toInt.asInstanceOf[Integer]).getOrElse(null)
                          spCellBandDLVal = dlBand
                          spCellDuplexModeVal = Try(ParseUtils.getNRDuplexMode.get(spCellBandDLVal)).getOrElse(null)
                          spCellTypeDLVal = Try(ParseUtils.getNRCellType.get(spCellBandDLVal)).getOrElse(null)
                          //logger.info("5G NR ASN DL frequencyBandList -> elementsJson : " + elementsJson)
                        } else {
                          //logger.info("5G NR ASN DL frequencyBandList -> elements is Empty")
                        }
                      } else {
                        //logger.info("5G NR ASN DL frequencyBandListJson is Empty")
                      }
                      val scs_SpecificCarrierListJson = getSubJsonValueByKey(reconfigWithSyncJson, "scs_SpecificCarrierList")

                      if (scs_SpecificCarrierListJson.nonEmpty) {
                        val elementsJson = getSubJsonValueByKey(scs_SpecificCarrierListJson, "elements")
                        if (elementsJson.nonEmpty) {
                          Try(elementsJson.toInt.asInstanceOf[Integer]).getOrElse(null)
                          spCellSubCarSpaceDLVal = Try(getSubJsonValueByKey(elementsJson, "subcarrierSpacing")).getOrElse(null)
                          if(spCellSubCarSpaceDLVal!=null){
                            spCellSubCarSpaceDLVal = spCellSubCarSpaceDLVal.replaceAll("\"","")
                          }
                          spCellNumPRBDLVal = Try(getSubJsonValueByKey(elementsJson, "carrierBandwidth").toInt.asInstanceOf[Integer]).getOrElse(null)
                          if(spCellTypeDLVal != null && spCellSubCarSpaceDLVal!=null && spCellNumPRBDLVal!=null){

                            val bandWidthDLStr = spCellTypeDLVal + "_SCS" + spCellSubCarSpaceDLVal.toLowerCase.replace("khz","").replace("mhz","")+"_"+spCellNumPRBDLVal.toString
                            spCellBandwidthDLVal = ParseUtils.getNRBandwidth().get(bandWidthDLStr)
                          }
                        } else {
                          //logger.info("5G NR ASN DL scs_SpecificCarrierListJson->elementsJson is Empty")
                        }
                      } else {
                        //logger.info("5G NR ASN DL scs_SpecificCarrierListJson is Empty")
                      }
                    } else {
                      //logger.info("5G NR ASN DL frequencyInfoDLJson is Empty")
                    }
                  } else {
                    //logger.info("5G NR ASN DL dlConfigJson is Empty")
                  }


                  val ulConfigJson = getSubJsonValueByKey(reconfigWithSyncJson, "uplinkConfigCommon")
                  if (ulConfigJson.nonEmpty) {
                    val frequencyInfoULJson = getSubJsonValueByKey(ulConfigJson, "frequencyInfoUL")

                    val absoluteFrequencySSBJson = getSubJsonValueByKey(frequencyInfoULJson, "absoluteFrequencySSB")
                    if (absoluteFrequencySSBJson.nonEmpty) {
                      ulFreq = Try(absoluteFrequencySSBJson.toLong.asInstanceOf[Integer]).getOrElse(null)
                      spCellNRARFCNULVal = ulFreq
                      //logger.info("5G NR ASN UL absoluteFrequencySSB : " + absoluteFrequencySSBJson)

                    } else {
                      //logger.info("5G NR ASN DL absoluteFrequencySSB is Empty")
                    }

                    if (frequencyInfoULJson.nonEmpty) {

                      val frequencyBandListJson = getSubJsonValueByKey(frequencyInfoULJson, "frequencyBandList")
                      if (frequencyBandListJson.nonEmpty) {
                        val elementsJson = getSubJsonValueByKey(frequencyBandListJson, "elements")

                        if (elementsJson.nonEmpty) {
                          ulBand = Try(elementsJson.toInt.asInstanceOf[Integer]).getOrElse(null)
                          spCellBandULVal = ulBand
                          spCellTypeULVal = Try(ParseUtils.getNRCellType.get(spCellBandULVal)).getOrElse(null)
                          //logger.info("5G NR ASN DL frequencyBandList -> elementsJson : " + elementsJson)
                        } else {
                          //logger.info("5G NR ASN DL frequencyBandList -> elements is Empty")
                        }
                      } else {
                        //logger.info("5G NR ASN DL frequencyBandListJson is Empty")
                      }

                      val scs_SpecificCarrierListJson = getSubJsonValueByKey(frequencyInfoULJson, "scs_SpecificCarrierList")

                      if (scs_SpecificCarrierListJson.nonEmpty) {
                        val elementsJson = getSubJsonValueByKey(scs_SpecificCarrierListJson, "elements")
                        if (elementsJson.nonEmpty) {
                          Try(elementsJson.toInt.asInstanceOf[Integer]).getOrElse(null)
                          spCellSubCarSpaceULVal = Try(getSubJsonValueByKey(elementsJson, "subcarrierSpacing")).getOrElse(null)
                          if(spCellSubCarSpaceULVal!=null){
                            spCellSubCarSpaceULVal = spCellSubCarSpaceULVal.replaceAll("\"","")
                          }
                          spCellNumPRBULVal = Try(getSubJsonValueByKey(elementsJson, "carrierBandwidth").toInt.asInstanceOf[Integer]).getOrElse(null)
                          if(spCellTypeULVal != null && spCellSubCarSpaceULVal!=null && spCellNumPRBULVal!=null){
                            val bandWidthULStr = spCellTypeULVal + "_SCS" + spCellSubCarSpaceULVal.toLowerCase.replace("khz","").replace("mhz","")+"_"+spCellNumPRBULVal.toString
                            spCellBandwidthDLVal = ParseUtils.getNRBandwidth().get(bandWidthULStr)
                          }
                        } else {
                          //logger.info("5G NR ASN DL scs_SpecificCarrierListJson->elementsJson is Empty")
                        }
                      } else {
                        //logger.info("5G NR ASN UL scs_SpecificCarrierListJson is Empty")
                      }
                    } else {
                      //logger.info("5G NR ASN UL frequencyInfoULJson is Empty")
                    }
                  } else {
                    //logger.info("5G NR ASN UL ulConfigJson is Empty")
                  }
                } else {
                  //logger.info("5G NR ASN spCellConfigCommonJson is Empty")
                }
              } else {
                //logger.info("5G NR ASN reconfigWithSyncJson is Empty")
              }
            } else {
              //logger.info("5G NR ASN spCellConfigJson is Empty")
            }

            val macCellConfigJson: String = getSubJsonValueByKey(asnJsonStr, "mac_CellGroupConfig")
            if(macCellConfigJson.nonEmpty) {
              val setupJson = getSubJsonValueByKey(macCellConfigJson, "setup")
              if(setupJson.nonEmpty) {
                cdrxOndurationTimerVal = Try(getSubJsonValueByKey(setupJson, "drx_onDurationTimer_milliSeconds")).getOrElse(null)
                cdrxInactivityTimerVal = Try(getSubJsonValueByKey(setupJson, "drx_InactivityTimer")).getOrElse(null)
                cdrxLongCycleStartOffsetVal = Try(getSubJsonValueByKey(setupJson, "drx_LongCycleStartOffset")).getOrElse(null)
                cdrxShortCycleVal = Try(getSubJsonValueByKey(setupJson, "shortDRX_drx_ShortCycle")).getOrElse(null)
              }
            }

            if (dlBand != null) {
              duplexMode = ParseUtils.getNRDuplexMode().get(dlBand)
              if (duplexMode.equalsIgnoreCase("TDD")) {
                ulFreq = dlFreq
                ulBand = dlBand
              }
            }

          case "RRC_RECONFIG_COMPLETE" =>
            nrRbConfig = 1
            nrStateVal = "Connected"
            nrScgCfgEvtVal = 1
            nrRrcReconfigComplete = 1
            //logger.info("NRState for RRC_RECONFIG_COMPLETE: "+nrStateVal)
            val pciIdVal = getJsonValueByKey(asnJsonStr,"Phy Cell Id")
            pci =  if (Constants.isValidNumeric(pciIdVal,"NUMBER")) pciIdVal.toInt else null
          //nrRrcReconfigComplete is based on RRC_RECONFIG_COMPLETE msg, commenting out below
          /*case "RADIO_BEARER_CONFIG" =>
            nrRrcReconfigComplete = 1*/

          case "MEAS_RESULT_CELLLIST_SFTD_EUTRA" =>
            val radioBearerConfigJson = getSubJsonValueByKey(asnJsonStr, "RadioBearerConfig")
            if (radioBearerConfigJson.nonEmpty) {
              nrPrimaryPathVal = Try {
                getJsonValueByKey(radioBearerConfigJson, "cellGroup").toInt match {
                  case 0 => "LTE"
                  case 1 => "NR"
                  case _ => null
                }
              }.getOrElse(null)

              nrUlDataSplitThresholdVal = Try {
                getJsonValueByKey(radioBearerConfigJson, "ul_DataSplitThreshold") match {
                  case ul if ul.startsWith("b") => {
                    (ul.substring(1).toFloat / 1000).toString.concat(" kBytes")
                  }
                  case _ => null
                }
              }.getOrElse(null)
            }

          case _ =>
        }

        var nrSRSTxSwitchConfigVal: String = null
        val srsResourceSet = getSubJsonValueByKey(asnJsonStr, "srs_ResourceSetToAddModList")
        val usage = getJsonValueByKey(srsResourceSet, "usage")

        if (usage != null && usage.toLowerCase.contains("antennaswitching")) {
          val srsResourceIdListObj = parse(getSubJsonValueByKey(srsResourceSet, "srs_ResourceIdList"))
          val elementsList = for {JObject(obj) <- srsResourceIdListObj; JField("elements", JArray(arr)) <- obj} yield arr
          val r = elementsList.head.size

          val ports = getJsonValueByKey(asnJsonStr, "nrofSRS_Ports")
          if (ports != null) {
            val t = parseInteger(ports.charAt(4).toString)
            nrSRSTxSwitchConfigVal = s"t${t}r${r}"
          }
        }

        //logger.info("duplexMode=" + duplexMode)
        var logRecordASNObj: LogRecordASN = new LogRecordASN()
        logRecordASNObj = logRecordASNObj.copy(
          nrSpcellPci = pci,
          nrSpcellDlFreq = dlFreq,
          nrSpcellBand = dlBand,
          nrSpcellDuplexMode = duplexMode,
          nrSpcellUlFreq = ulFreq,
          nrSpcellDlBw = dlBandWidth,
          nrSpcellUlBw = ulBandWidth,
          nrRrcReConfig = nrRrcReconfig,
          nrRbConfig = nrRbConfig,
          nrRrcReConfigCmp = nrRrcReconfigComplete,
          subTitle = subTitleVal,
          nrState = nrStateVal,
          asn5GPCI = asn5GPCIVal,
          nrScgCfgEvt = nrScgCfgEvtVal,
          spCellBandDL = spCellBandDLVal, spCellNRARFCNDL = spCellNRARFCNDLVal, spCellSubCarSpaceDL = spCellSubCarSpaceDLVal, spCellNumPRBDL = spCellNumPRBDLVal, spCellBandwidthDL = spCellBandwidthDLVal,
          spCellDuplexMode = spCellDuplexModeVal, spCellTypeDL = spCellTypeDLVal, spCellBandUL = spCellBandULVal, spCellNRARFCNUL = spCellNRARFCNULVal,
          spCellSubCarSpaceUL = spCellSubCarSpaceULVal, spCellNumPRBUL = spCellNumPRBULVal, spCellBandwidthUL = spCellBandwidthULVal, spCellTypeUL = spCellTypeULVal,
          cdrxOndurationTimer = cdrxOndurationTimerVal, cdrxInactivityTimer = cdrxInactivityTimerVal, cdrxLongDrxCyle = cdrxLongCycleStartOffsetVal, cdrxShortDrxCyle = cdrxShortCycleVal,
          nrPrimaryPath = nrPrimaryPathVal, nrUlDataSplitThreshold = nrUlDataSplitThresholdVal, nrSRSTxSwitchConfig = nrSRSTxSwitchConfigVal
        )
        logRecord = logRecord.copy(logRecordName = "ASN", logCode = "47137",
          hexLogCode = "0xB821", missingVersion = asnMissVer, logRecordASN = logRecordASNObj)
    }
    logRecord
  }

}
