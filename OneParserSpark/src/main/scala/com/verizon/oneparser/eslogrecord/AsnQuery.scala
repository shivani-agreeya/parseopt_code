package com.verizon.oneparser.eslogrecord

object AsnQuery {
  val dateTimePlaceHolder = "<DATETIME>" //
  val dateWithoutMills="dateWithoutMillis"
  val dateWithMills="dmTimeStamp"


  val query: String =
    "last_value(p0_NominalPUCCH) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as p0_NominalPUCCH, " +
      "last_value(ssac_BarringForMMTEL, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as ssac_BarringForMMTEL, " +
      "last_value(upperLayerIndication_r15, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as upperLayerIndication_r15, " +
      "last_value(lteRrcConRestRej, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteRrcConRestRej, " +
      "last_value(lteRrcConRej, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteRrcConRej, " +
      "last_value(nrSpcellPci, true) over(partition by filename,"+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and current row) as nrSpCellPci_temp, " +
      "last_value(NRCellId_0xB825, true) over(partition by filename,"+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and current row) as nrCellId_temp, " +
      "last_value(NRArfcn_0xB825, true) over(partition by filename,"+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and current row) as nrArfcn_temp, " +
      "last_value(nrSpcellDlFreq, true) over(partition by filename,"+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and current row) as nrSpCellDlFreq_temp, " +
      "last_value(nrSpcellBand, true) over(partition by filename,"+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and current row) as nrSpCellBand_temp, " +
      "last_value(nrSpcellDuplexMode, true) over(partition by filename,"+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and current row) as nrSpCellDuplexMode_temp, " +
      "last_value(nrSpcellDlBw, true) over(partition by filename,"+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and current row) as nrSpCellDlBw_temp, " +
      "last_value(nrSpcellUlBw, true) over(partition by filename,"+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and current row) as nrSpCellUlBw_temp, " +
      "last_value(nrSpcellUlFreq, true) over(partition by filename,"+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and current row) as nrSpCellUlFreq_temp, " +
      "last_value(nrState, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and current row) as nrState, " +
      "last_value(nrFailureType, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and current row) as nrFailureType, " +
      "last_value(nrRlf, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nrRlf, " +
      "last_value(nrScgCfgEvt, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nrScgCfgEvt, " +
      "last_value(NRRlfEvt, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as NRRlfEvt, " +
      "last_value(NRRlfEvtCause, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as NRRlfEvtCause, " +
      "last_value(NRRrcNewCellIndEvt, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as NRRrcNewCellIndEvt, " +
      "last_value(NRRrcHoFailEvt, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as NRRrcHoFailEvt, " +
      "last_value(NRScgFailureEvt, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as NRScgFailureEvt, " +
      "last_value(NRScgFailureEvtCause, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as NRScgFailureEvtCause, " +
       "last_value(NRBeamFailureEvt, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as NRBeamFailureEvt, " +
      "sum(lteRrcConRel) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as lteRrcConRelCnt, " +
      "sum(lteRrcConReq) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as lteRrcConReqCnt, " +
      "sum(lteRrcConSetup) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as lteRrcConSetupCnt, " +
      "sum(lteRrcConSetupCmp) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as lteRrcConSetupCmpCnt, " +
      "sum(lteSecModeCmd) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as lteSecModeCmdCnt, " +
      "sum(lteSecModeCmp) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as lteSecModeCmpCnt, " +
      "sum(lteRrcConReconfig) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as lteRrcConReconfigCnt, " +
      "sum(lteRrcConReconfCmp) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as lteRrcConReconfCmpCnt, " +
      "sum(lteUeCapEnq) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as lteUeCapEnqCnt, " +
      "sum(lteUeCapInfo) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as lteUeCapInfoCnt, " +
      "sum(nrRrcReConfig) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as nrRrcReConfigCnt, " +
      "sum(nrRbConfig) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as nrRbConfigCnt, " +
      "sum(nrRrcReConfigCmp) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as nrRrcReConfigCmpCnt, " +
      "sum(lteHoCmd) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as lteHoCmdCnt, "+
      "sum(lteRrcConRestReq) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as lteRrcConRestReqCnt, "+
      "sum(lterrcconrestcmp) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as lterrcconrestcmpCnt, "+
      "sum(lteRrcConRest) over(partition by filename, "+dateTimePlaceHolder+" rows between unbounded preceding and unbounded following) as lteRrcConRestCnt, " +
      "last_value(lteRrcConSetup, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as RrcConSetup, " +
      "last_value(lteRrcConSetupCmp, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as RrcConSetupCmp, " +
      "last_value(cdrxOndurationTimer, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cdrxOndurationTimer_0xB821, " +
      "last_value(cdrxInactivityTimer, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cdrxInactivityTimer_0xB821, " +
       "last_value(cdrxLongDrxCyle, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cdrxLongDrxCycle_0xB821, " +
      "last_value(cdrxShortDrxCyle, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cdrxShortDrxCycle_0xB821, " +
      "last_value(NRPrimaryPath, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as NRPrimaryPath, " +
      "last_value(NRUlDataSplitThreshold, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as NRUlDataSplitThreshold, "+
      "last_value(nrSRSTxSwitchConfig, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nrSRSTxSwitchConfig, "



  /*+
      "row_number() over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as rn "
*/


  def getAsnQuery(reportFlag: Boolean): String ={
    if(!reportFlag){
      query.replaceAll(dateTimePlaceHolder, dateWithoutMills)
    }else{
      query.replaceAll(dateTimePlaceHolder, dateWithMills)
    }
  }
}

