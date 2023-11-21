package com.verizon.oneparser.eslogrecord

object QualComm2QueryForSingle {

  val dateTimePlaceHolder = "<DATETIME>" //
  val dateWithoutMills = "dateWithoutMillis"
  val dateWithMills = "dmTimeStamp"

  val query: String =
    "last_value(RadioLinkFailCause, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as RadioLinkFailCause, " +
      "last_value(wifiBSSID, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiBSSID, " +
      "last_value(wifiDhcpServer, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiDhcpServer, " +
      "last_value(wifiGateway, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiGateway, " +
      "last_value(wifiHiddenSSID, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiHiddenSSID, " +
      "last_value(wifiIPAddr, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiIPAddr, " +
      "last_value(wifiLinkSpeed, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiLinkSpeed, " +
      "last_value(wifiMacAddr, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiMacAddr, " +
      "last_value(wifiNetMask, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiNetMask, " +
      "last_value(wifiNetworkId, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiNetworkId, " +
      "last_value(wifiPrimaryDNS, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiPrimaryDNS, " +
      "last_value(wifiRSSI, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiRSSI, " +
      "last_value(wifiSecDNS, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiSecDNS, " +
      "last_value(wifiSSID, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiSSID, " +
      "last_value(WiFiCallState, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as WiFiCallState, " +
      "last_value(wifiChannel, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiChannel, " +
      "last_value(wifiDetailState, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiDetailState, " +
      "last_value(wifiFrequency, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiFrequency, " +
      "last_value(wifiISP, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiISP, " +
      "last_value(wifiModel, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiModel, " +
      "last_value(wifiSupplicantState, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as wifiSupplicantState, " +
      "last_value(txModeUl, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as txModeUl, " +
      "last_value(txModeDl, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as txModeDl, " +
      "last_value(lteOos, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteOos, " +
      "last_value(newCellCause, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as newCellCause, " +
      "last_value(HoFailureCause, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as HoFailureCause, " +
      "last_value(HoFailure, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as HoFailure, " +
      "last_value(lteIntraHoFail, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteIntraHoFail, " +
      "last_value(lteMobilityFromEutraFail, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteMobilityFromEutraFail, " +
      "last_value(lteSibReadFailure, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteSibReadFailure, " +
      "last_value(email, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as email, " +
      "last_value(firstName, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as firstName, " +
      "last_value(lastName, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lastName, " +
      "last_value(dmUser, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as dmUser, " +
      "last_value(imei_QComm2, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as imei_QComm2, " +
      "last_value(imsStatus, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as imsStatus, " +
      "last_value(imsFeatureTag, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as imsFeatureTag, " +
      "last_value(imsi_QComm2, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as imsi_QComm2, " +
      "last_value(isInBuilding, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as isInBuilding, " +
      "last_value(mdn, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as mdn, " +
      "last_value(isLastChunk, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as isLastChunk, " +
      "last_value(modelName, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as modelName, " +
      "last_value(oemName, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as oemName, " +
      "last_value(osBuildId, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as osBuildId, " +
      "last_value(osSdkLevel, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as osSdkLevel, " +
      "last_value(osVersion, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as osVersion, " +
      "last_value(MacDlSfn, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacDlSfn, " +
      "last_value(MacDlSubSfn, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacDlSubSfn, " +
      "last_value(MacDlNumSamples, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacDlNumSamples, " +
      "last_value(MacDlTbs, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacDlTbs, " +
      "last_value(MacUlSfn, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacUlSfn, " +
      "last_value(MacUlSubSfn, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacUlSubSfn, " +
      "last_value(MacUlNumSamples, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacUlNumSamples, " +
      "last_value(MacUlGrant, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as MacUlGrant, " +
      "last_value(emmState, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as emmState, " +
      "last_value(emmSubState, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as emmSubState, " +
      "last_value(lteIntraReselFail, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteIntraReselFail, " +
      "last_value(lteReselFromGsmUmtsFail, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteReselFromGsmUmtsFail, " +
      "last_value(lteRlf, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lteRlf, " +
      "last_value(stk_SYS_MODE, true) over(partition by filename," + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and current row) as mSYS_MODE_temp, " +
      "last_value(stk1_SYS_MODE, true) over(partition by filename," + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and current row) as mSYS1_MODE_temp, " +
      "last_value(stk_SRV_STATUS, true) over(partition by filename," + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and current row) as mSRV_STATUS_temp, " +
      "last_value(stk1_SRV_STATUS, true) over(partition by filename," + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and current row) as mSRV1_STATUS_temp, " +
      "last_value(stk_SID, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as CDMA_SID, " +
      "last_value(stk_NID, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as CDMA_NID, " +
      "last_value(stk_ECIO, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as mECIO, " +
      "last_value(stk_ECIO_EvDo, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as ECIO_EvDo, " +
      "last_value(stk_SINR_EvDo, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as SINR_EvDo, " +
      //"last_value(stk_RSSI_wcdma, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as RSSI_wcdma, " +
      //"last_value(stk_GW_RSCP, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as GW_RSCP, " +
      "last_value(bandClass_QComm2, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as bandClass_QComm2, " +
      "last_value(HdrHybrid, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as mHdrHybrid, " +
      "last_value(stk_ROAM_STATUS, true) over(partition by filename," + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and current row) as ROAM_STATUS_temp, " +
      "last_value(timingAdvanceEvt, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as timingAdvanceEvt, " +
      "last_value(lteRrcState, true) over(partition by filename,"+dateTimePlaceHolder+" order by dmTimeStamp asc rows between unbounded preceding and unbounded following) as lteRrcState_temp, " +
      "last_value(stk_SYS_MODE0_0x184E, true) over(partition by filename," + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and current row) as stk_SYS_MODE0_0x184E_temp, " +
      "last_value(stk_SYS_MODE1_0x184E, true) over(partition by filename," + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and current row) as stk_SYS_MODE1_0x184E_temp, " +
      "last_value(stk_SRV_STATUS0x184E, true) over(partition by filename," + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and current row) as SRV_STATUS0x184E_temp, " +
      "last_value(stk_SID0x184E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as CDMA_SID_0x184E, " +
      "last_value(stk_NID0x184E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as CDMA_NID_0x184E, " +
      "last_value(stk_ECIO0x184E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as mECIO_0x184E, " +
      "last_value(stk_ECIO_EvDo0x184E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as ECIO_EvDo_0x184E, " +
      "last_value(stk_SINR_EvDo0x184E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as SINR_EvDo_0x184E, " +
      //"last_value(stk_RSSI_wcdma0x184E, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as RSSI_wcdma_0x184E, " +
      //"last_value(stk_GW_RSCP0x184E, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as GW_RSCP_0x184E, " +
      "last_value(sys_ID0x184E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as sys_ID0x184E, " +
      "last_value(sys_Mode_Operational_0x184E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as sys_Mode_Operational_0x184E, " +
      "last_value(CdmaEcIo_0x184E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as CdmaEcIo_0x184E, " +
      "last_value(BandClass_0x184E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as BandClass_0x184E, " +
      "last_value(EvDoECIO_0x184E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as EvDoECIO_0x184E, " +
      "last_value(EvDoSINR_0x184E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as EvDoSINR_0x184E, " +
      "last_value(WcdmaRssi_0x184E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as WcdmaRssi_0x184E, " +
      "last_value(WcdmaRscp_0x184E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as WcdmaRscp_0x184E, " +
      "last_value(WcdmaEcio_0x184E, true) over(partition by filename, " + dateTimePlaceHolder + " order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as WcdmaEcio_0x184E, " +
      "sum(ltehandoversuccess) over(partition by filename, " + dateTimePlaceHolder + " rows between unbounded preceding and unbounded following) as lteHandoverSuccessCnt, " +
      "last_value(RMTestName, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rmTestName, " +
      "last_value(RMTestState, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as rmTestState, " +
      "last_value(TestCycleId, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as testCycleId, " +
      "last_value(DeviceTime, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as deviceTime, " +
      "last_value(DeviceId, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as deviceId, " +
      "last_value(M2MReceiveStart, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as M2MReceive_Start, " +
      "last_value(M2MReceiveEnd, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as m2mReceiveEnd, " +
      "last_value(M2MCallStart, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as M2MCall_Start, " +
      "last_value(M2MCallEnd, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as m2mCallEnd, " +
      "last_value(UDPECHOStart, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as UDPECHO_Start, " +
      "last_value(UDPECHOEnd, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as udpechoEnd, " +
      "last_value(SMSStart, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as SMS_Start, " +
      "last_value(SMSEnd, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as smsEnd, " +
      "last_value(IdleStart, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as Idle_Start, " +
      "last_value(IdleEnd, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as idleEnd, " +
      "last_value(DNSStart, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as DNS_Start, " +
      "last_value(DNSEnd, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as dnsEnd, " +
      "last_value(LDRStart, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as LDR_Start, " +
      "last_value(LDREnd, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as ldrEnd, " +
      "last_value(LDRSStart, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as LDRS_Start, " +
      "last_value(LDRSEnd, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as ldrsEnd, " +
      "last_value(UPLOADStart, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as UPLOAD_Start, " +
      "last_value(UPLOADEnd, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as uploadEnd, " +
      "last_value(DOWNLOADStart, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as DOWNLOAD_Start, " +
      "last_value(DOWNLOADEnd, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as downloadEnd, " +
      "last_value(nr_rsrq, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nr_rsrq, " +
      "last_value(nr_rsrp, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nr_rsrp, " +
      "last_value(nr_rssnr, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as nr_rssnr, " +
      "last_value(lte_rsrq, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lte_rsrq, " +
      "last_value(lte_rsrp, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lte_rsrp, " +
      "last_value(lte_rssnr, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lte_rssnr, " +
      "last_value(lte_enbId, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as lte_enbId, " +
      "last_value(cellId, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as cellId, " +
      "last_value(startTime, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as startTime, " +
      "last_value(testName, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as testName, " +
      "last_value(resultType, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as resultType, " +
      "last_value(avg_nr_rsrq, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as avg_nr_rsrq, " +
      "last_value(avg_nr_rsrp, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as avg_nr_rsrp, " +
      "last_value(avg_nr_rssnr, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as avg_nr_rssnr, " +
      "last_value(avg_lte_rsrq, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as avg_lte_rsrq, " +
      "last_value(avg_lte_rsrp, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as avg_lte_rsrp, " +
      "last_value(avg_lte_rssnr, true) over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc rows between unbounded preceding and unbounded following) as avg_lte_rssnr, "

  //  +
  //    "row_number() over(partition by filename, "+dateTimePlaceHolder+" order by dmTimeStamp desc) as rn "

  def getQComm2Query(reportFlag: Boolean): String = {
    if (!reportFlag) {
      query.replaceAll(dateTimePlaceHolder, dateWithoutMills)
    } else {
      query.replaceAll(dateTimePlaceHolder, dateWithMills)
    }
  }
}
